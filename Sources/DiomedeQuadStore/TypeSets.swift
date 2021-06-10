//
//  TypeSets.swift
//  DiomedeQuadStore
//
//  Created by Gregory Todd Williams on 5/27/20.
//

import Foundation
import Diomede
import SPARQLSyntax
import Sketching

public struct TypeIDSet: Codable {
    public typealias TermID = UInt64
    public var graph: TermID
    public var count: Int
    public var types: Set<TermID>
    // NOTE: whenever new variables are added here, they must be serialized and deserialized
    //       in the TypeIDSet extension below that implements asData()/fromData().

    public init(graph: TermID, types: Set<TermID>, count: Int = 0) {
        self.count = count
        self.graph = graph
        self.types = types
    }

    public mutating func formUnion(_ other: TypeIDSet) {
        self.count += other.count
        self.types.formUnion(other.types)
    }

    public func union(_ other: TypeIDSet) -> TypeIDSet {
        let count = self.count + other.count
        let types = self.types
        return TypeIDSet(graph: self.graph, types: types, count: count)
    }

    public mutating func addStar() {
        // caller is responsible for ensuring that all added stars have the same types
        self.count += 1
    }
    
    func isSuperset(of subset: TypeIDSet) -> Bool {
        return types.isSuperset(of: subset.types)
    }
}

public struct TypeSet: Codable {
    public var count: Int
    public var types: Set<Term>
    
    public init(_ cs: TypeIDSet, from store: DiomedeQuadStore) {
        self.count = cs.count
        self.types = []
        for tid in cs.types {
            let terms = store.termIterator(fromIds: [tid])
            let term = terms.next()!
            self.types.insert(term)
        }
    }
    
    public init(types: Set<Term>) {
        self.count = 0
        self.types = types
    }

    public init(types: Set<Term>, count: Int) {
        self.count = count
        self.types = types
    }

    public mutating func formUnion(_ other: TypeSet) {
        self.count += other.count
        self.types.formUnion(other.types)
    }

    public func union(_ other: TypeSet) -> TypeSet {
        let count = self.count + other.count
        let types = self.types.union(other.types)
        return TypeSet(types: types, count: count)
    }

    public func isSuperset(of subset: TypeSet) -> Bool {
        return types.isSuperset(of: subset.types)
    }
}

public struct TypeDataSet {
    typealias TermID = UInt64
    var typeSets: [TypeIDSet]
    var store: DiomedeQuadStore

    public var sets: [TypeSet] {
        return typeSets.map {
            TypeSet($0, from: store)
        }
    }
    
    public init(_ store: DiomedeQuadStore, typeSets: [TypeIDSet]) throws {
        self.store = store
        self.typeSets = typeSets
    }
    
    public init(_ store: DiomedeQuadStore, in graph: Term) throws {
        self.store = store
        if let bestIndex = try store.bestIndex(matchingBoundPositions: [0, 1, 3]) {
            if bestIndex == .gpso {
                // we can access triples sorted by subject, meaning we can pipeline the first grouping without keeping all triples in memory
                self.typeSets = try TypeDataSet.generateTypeSets_ordered(store: store, using: bestIndex, in: graph)
                return
            }
        }
        self.typeSets = try TypeDataSet.generateTypeSets_naive(store: store, in: graph)
    }
    
    static func generateTypeSets_ordered(store: DiomedeQuadStore, using index: DiomedeQuadStore.IndexOrder, in graph: Term) throws -> [TypeIDSet] {
        precondition(index == .gpso)
        var characteristicSets = [TypeIDSet]()
        var lastSubject: TermID? = nil
        var triples = [[TermID]]()
        var css = [Set<TermID>: TypeIDSet]()
        
        var qp = QuadPattern.all
        qp.graph = .bound(graph)

        guard case .bound(let graph) = qp.graph, let gid = try store.id(for: graph), let typeid = try store.id(for: Term.rdf("type")) else {
            throw DiomedeError.indexError
        }
        
        let quadIds = try store.quadIds(usingIndex: index, withPrefix: [gid, typeid])
        for tids in quadIds {
            let t = tids[0..<3]
            let s = t[0]
            if let last = lastSubject, last != s {
                let types = Set(triples.map { $0[2] })
                
                css[types, default: TypeIDSet(graph: gid, types: types)].addStar()
                triples = []
            }
            triples.append(Array(t))
            lastSubject = s
        }
        
        // handle the remaining triples
        if !triples.isEmpty {
            let types = Set(triples.map { $0[2] })

            css[types, default: TypeIDSet(graph: gid, types: types)].addStar()
        }
        
        characteristicSets.append(contentsOf: css.values)
        return characteristicSets
    }
    
    static func generateTypeSets_naive(store: DiomedeQuadStore, in graph: Term) throws -> [TypeIDSet] {
        var typeSets = [TypeIDSet]()
        var allTypes = [TermID: Set<TermID>]()
        var css = [Set<TermID>: TypeIDSet]()

        var qp = QuadPattern.all
        qp.graph = .bound(graph)
        qp.predicate = .bound(Term.rdf("type"))

        for tids in try store.quadIds(matching: qp) {
            let t = tids[0..<3]
            let s = t[0]
            let type = t[2]
            allTypes[s, default: []].insert(type)
        }
        
        guard let gid = try store.id(for: graph) else {
            throw DiomedeError.indexError
        }
        
        for (_, types) in allTypes {
            css[types, default: TypeIDSet(graph: gid, types: types)].addStar()
        }
        
        typeSets.append(contentsOf: css.values)
        return typeSets
    }
    
    public func selectivity(of object: Term, given predicate: Node, in graph: Term, store: DiomedeQuadStore) throws -> Double {
        var pattern = QuadPattern.all
        pattern.graph = .bound(graph)
        pattern.predicate = predicate
        let p_count = try store.countQuads(matching: pattern)
        pattern.object = .bound(object)
        let op_count = try store.countQuads(matching: pattern)
        
        let s = Double(op_count) / Double(p_count)
//        print("selectivity of \(object) given \(predicate) in \(graph): \(s)")
        return s
    }
    
    public var instanceCount: Int {
        return typeSets.reduce(0, { $0 + $1.count })
    }

    public func aggregatedTypeSet(matching bgp: [TriplePattern], in graph: Term, store: DiomedeQuadStore) throws -> TypeIDSet {
        guard let gid = try store.id(for: graph) else {
            throw DiomedeError.indexError
        }
        
        let typeTriples = bgp.filter { (t) in
            if case .bound(Term.rdf("type")) = t.predicate {
                return true
            }
            return false
        }
        let types = typeTriples.map { (t) -> Term? in
            let n = t.object
            if case .bound(let term) = n {
                return term
            } else {
                return nil
            }
        }.compactMap { $0 }
        
        let subset = try self.typeIDSet(matching: Set(types), in: graph, store: store)
        let matching = typeSets.filter { $0.isSuperset(of: subset) }
        let acs = matching.reduce(TypeIDSet(graph: gid, types: [])) { $0.union($1) }
        return acs
    }
    
    public func typeIDSet(matching types: Set<Term>, in graph: Term, store: DiomedeQuadStore) throws -> TypeIDSet {
        var termIds = [UInt64]()
        try store.env.read { (txn) -> Int in
            for term in types {
                guard let id = try store.id(for: term, txn: txn) else {
                    throw DiomedeError.nonExistentTermError
                }
                termIds.append(id)
            }
            return 0
        }
        
        guard let gid = try store.id(for: graph) else {
            throw DiomedeError.indexError
        }
        
        var subset = TypeIDSet(graph: gid, types: Set(termIds))
        subset.addStar()
        // the counts don't matter in this TypeIDSet because it will only be used
        // in a subsequent code to match supersets (which will contain real counts of the data).
        return subset
    }
    
//    public func starCardinality(matching bgp: [TriplePattern], in graph: Term, store: DiomedeQuadStore) throws -> Double {
//        let subset = try self.typeIDSet(matching: bgp, in: graph, store: store)
//        let q = bgp
//
//        var card = 0.0
//        let matching = typeSets.filter { $0.isSuperset(of: subset) }
////        let subsetPreds = store.termIterator(fromIds: Array(subset.predicates)).map { $0.description }.sorted()
////        print("\(matching.count) characteristic sets match: \(subsetPreds)")
//        for set in matching {
////            let cs = TypeSet(set, from: store)
////            print("matching set: \(cs)")
//            let distinct = Double(set.count)
//            var m = 1.0
//            var o = 1.0
//            for t in q {
//                let pred = t.predicate
//                let obj = t.object
//                if case .bound(let obj) = obj {
//                    o = try Swift.min(o, self.selectivity(of: obj, given: pred, in: graph, store: store))
//                } else if case .bound(let pred) = pred {
//                    guard let pid = try store.id(for: pred) else {
//                        throw DiomedeError.nonExistentTermError
//                    }
//                    let tm = Double(set.predCounts[pid]?.sum ?? 0) / distinct
////                    print("\(tm) <= \(t)")
//                    m *= tm
//                } else {
//                    // unbound predicate; sum up all the counts
//                    let allPredCounts = set.predCounts.values.map { Double($0.sum) }.reduce(0.0) { $0 + $1 }
//                    let tm = allPredCounts / distinct
////                    print("\(tm) <= \(t)")
//                    m *= tm
//                }
//            }
//            let prod = distinct * m * o
////            print("\(distinct) * \(m) * \(o) = \(prod)")
//            card += prod
//        }
////        print("= \(card)")
//        return card
//    }
    
}

extension TypeDataSet: Sequence {
    public func makeIterator() -> AnyIterator<TypeIDSet> {
        return AnyIterator(typeSets.makeIterator())
    }
}

extension TypeSet: CustomDebugStringConvertible {
    public var debugDescription: String {
        return "TypeSet(\(count); \(types.sorted()))"
    }
}

extension TypeIDSet: CustomDebugStringConvertible {
    public var debugDescription: String {
        return "TypeIDSet(\(count); \(types.sorted()))"
    }
}

extension TypeDataSet: CustomDebugStringConvertible {
    public var debugDescription: String {
        var s = "Type Sets [\n"
        for set in typeSets.sorted(by: { (a, b) in return a.count > b.count }) {
            s += "\t\(set.debugDescription)\n"
        }
        s += "]\n"
        return s
    }
}

extension DiomedeQuadStore {
    public var hasTypeSets: Bool {
        let indexName = "typeSets"
        
        do {
            let databases = Set(try env.databases())
            return databases.contains(indexName)
        } catch {
            return false
        }
    }
    
    public var typeSetsAreAccurate: Bool {
        guard hasTypeSets else { return false }
        let csHeader = "TypeSets-Last-Modified"
        let quadsHeader = "Quads-Last-Modified"
        if let csDate = self.read(mtimeHeader: csHeader), let quadsDate = self.read(mtimeHeader: quadsHeader) {
            return csDate >= quadsDate
        }
        return false
    }
    
    public func typeSets(for graph: Term) throws -> TypeDataSet {
        let indexName = "typeSets"
        
        guard let index = self.env.database(named: indexName) else {
            throw DiomedeError.indexError
        }
        guard let gid = try self.id(for: graph) else {
            throw DiomedeError.nonExistentTermError
        }

        let lower = [Int(gid), 0].asData()
        let upper = [Int(gid+1), 0].asData()
        
        var sets = [TypeIDSet]()
        try index.iterate(between: lower, and: upper) { (k, v) in
            let key = [Int].fromData(k)
            guard key[0] == gid else {
                return
            }
            let cs = try TypeIDSet.fromData(v, in: gid)
            sets.append(cs)
        }
        return try TypeDataSet(self, typeSets: sets)
    }
    
    public func dropTypeSets() throws {
        let indexName = "typeSets"
        let databases = Set(try env.databases())
        if databases.contains(indexName) {
//            print("dropping \(indexName)...")
            if let index = self.env.database(named: indexName) {
                try index.drop()
                try self.touch(mtimeHeaders: ["TypeSets-Last-Modified"]) // update the last-modified timestamp
            }
        } else {
//            print("no-op")
        }
    }
    
    public func computeTypeSets() throws {
        let indexName = "typeSets"
        let databases = Set(try env.databases())
        if databases.contains(indexName) {
            guard let index = self.env.database(named: indexName) else {
                throw DiomedeError.indexError
            }
            try index.clear()
        } else {
            try self.write { (txn) -> Int in
                try self.env.createDatabase(txn: txn, named: indexName)
                return 0
            }
        }
    
        for graph in self.graphs() {
            let sets = try TypeDataSet(self, in: graph)
            guard let gid = try self.id(for: graph) else {
                throw DiomedeError.nonExistentTermError
            }
            
            var pairs = [(Data, Data)]()
            for (i, cs) in sets.enumerated() {
                let key = [Int(gid), i]
                let keyData = key.asData()
                let valueData = try cs.asData()
                pairs.append((keyData, valueData))
            }

            try self.write(mtimeHeaders: ["TypeSets-Last-Modified"]) { (txn) -> Int in
                let index = self.env.database(txn: txn, named: indexName)!
                try index.insert(txn: txn, uniqueKeysWithValues: pairs)
                return 0
            }
        }
    }
}

extension TypeIDSet {
    public func asData() throws -> Data {
        // NOTE: this does not contain the graph ID, which is serialized in the key
        var value = [count]
        for type in types {
            value.append(Int(type))
        }
        let valueData = value.asData()
        return valueData
    }
    
    public static func fromData(_ data: Data, in gid: TermID) throws -> Self {
        var values = [Int].fromData(data)
        let count = values[0]
        values.removeFirst()
        let types = Set(values.map { UInt64($0) })
        let cs = TypeIDSet(graph: gid, types: types, count: count)
        return cs
    }
}


private let hll = HyperLogLog<FNV1AHashing>(precision: 5)
private struct FNV1AHashing: IntegerHashing {
    // From <https://github.com/pNre/Sketching/blob/7bd697a0472e4760d37cc1f6ba4792086ef89946/Tests/SketchingTests/Hashing.swift>
    static var digestBitWidth: Int {
        return UInt32.bitWidth
    }

    static func hash<S>(_ value: S) -> AnySequence<UInt32> where S : Sequence, S.Element == UInt8 {

        return hash(value, upperBound: .max)
    }

    static func hash<S>(_ value: S, upperBound: UInt32) -> AnySequence<UInt32> where S : Sequence, S.Element == UInt8 {
        let a = hash(value, offsetBasis: 2166136261)
        let b = hash(value, offsetBasis: 3560826425)
        return AnySequence(sequence(state: 1) { (i) -> UInt32? in
            let hash = UInt32((UInt64(a) + UInt64(i) * UInt64(b)) % UInt64(upperBound))
            i += 1
            return hash
        })
    }

    private static func hash<S: Sequence>(_ val: S, offsetBasis: UInt32) -> UInt32 where S.Element == UInt8 {
        var hash: UInt32 = offsetBasis
        for byte in val {
            hash ^= UInt32(byte)
            hash = hash &* 16777619
        }
        return hash
    }
}
