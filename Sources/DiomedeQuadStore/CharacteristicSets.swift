//
//  CharacteristicSets.swift
//  DiomedeQuadStore
//
//  Created by Gregory Todd Williams on 5/27/20.
//

import Foundation
import Diomede
import SPARQLSyntax

struct JoinSelectivities {
    var ss: Double
    var sp: Double
    var so: Double
    var pp: Double
    var po: Double
    var oo: Double
}

/// Statistics related to a predicate within the context of a Characteristic Set
public struct PredicateCount: Codable {
    /// The total number of triples using this predicate within this Characteristic Set
    public var sum: Int
    
    /// The minimum number of triples with this predicate for any subject within this Characteristic Set
    public var min: Int
    
    /// The maximum number of triples with this predicate for any subject within this Characteristic Set
    public var max: Int

    // NOTE: whenever new variables are added here, they must be serialized and deserialized
    //       in the CharacteristicIDSet extension below that implements asData()/fromData().

    
    /// Update statistics for this predicate based on the observation of a CS star, with a specified number of triples using this predicate
    /// - Parameter count: The number of triples sharing a subject with this predicate observed in the data
    public mutating func addMultiplicity(_ count: Int) {
        self.sum += count
        self.min = Swift.min(self.min, count)
        self.max = Swift.max(self.max, count)
    }
    
    public mutating func formUnion(_ other: PredicateCount) {
        self.sum += other.sum
        self.min = Swift.min(self.min, other.min)
        self.max = Swift.max(self.max, other.max)
    }

    public func union(_ other: PredicateCount) -> PredicateCount {
        var u = PredicateCount(sum: 0, min: 0, max: 0)

        u.sum += self.sum
        u.sum += other.sum
        
        return u
    }
}

public struct CharacteristicIDSet: Codable {
    public typealias TermID = UInt64
    public var graph: TermID
    public var count: Int
    public var predCounts: [TermID: PredicateCount]
    // NOTE: whenever new variables are added here, they must be serialized and deserialized
    //       in the CharacteristicIDSet extension below that implements asData()/fromData().

    public init(graph: TermID) {
        self.count = 0
        self.graph = graph
        self.predCounts = [:]
    }

    public init(graph: TermID, predicates: Set<TermID>, count: Int, predCounts: [TermID: PredicateCount]) {
        self.count = count
        self.graph = graph
        self.predCounts = predCounts
    }

    public mutating func formUnion(_ other: CharacteristicIDSet) {
        self.count += other.count
        for (tid, pc) in other.predCounts {
            self.predCounts[tid, default: PredicateCount(sum: 0, min: 0, max: 0)].formUnion(pc)
        }
    }

    public func union(_ other: CharacteristicIDSet) -> CharacteristicIDSet {
        let count = self.count + other.count
        var predCounts = self.predCounts
        predCounts.merge(other.predCounts) { $0.union($1) }
        let preds = Set(predCounts.keys)
        return CharacteristicIDSet(graph: self.graph, predicates: preds, count: count, predCounts: predCounts)
    }

    public mutating func addStar(_ quadids: [[TermID]]) {
        // caller is responsible for ensuring that all added stars have the same predicates
        self.count += 1
        let grouped = Dictionary(grouping: quadids, by: { $0[1] })
        for (pid, quadids) in grouped {
            let count = quadids.count
            predCounts[pid, default: PredicateCount(sum: 0, min: Int.max, max: Int.min)].addMultiplicity(count)
        }
    }
    
    var predicates: Set<TermID> {
        return Set(predCounts.keys)
    }
    
    func isSuperset(of subset: CharacteristicIDSet) -> Bool {
        return predicates.isSuperset(of: subset.predicates)
    }
}

public struct CharacteristicSet: Codable {
    public var count: Int
    public var predCounts: [Term: PredicateCount]
    
    public init(_ cs: CharacteristicIDSet, from store: DiomedeQuadStore) {
        self.count = cs.count
        self.predCounts = [:]
        for (tid, predcount) in cs.predCounts {
            let terms = store.termIterator(fromIds: [tid])
            let term = terms.next()!
            self.predCounts[term] = predcount
        }
    }
    
    public init(predicates: Set<Term>) {
        self.count = 0
        self.predCounts = Dictionary(uniqueKeysWithValues: predicates.map { ($0, PredicateCount(sum: 1, min: 1, max: 1)) })
    }

    public init(predicates: Set<Term>, count: Int, predCounts: [Term: PredicateCount]) {
        self.count = count
        self.predCounts = predCounts
    }

    public mutating func formUnion(_ other: CharacteristicSet) {
        self.count += other.count
        for (tid, pc) in other.predCounts {
            self.predCounts[tid, default: PredicateCount(sum: 0, min: 0, max: 0)].formUnion(pc)
        }
    }

    public func union(_ other: CharacteristicSet) -> CharacteristicSet {
        let count = self.count + other.count
        var predCounts = self.predCounts
        predCounts.merge(other.predCounts) { $0.union($1) }
        let preds = Set(predCounts.keys)
        return CharacteristicSet(predicates: preds, count: count, predCounts: predCounts)
    }

    public var predicates: Set<Term> {
        return Set(predCounts.keys)
    }
    
    public func isSuperset(of subset: CharacteristicSet) -> Bool {
        return predicates.isSuperset(of: subset.predicates)
    }
}

public struct CharacteristicDataSet {
    typealias TermID = UInt64
    var characteristicSets: [CharacteristicIDSet]
    var store: DiomedeQuadStore

    public var sets: [CharacteristicSet] {
        return characteristicSets.map {
            CharacteristicSet($0, from: store)
        }
    }
    
    public init(_ store: DiomedeQuadStore, characteristicSets: [CharacteristicIDSet]) throws {
        self.store = store
        self.characteristicSets = characteristicSets
    }
    
    public init(_ store: DiomedeQuadStore, in graph: Term) throws {
        self.store = store
        if let bestIndex = try store.bestIndex(matchingBoundPositions: [0, 3]) {
            let order = bestIndex.order()
            if order[0] == 3 && order[1] == 0 {
                // we can access triples sorted by subject, meaning we can pipeline the first grouping without keeping all triples in memory
                self.characteristicSets = try CharacteristicDataSet.generateCharacteristicSets_ordered(store: store, using: bestIndex, in: graph)
                return
            }
        }
        self.characteristicSets = try CharacteristicDataSet.generateCharacteristicSets_naive(store: store, in: graph)
    }
    
    static func generateCharacteristicSets_ordered(store: DiomedeQuadStore, using index: DiomedeQuadStore.IndexOrder, in graph: Term) throws -> [CharacteristicIDSet] {
        var characteristicSets = [CharacteristicIDSet]()
        var lastSubject: TermID? = nil
        var triples = [[TermID]]()
        var css = [Set<TermID>: CharacteristicIDSet]()
        
        var qp = QuadPattern.all
        qp.graph = .bound(graph)

        guard case .bound(let graph) = qp.graph, let gid = try store.id(for: graph) else {
            throw DiomedeError.indexError
        }
        
        let quadIds = try store.quadIds(usingIndex: index, withPrefix: [gid])
        for tids in quadIds {
            let t = tids[0..<3]
            let s = t[0]
            if let last = lastSubject, last != s {
                let predicates = triples.map { $0[1] }
                let set = Set(predicates)
                
                css[set, default: CharacteristicIDSet(graph: gid)].addStar(triples)
                triples = []
            }
            triples.append(Array(t))
            lastSubject = s
        }
        
        // handle the remaining triples
        if !triples.isEmpty {
            let predicates = triples.map { $0[1] }
            let set = Set(predicates)

            css[set, default: CharacteristicIDSet(graph: gid)].addStar(triples)
        }
        
        characteristicSets.append(contentsOf: css.values)
        return characteristicSets
    }
    
    static func generateCharacteristicSets_naive(store: DiomedeQuadStore, in graph: Term) throws -> [CharacteristicIDSet] {
        var characteristicSets = [CharacteristicIDSet]()
        var triples = [TermID: [[TermID]]]()
        var css = [Set<TermID>: CharacteristicIDSet]()

        var qp = QuadPattern.all
        qp.graph = .bound(graph)

        for tids in try store.quadIds(matching: qp) {
            let t = tids[0..<3]
            let s = t[0]
            triples[s, default: []].append(Array(t))
        }
        
        guard let gid = try store.id(for: graph) else {
            throw DiomedeError.indexError
        }
        
        for (_, triples) in triples {
            let predicates = triples.map { $0[1] }
            let set = Set(predicates)
            css[set, default: CharacteristicIDSet(graph: gid)].addStar(triples)
        }
        
        characteristicSets.append(contentsOf: css.values)
        return characteristicSets
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
        return characteristicSets.reduce(0, { $0 + $1.count })
    }

    public func aggregatedCharacteristicSet(matching bgp: [TriplePattern], in graph: Term, store: DiomedeQuadStore) throws -> CharacteristicIDSet {
        guard let gid = try store.id(for: graph) else {
            throw DiomedeError.indexError
        }
        let subset = try self.characteristicIDSet(matching: bgp, in: graph, store: store)
        let matching = characteristicSets.filter { $0.isSuperset(of: subset) }
        let acs = matching.reduce(CharacteristicIDSet(graph: gid)) { $0.union($1) }
        return acs
    }
    
    public func characteristicIDSet(matching bgp: [TriplePattern], in graph: Term, store: DiomedeQuadStore) throws -> CharacteristicIDSet {
        let q = bgp
        let sq = q.map { $0.predicate }.compactMap { (node) -> Term? in
            if case .bound(let term) = node {
                return term
            } else {
                return nil
            }
        }
        
        var termIds = [UInt64]()
        try store.env.read { (txn) -> Int in
            for term in sq {
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
        
        var subset = CharacteristicIDSet(graph: gid)
        subset.addStar(termIds.map { [0, $0, 0, 0] })
        // the counts don't matter in this CharacteristicIDSet because it will only be used
        // in a subsequent code to match supersets (which will contain real counts of the data).
        return subset
    }
    
    public func starCardinality(matching bgp: [TriplePattern], in graph: Term, store: DiomedeQuadStore) throws -> Double {
        let subset = try self.characteristicIDSet(matching: bgp, in: graph, store: store)
        let q = bgp

        var card = 0.0
        let matching = characteristicSets.filter { $0.isSuperset(of: subset) }
//        let subsetPreds = store.termIterator(fromIds: Array(subset.predicates)).map { $0.description }.sorted()
//        print("\(matching.count) characteristic sets match: \(subsetPreds)")
        for set in matching {
//            let cs = CharacteristicSet(set, from: store)
//            print("matching set: \(cs)")
            let distinct = Double(set.count)
            var m = 1.0
            var o = 1.0
            for t in q {
                let pred = t.predicate
                let obj = t.object
                if case .bound(let obj) = obj {
                    o = try Swift.min(o, self.selectivity(of: obj, given: pred, in: graph, store: store))
                } else if case .bound(let pred) = pred {
                    guard let pid = try store.id(for: pred) else {
                        throw DiomedeError.nonExistentTermError
                    }
                    let tm = Double(set.predCounts[pid]?.sum ?? 0) / distinct
//                    print("\(tm) <= \(t)")
                    m *= tm
                } else {
                    // unbound predicate; sum up all the counts
                    let allPredCounts = set.predCounts.values.map { Double($0.sum) }.reduce(0.0) { $0 + $1 }
                    let tm = allPredCounts / distinct
//                    print("\(tm) <= \(t)")
                    m *= tm
                }
            }
            let prod = distinct * m * o
//            print("\(distinct) * \(m) * \(o) = \(prod)")
            card += prod
        }
//        print("= \(card)")
        return card
    }
    
}

extension CharacteristicDataSet: Sequence {
    public func makeIterator() -> AnyIterator<CharacteristicIDSet> {
        return AnyIterator(characteristicSets.makeIterator())
    }
}

extension CharacteristicSet: CustomDebugStringConvertible {
    public var debugDescription: String {
        return "CharacteristicSet(\(count); \(predicates.sorted()))"
    }
}

extension CharacteristicIDSet: CustomDebugStringConvertible {
    public var debugDescription: String {
        return "CharacteristicIDSet(\(count); \(predicates.sorted()))"
    }
}

extension CharacteristicDataSet: CustomDebugStringConvertible {
    public var debugDescription: String {
        var s = "Characteristic Sets [\n"
        for set in characteristicSets.sorted(by: { (a, b) in return a.count > b.count }) {
            s += "\t\(set.debugDescription)\n"
        }
        s += "]\n"
        return s
    }
}

extension DiomedeQuadStore {
    public var hasCharacteristicSets: Bool {
        let indexName = "characteristicSets"
        
        do {
            let databases = Set(try env.databases())
            return databases.contains(indexName)
        } catch {
            return false
        }
    }
    
    public var characteristicSetsAreAccurate: Bool {
        guard hasCharacteristicSets else { return false }
        let csHeader = "CharacteristicSets-Last-Modified"
        let quadsHeader = "Quads-Last-Modified"
        if let csDate = self.read(mtimeHeader: csHeader), let quadsDate = self.read(mtimeHeader: quadsHeader) {
            return csDate >= quadsDate
        }
        return false
    }
    
    public func characteristicSets(for graph: Term) throws -> CharacteristicDataSet {
        let indexName = "characteristicSets"
        
        guard let index = self.env.database(named: indexName) else {
            throw DiomedeError.indexError
        }
        guard let gid = try self.id(for: graph) else {
            throw DiomedeError.nonExistentTermError
        }

        let lower = [Int(gid), 0].asData()
        let upper = [Int(gid+1), 0].asData()
        
        var sets = [CharacteristicIDSet]()
        try index.iterate(between: lower, and: upper) { (k, v) in
            let key = [Int].fromData(k)
            guard key[0] == gid else {
                return
            }
            let cs = try CharacteristicIDSet.fromData(v, in: gid)
            sets.append(cs)
        }
        return try CharacteristicDataSet(self, characteristicSets: sets)
    }
    
    public func dropCharacteristicSets() throws {
        let indexName = "characteristicSets"
        let databases = Set(try env.databases())
        if databases.contains(indexName) {
//            print("dropping \(indexName)...")
            if let index = self.env.database(named: indexName) {
                try index.drop()
                try self.touch(mtimeHeaders: ["CharacteristicSets-Last-Modified"]) // update the last-modified timestamp
            }
        } else {
//            print("no-op")
        }
    }
    
    public func computeCharacteristicSets() throws {
        let indexName = "characteristicSets"
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
            let sets = try CharacteristicDataSet(self, in: graph)
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

            try self.write(mtimeHeaders: ["CharacteristicSets-Last-Modified"]) { (txn) -> Int in
                let index = self.env.database(txn: txn, named: indexName)!
                try index.insert(txn: txn, uniqueKeysWithValues: pairs)
                return 0
            }
        }
    }
}

extension CharacteristicIDSet {
    public func asData() throws -> Data {
        // NOTE: this does not contain the graph ID, which is serialized in the key
        var value = [count]
        for (pred, count) in predCounts {
            value.append(contentsOf: [Int(pred), count.sum, count.min, count.max])
        }
        let valueData = value.asData()
        return valueData
    }
    
    public static func fromData(_ data: Data, in gid: TermID) throws -> Self {
        var values = [Int].fromData(data)
        let count = values[0]
        values.removeFirst()
        let pairs = stride(from: 0, to: values.endIndex, by: 4).map {
            (UInt64(values[$0]), PredicateCount(sum: values[$0.advanced(by: 1)], min: values[$0.advanced(by: 2)], max: values[$0.advanced(by: 3)]))
        }
        let predCounts = Dictionary(uniqueKeysWithValues: pairs)
        let preds = Set(predCounts.keys)
        let cs = CharacteristicIDSet(graph: gid, predicates: preds, count: count, predCounts: predCounts)
        return cs
    }
}
