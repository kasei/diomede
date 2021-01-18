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

public struct CharacteristicIDSet: Codable {
    public typealias TermID = UInt64
    public var graph: TermID
    public var count: Int
    public var predCounts: [TermID: Int]
    
    init(graph: TermID, predicates: Set<TermID>) {
        self.count = 0
        self.graph = graph
        self.predCounts = Dictionary(uniqueKeysWithValues: predicates.map { ($0, 1) })
    }

    init(graph: TermID, predicates: Set<TermID>, count: Int, predCounts: [TermID: Int]) {
        self.count = count
        self.graph = graph
        self.predCounts = predCounts
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
    public var predCounts: [Term: Int]
    
    init(_ cs: CharacteristicIDSet, from store: DiomedeQuadStore) {
        self.count = cs.count
        self.predCounts = [:]
        for (tid, count) in cs.predCounts {
            let terms = store.termIterator(fromIds: [tid])
            let term = terms.next()!
            self.predCounts[term] = count
        }
    }
    
    init(predicates: Set<Term>) {
        self.count = 0
        self.predCounts = Dictionary(uniqueKeysWithValues: predicates.map { ($0, 1) })
    }

    init(predicates: Set<Term>, count: Int, predCounts: [Term: Int]) {
        self.count = count
        self.predCounts = predCounts
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
        var counts = [Set<TermID>: Int]()
        var predCounts = [Set<TermID>: [TermID: Int]]()
        
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
                counts[set, default: 0] += 1
                
                for t in triples {
                    let p = t[1]
                    predCounts[set, default: [:]][p, default: 0] += 1
                }
                
                triples = []
            }
            triples.append(Array(t))
            lastSubject = s
        }
        
        // handle the remaining triples
        if !triples.isEmpty {
            let predicates = triples.map { $0[1] }
            let set = Set(predicates)
            counts[set, default: 0] += 1
            
            for t in triples {
                let p = t[1]
                predCounts[set, default: [:]][p, default: 0] += 1
            }
        }
        
        for (set, count) in counts {
            let pcounts = predCounts[set, default: [:]]
            let cs = CharacteristicIDSet(graph: gid, predicates: set, count: count, predCounts: pcounts)
            characteristicSets.append(cs)
        }
        return characteristicSets
    }
    
    static func generateCharacteristicSets_naive(store: DiomedeQuadStore, in graph: Term) throws -> [CharacteristicIDSet] {
        var characteristicSets = [CharacteristicIDSet]()
        var triples = [TermID: [[TermID]]]()

        var qp = QuadPattern.all
        qp.graph = .bound(graph)

        for tids in try store.quadIds(matching: qp) {
            let t = tids[0..<3]
            let s = t[0]
            triples[s, default: []].append(Array(t))
        }
        
        var counts = [Set<TermID>: Int]()
        var predCounts = [Set<TermID>: [TermID: Int]]()
        for (_, triples) in triples {
            let predicates = triples.map { $0[1] }
            let set = Set(predicates)
            counts[set, default: 0] += 1
            
            for t in triples {
                let p = t[1]
                predCounts[set, default: [:]][p, default: 0] += 1
            }
        }
        
        guard let gid = try store.id(for: graph) else {
            throw DiomedeError.indexError
        }
        
        for (set, count) in counts {
            let pcounts = predCounts[set, default: [:]]
            let cs = CharacteristicIDSet(graph: gid, predicates: set, count: count, predCounts: pcounts)
            characteristicSets.append(cs)
        }
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
        let subset = CharacteristicIDSet(graph: gid, predicates: Set(termIds))
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
                    let tm = Double(set.predCounts[pid] ?? 0) / distinct
//                    print("\(tm) <= \(t)")
                    m *= tm
                } else {
                    // unbound predicate; sum up all the counts
                    let allPredCounts = set.predCounts.values.map { Double($0) }.reduce(0.0) { $0 + $1 }
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
        return "CharacteristicSet(\(count); \(predicates.sorted()))"
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
            var values = [Int].fromData(v)
            guard key[0] == gid else {
                return
            }
//            let i = key[1]
            let count = values[0]
            values.removeFirst()
            let pairs = stride(from: 0, to: values.endIndex, by: 2).map {
                (UInt64(values[$0]), values[$0.advanced(by: 1)])
            }
            let predCounts = Dictionary(uniqueKeysWithValues: pairs)
            let preds = Set(predCounts.keys)
            let cs = CharacteristicIDSet(graph: gid, predicates: preds, count: count, predCounts: predCounts)
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
                var value = [cs.count]
                for (pred, count) in cs.predCounts {
                    value.append(contentsOf: [Int(pred), count])
                }
                let valueData = value.asData()
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
