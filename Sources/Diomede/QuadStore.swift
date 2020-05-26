//
//  QuadStore.swift
//  Diomede
//
//  Created by Gregory Todd Williams on 5/23/20.
//

import Foundation
import CryptoKit

import SPARQLSyntax
import Kineo

public struct DiomedeQuadStore {
    public enum IndexOrder: String {
        case spog
        case spgo
        case sopg
        case sogp
        case sgpo
        case sgop
        case psog
        case psgo
        case posg
        case pogs
        case pgso
        case pgos
        case ospg
        case osgp
        case opsg
        case opgs
        case ogsp
        case ogps
        case gspo
        case gsop
        case gpso
        case gpos
        case gosp
        case gops
        
        public func order() -> [Int] {
            let positions : [Character: Int] = [
                "s": 0,
                "p": 1,
                "o": 2,
                "g": 3
            ]
            let name = self.rawValue
            var order = [Int]()
            for c in name {
                guard let p = positions[c] else {
                    fatalError()
                }
                order.append(p)
            }
            return order
        }
    }
    
    public enum StaticDatabases: String {
        case fullIndexes
        case stats
        case id_to_term
        case term_to_id
        case graphs
    }
    
    var env: Environment
    var t2i: Environment.Database
    var i2t: Environment.Database
    var indexes: Environment.Database
    var stats: Environment.Database
    var graphs: Environment.Database
    var next_unassigned_id: Int
    var fullIndexes: [IndexOrder: (Environment.Database, [Int])]
    
    private init?(environment e: Environment) {
        self.env = e
        
        guard let indexes = e.database(named: StaticDatabases.fullIndexes.rawValue),
            let stats = e.database(named: StaticDatabases.stats.rawValue),
            let i2t = e.database(named: StaticDatabases.id_to_term.rawValue),
            let t2i = e.database(named: StaticDatabases.term_to_id.rawValue),
            let graphs = e.database(named: StaticDatabases.graphs.rawValue) else { return nil }
        self.i2t = i2t
        self.t2i = t2i
        self.stats = stats
        self.graphs = graphs
        self.indexes = indexes
        
        self.fullIndexes = [:]
        do {
            self.next_unassigned_id = try self.stats.get(key: "next_unassigned_id").map { Int.fromData($0) } ?? 1
            var indexPairs = [(IndexOrder, [Int])]()
            try indexes.iterate { (k, v) in
                let name = try String.fromData(k)
                guard let key = IndexOrder(rawValue: name) else {
                    throw DiomedeError.indexError
                }
                let order = Array<Int>.fromData(v)
                indexPairs.append((key, order))
            }
            
            for (key, order) in indexPairs {
                let name = key.rawValue
                guard let idb = e.database(named: name) else {
                    throw DiomedeError.indexError
                }
                self.fullIndexes[key] = (idb, order)
            }
        } catch {
            return nil
        }
        guard !self.fullIndexes.isEmpty else {
            return nil
        }
    }

    public init?(path: String, create: Bool = false) {
        if create {
            do {
                let f = FileManager.default
                if !f.fileExists(atPath: path) {
                    try f.createDirectory(atPath: path, withIntermediateDirectories: true, attributes: nil)
                }
                guard let e = Environment(path: path) else {
                    fatalError()
                }
                
                try e.write { (txn) -> Int in
                    try e.createDatabase(txn: txn, named: StaticDatabases.stats.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.fullIndexes.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.term_to_id.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.id_to_term.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.graphs.rawValue)
                    try e.createDatabase(txn: txn, named: "spog") // spog is required
                    try e.createDatabase(txn: txn, named: "gpso") // all other index orders are optional
                    let stats = e.database(txn: txn, named: StaticDatabases.stats.rawValue)!
                    let indexes = e.database(txn: txn, named: StaticDatabases.fullIndexes.rawValue)!
                    try indexes.insert(uniqueKeysWithValues: [
                        ("spog", [0,1,2,3]),
                        ("gpso", [3,1,0,2])
                    ])
                    
                    let now = ISO8601DateFormatter().string(from: Date.init())
                    try stats.insert(uniqueKeysWithValues: [
                        ("Version", "0.0.1"),
                        ("meta", ""),
                        ("Last-Modified", now)
                    ])
                    try stats.insert(uniqueKeysWithValues: [
                        ("next_unassigned_id", 1)
                    ])
                    return 0
                }
                
                self.init(environment: e)
            } catch {
                return nil
            }
        } else {
            guard let e = Environment(path: path) else { return nil }
            self.init(environment: e)
        }
    }

    private func read(handler: (OpaquePointer) throws -> Int) throws {
        try self.env.read(handler: handler)
    }

    private func write(handler: (OpaquePointer) throws -> Int) throws {
        try self.env.write(handler: handler)
    }

    public mutating func dropFullIndex(order indexOrder: IndexOrder) throws {
        guard indexOrder != .spog, let _ = self.fullIndexes[indexOrder] else {
            throw DiomedeError.indexError
        }

        let indexName = indexOrder.rawValue
        try self.write { (txn) -> Int in
            self.fullIndexes.removeValue(forKey: indexOrder)
            try self.indexes.delete(txn: txn, key: indexName)
            try self.env.dropDatabase(txn: txn, named: indexName)
            return 0
        }
    }
    
    public func addFullIndex(order indexOrder: IndexOrder) throws {
        if let _ = self.fullIndexes[indexOrder] {
            return
        }
        let indexName = indexOrder.rawValue
        let order = indexOrder.order()

        var quads = [[Int]]()
        try self.read { (txn) throws -> Int in
            try self.iterateQuadIds(txn: txn, usingIndex: .spog) { (tids) in
                quads.append(tids)
            }
            return 0
        }

        let emptyValue = Data()
        let indexOrderedKeys = quads.map { (q) in order.map({ q[$0].asData() }).reduce(Data()) { $0 + $1 } }
        let indexOrderedPairs = indexOrderedKeys.map { ($0, emptyValue) }
        try self.write { (txn) -> Int in
            try self.env.createDatabase(txn: txn, named: indexName)
            let index = self.env.database(txn: txn, named: indexName)!
            try index.insert(txn: txn, uniqueKeysWithValues: indexOrderedPairs)
            try self.indexes.insert(txn: txn, uniqueKeysWithValues: [
                (indexName, order),
            ])
            return 0
        }
    }
    
    private func bestIndex(matchingBoundPositions: Set<Int>, txn: OpaquePointer) throws -> IndexOrder {
        let bound = matchingBoundPositions
        var scores = [(Int, IndexOrder)]()
        for (k, v) in self.fullIndexes {
            let order = v.1
            var score = 0
            for pos in order {
                if bound.contains(pos) {
                    score += 1
                } else {
                    break
                }
            }
            scores.append((score, k))
        }
        
        scores.sort(by: { $0.0 > $1.0 })
        guard let first = scores.first else {
            throw DiomedeError.indexError
        }
        let indexOrder = first.1
        return indexOrder
    }

    private func iterateQuadIds(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, withPrefix prefix: [Int], handler: ([Int]) throws -> ()) throws {
        guard !prefix.isEmpty else {
            return try self.iterateQuadIds(txn: txn, usingIndex: indexOrder, handler: handler)
        }
        guard let (index, order) = self.fullIndexes[indexOrder] else {
            throw DiomedeError.indexError
        }
        print("using index \(indexOrder.rawValue) with order \(order)")
        let iterationHandler = { (qidsData: Data, _: Data) throws -> () in
            var tids = Array<Int>(repeating: 0, count: 4)
            let strideBy = qidsData.count / 4
            for (pos, i) in zip(order, stride(from: 0, to: qidsData.count, by: strideBy)) {
                let data = qidsData[i..<(i+strideBy)]
                let tid = Int.fromData(data)
                tids[pos] = tid
            }
            try handler(tids)
        }
        
        let empty = Array(repeating: 0, count: 4)
        let lower = Array((prefix + empty).prefix(4))
        var upper = lower
        upper[prefix.count - 1] += 1

        let lowerKey = lower.asData()
        let upperKey = upper.asData()
//        print("from \(lowerKey._hexValue)")
//        print("to   \(upperKey._hexValue)")
        try index.iterate(txn: txn, between: lowerKey, and: upperKey, inclusive: false, handler: iterationHandler)
    }

    private func iterateQuadIds(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: ([Int]) throws -> ()) throws {
        guard let (index, order) = self.fullIndexes[indexOrder] else {
            throw DiomedeError.indexError
        }
        let iterationHandler = { (qidsData: Data, _: Data) throws -> () in
            var tids = Array<Int>(repeating: 0, count: 4)
            let strideBy = qidsData.count / 4
            for (pos, i) in zip(order, stride(from: 0, to: qidsData.count, by: strideBy)) {
                let data = qidsData[i..<(i+strideBy)]
                let tid = Int.fromData(data)
                tids[pos] = tid
            }
            try handler(tids)
        }
        
        try index.iterate(txn: txn, handler: iterationHandler)
    }

    private func iterateQuads(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, withPrefix prefix: [Int], handler: (Quad) throws -> ()) throws {
        let cache = LRUCache<Int, Term>(capacity: 4_096)
        try iterateQuadIds(txn: txn, usingIndex: indexOrder, withPrefix: prefix) { (tids) throws in
            var terms = [Term]()
            for tid in tids {
                if let term = cache[tid] {
                    terms.append(term)
                } else if let tdata = try i2t.get(txn: txn, key: tid) {
                    let term = try Term.fromData(tdata)
                    terms.append(term)
                    cache[tid] = term
                } else {
                    print("iterateQuads[]: no term for ID \(tid)")
                    return
                }
            }
            if terms.count == 4 {
                let q = Quad(subject: terms[0], predicate: terms[1], object: terms[2], graph: terms[3])
                try handler(q)
            } else {
                print("*** Bad quad")
            }
        }
    }

    private func iterateQuads(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: (Quad) throws -> ()) throws {
        let cache = LRUCache<Int, Term>(capacity: 4_096)
        try iterateQuadIds(txn: txn, usingIndex: indexOrder) { (tids) throws in
            var terms = [Term]()
            for tid in tids {
                if let term = cache[tid] {
                    terms.append(term)
                } else if let tdata = try i2t.get(txn: txn, key: tid) {
                    let term = try Term.fromData(tdata)
                    terms.append(term)
                    cache[tid] = term
                } else {
                    print("iterateQuads: no term for ID \(tid)")
                    return
                }
            }
            if terms.count == 4 {
                let q = Quad(subject: terms[0], predicate: terms[1], object: terms[2], graph: terms[3])
                try handler(q)
            } else {
                print("*** Bad quad")
            }
        }
    }

    private func iterateNamedGraphs(txn: OpaquePointer, handler: (Term) throws -> ()) throws {
        try graphs.iterate(txn: txn) { (data: Data, _: Data) throws -> () in
            let gid = Int.fromData(data)
            if let tdata = try i2t.get(txn: txn, key: gid) {
                let term = try Term.fromData(tdata)
                try handler(term)
            } else {
                print("no term for graph ID \(gid)")
                return
            }
        }
    }

    public func quads(matching pattern: QuadPattern) throws -> AnyIterator<Quad> {
        var quads = [Quad]()
        do {
            try self.env.read { (txn) -> Int in
                var boundPositions = Set<Int>()
                for (i, n) in pattern.enumerated() {
                    if case .bound(_) = n {
                        boundPositions.insert(i)
                    }
                }
                let index = try self.bestIndex(matchingBoundPositions: boundPositions, txn: txn)
//                print("Best index order is \(index.rawValue)")
                let order = index.order()
                var prefix = [Int]()
                let nodes = Array(pattern)
                
                for i in order {
                    let node = nodes[i]
                    guard case .bound(let term) = node else {
                        break
                    }
                    guard let tid = try self.id(for: term, txn: txn) else {
                        throw DiomedeError.nonExistentTermError
                    }
                    prefix.append(tid)
                }
                
                if prefix.isEmpty {
                    print("matching all quads")
                    try self.iterateQuads(txn: txn, usingIndex: index) { (q) in
                        if pattern.matches(q) {
                            quads.append(q)
                        }
                    }
                } else {
                    print("matching quads with prefix: \(prefix)")
                    try self.iterateQuads(txn: txn, usingIndex: index, withPrefix: prefix) { (q) in
                        if pattern.matches(q) {
                            quads.append(q)
                        }
                    }
                }
                return 0
            }
        } catch DiomedeError.nonExistentTermError {
            return AnyIterator([].makeIterator())
        }
        return AnyIterator(quads.makeIterator())
    }
    
    public func namedGraphs() throws -> [Term] {
        var terms = [Term]()
        try self.env.read { (txn) throws -> Int in
            try self.iterateNamedGraphs(txn: txn) { (graph) in
                terms.append(graph)
            }
            return 0
        }
        return terms
    }
    
    private func id(for term: Term, txn: OpaquePointer) throws -> Int? {
        let d = try term.asData()
        let term_key = Data(SHA256.hash(data: d))
        if let eid = try self.t2i.get(txn: txn, key: term_key) {
            return Int.fromData(eid)
        } else {
            return nil
        }
    }
    
    public func loadRDF(from url: URL) throws {
        let parser = RDFParserCombined()
        let graph = Term(iri: url.absoluteString)
        guard let (spog, _) = self.fullIndexes[.spog] else {
            throw DiomedeError.indexError
        }

        try env.write { (txn) -> Int in
            let nextData = try stats.get(txn: txn, key: "next_unassigned_id")
            var next = nextData.map { Int.fromData($0) } ?? 1
            
            var graphIds = Set<Int>()
            var quads = [[Int]]()
            var terms = Set<Term>()
            try parser.parse(file: url.path, base: graph.value) { (s, p, o) in
                let q = Quad(subject: s, predicate: p, object: o, graph: graph)
                do {
                    var quadIds = [Int]()
                    for (i, t) in q.enumerated() {
                        terms.insert(t)
                        let d = try t.asData()
                        
                        let term_key = Data(SHA256.hash(data: d))
                        var tid: Int
                        if let eid = try self.t2i.get(txn: txn, key: term_key) {
                            tid = Int.fromData(eid)
                        } else {
                            tid = next
                            let i2t_pair = (tid, d)
                            let t2i_pair = (Data(SHA256.hash(data: d)), tid)
                            try self.i2t.insert(txn: txn, uniqueKeysWithValues: [i2t_pair])
                            try self.t2i.insert(txn: txn, uniqueKeysWithValues: [t2i_pair])
                            next += 1
                        }

                        quadIds.append(tid)
                        if (i == 3) {
                            graphIds.insert(tid)
                        }
                    }
                    assert(quadIds.count == 4)
                    quads.append(quadIds)
                } catch {}
            }
            
            let graphIdPairs = graphIds.map { ($0, Data()) }
            try self.graphs.insert(txn: txn, uniqueKeysWithValues: graphIdPairs)
            
            try stats.insert(txn: txn, uniqueKeysWithValues: [
                ("next_unassigned_id", next),
            ])

            let spog_keys = quads.map { (q) in q.map { $0.asData() }.reduce(Data()) { $0 + $1 } }
            let emptyValue = Data()
            let spog_pairs = spog_keys.map { ($0, emptyValue) }
            try spog.insert(txn: txn, uniqueKeysWithValues: spog_pairs)

            for (key, pair) in self.fullIndexes {
                guard key != .spog else { continue }
                let (index, order) = pair
                let indexOrderedKeys = quads.map { (q) in order.map({ q[$0].asData() }).reduce(Data()) { $0 + $1 } }
                let indexOrderedPairs = indexOrderedKeys.map { ($0, emptyValue) }
                try index.insert(txn: txn, uniqueKeysWithValues: indexOrderedPairs)
            }
            
            return 0
        }
    }
}

extension DiomedeQuadStore {
    public func _private_bestIndex(matchingBoundPositions positions: Set<Int>, txn: OpaquePointer) throws -> IndexOrder {
        return try bestIndex(matchingBoundPositions: positions, txn: txn)
    }
    
    public func _private_iterateQuadIds(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: ([Int]) throws -> ()) throws {
        return try iterateQuadIds(txn: txn, usingIndex: indexOrder, handler: handler)
    }

    public func _private_iterateQuads(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: (Quad) throws -> ()) throws {
        return try iterateQuads(txn: txn, usingIndex: indexOrder, handler: handler)
    }
}
