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
        case quads
        case fullIndexes
        case stats
        case id_to_term
        case term_to_id
        case graphs
    }
    
    var env: Environment
    var quads: Environment.Database
    var t2i: Environment.Database
    var i2t: Environment.Database
    var indexes: Environment.Database
    var stats: Environment.Database
    var graphs: Environment.Database
    var next_unassigned_id: Int
    var next_quad_id: Int
    var fullIndexes: [IndexOrder: (Environment.Database, [Int])]
    
    private init?(environment e: Environment) {
        self.env = e
        
        guard let quads = e.database(named: StaticDatabases.quads.rawValue),
            let indexes = e.database(named: StaticDatabases.fullIndexes.rawValue),
            let stats = e.database(named: StaticDatabases.stats.rawValue),
            let i2t = e.database(named: StaticDatabases.id_to_term.rawValue),
            let t2i = e.database(named: StaticDatabases.term_to_id.rawValue),
            let graphs = e.database(named: StaticDatabases.graphs.rawValue) else { return nil }
        self.quads = quads
        self.i2t = i2t
        self.t2i = t2i
        self.stats = stats
        self.graphs = graphs
        self.indexes = indexes
        
        self.fullIndexes = [:]
        do {
            self.next_unassigned_id = try self.stats.get(key: "next_unassigned_id").map { Int.fromData($0) } ?? 1
            self.next_quad_id = try self.stats.get(key: "next_quad_id").map { Int.fromData($0) } ?? 1
            var indexPairs = [(IndexOrder, [Int])]()
            try indexes.iterate { (k, v) in
                let name = try String.fromData(k)
                guard let key = IndexOrder(rawValue: name) else {
                    throw DiomedeError.indexError
                }
                let order = Array<Int>.fromData(v)
                indexPairs.append((key, order))
            }
            
            try self.env.read { (txn) -> Int in
                for (key, order) in indexPairs {
                    let name = key.rawValue
                    guard let idb = e.database(txn: txn, named: name) else {
                        throw DiomedeError.indexError
                    }
                    self.fullIndexes[key] = (idb, order)
                }
                return 0
            }
        } catch {
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
                    fatalError("Failed to construct new LMDB Environment")
                }
                
                try e.write { (txn) -> Int in
                    try e.createDatabase(txn: txn, named: StaticDatabases.quads.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.stats.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.fullIndexes.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.term_to_id.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.id_to_term.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.graphs.rawValue)
//                    try e.createDatabase(txn: txn, named: "spog") // spog is required
//                    try e.createDatabase(txn: txn, named: "gpso") // all other index orders are optional
                    let stats = e.database(txn: txn, named: StaticDatabases.stats.rawValue)!
//                    let indexes = e.database(txn: txn, named: StaticDatabases.fullIndexes.rawValue)!
//                    try indexes.insert(uniqueKeysWithValues: [
//                        ("spog", [0,1,2,3]),
//                        ("gpso", [3,1,0,2])
//                    ])
                    
                    let now = ISO8601DateFormatter().string(from: Date.init())
                    try stats.insert(txn: txn, uniqueKeysWithValues: [
                        ("Version", "0.0.1"),
                        ("meta", ""),
                        ("Last-Modified", now)
                    ])
                    try stats.insert(txn: txn, uniqueKeysWithValues: [
                        ("next_unassigned_id", 1),
                        ("next_quad_id", 1),
                    ])
                    return 0
                }
                
                self.init(environment: e)
            } catch let e {
                print("error: \(e)")
                return nil
            }
        } else {
            guard let e = Environment(path: path) else {
                fatalError("Failed to open LMDB Environment")
                return nil
            }
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
        let indexName = indexOrder.rawValue
        try self.write { (txn) -> Int in
            self.fullIndexes.removeValue(forKey: indexOrder)
            try self.indexes.delete(txn: txn, key: indexName)
            try self.env.dropDatabase(txn: txn, named: indexName)
            return 0
        }
    }
    
    private func bestIndex(matchingBoundPositions: Set<Int>, txn: OpaquePointer) throws -> IndexOrder? {
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
            return nil
        }
        let indexOrder = first.1
        return indexOrder
    }

    private func quadIdsIterator<T>(usingIndex indexOrder: IndexOrder, withPrefix prefix: [Int], handler: @escaping (OpaquePointer, Int, [Int]) -> T) throws -> AnyIterator<T> {
        guard let (index, order) = self.fullIndexes[indexOrder] else {
            throw DiomedeError.indexError
        }
        //        print("using index \(indexOrder.rawValue) with order \(order)")
        let empty = Array(repeating: 0, count: 4)
        let lower = Array((prefix + empty).prefix(4))
        var upper = lower
        upper[prefix.count - 1] += 1
        
        let lowerKey = lower.asData()
        let upperKey = upper.asData()
        //        print("from \(lowerKey._hexValue)")
        //        print("to   \(upperKey._hexValue)")
        let i = try index.iterator(between: lowerKey, and: upperKey, inclusive: false) { (txn, qidData, qidsData) -> T in
            let qid = Int.fromData(qidData)
            var tids = Array<Int>(repeating: 0, count: 4)
            let strideBy = qidsData.count / 4
            for (pos, i) in zip(order, stride(from: 0, to: qidsData.count, by: strideBy)) {
                let data = qidsData[i..<(i+strideBy)]
                let tid = Int.fromData(data)
                tids[pos] = tid
            }
            return handler(txn, qid, tids)
        }
        return AnyIterator(i)
    }
    
    private func iterateQuadIds(txn: OpaquePointer, usingIndex indexOrder: IndexOrder? = nil, handler: (Int, [Int]) throws -> ()) throws {
        if let indexOrder = indexOrder, let (index, order) = self.fullIndexes[indexOrder] {
            let iterationHandler = { (qidsData: Data, qidData: Data) throws -> () in
                let qid = Int.fromData(qidData)
                var tids = Array<Int>(repeating: 0, count: 4)
                let strideBy = qidsData.count / 4
                for (pos, i) in zip(order, stride(from: 0, to: qidsData.count, by: strideBy)) {
                    let data = qidsData[i..<(i+strideBy)]
                    let tid = Int.fromData(data)
                    tids[pos] = tid
                }
                try handler(qid, tids)
            }
            
            try index.iterate(txn: txn, handler: iterationHandler)
        } else {
            // no index given, just use the quads table
            try self.quads.iterate(txn: txn) { (qidData, qidsData) in
                let qid = Int.fromData(qidData)
                var tids = [Int]()
                let strideBy = qidsData.count / 4
                for i in stride(from: 0, to: qidsData.count, by: strideBy) {
                    let data = qidsData[i..<(i+strideBy)]
                    let tid = Int.fromData(data)
                    tids.append(tid)
                }
                try handler(qid, tids)
            }
        }
    }

    private func quad(from tids: [Int], txn: OpaquePointer, cache: LRUCache<Int, Term>? = nil) throws -> Quad? {
        var terms = [Term]()
        for tid in tids {
            if let cache = cache, let term = cache[tid] {
                terms.append(term)
            } else if let tdata = try i2t.get(txn: txn, key: tid) {
                let term = try Term.fromData(tdata)
                terms.append(term)
                if let cache = cache {
                    cache[tid] = term
                }
            } else {
                print("iterateQuads[]: no term for ID \(tid)")
                return nil
            }
        }
        if terms.count == 4 {
            return Quad(subject: terms[0], predicate: terms[1], object: terms[2], graph: terms[3])
        } else {
            print("*** Bad quad")
            return nil
        }
    }

    private func iterateQuads(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: (Quad) throws -> ()) throws {
        let cache = LRUCache<Int, Term>(capacity: 4_096)
        try iterateQuadIds(txn: txn, usingIndex: indexOrder) { (qid, tids) throws in
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

    private func id(for term: Term, txn: OpaquePointer) throws -> Int? {
        let d = try term.asData()
        let term_key = Data(SHA256.hash(data: d))
        if let eid = try self.t2i.get(txn: txn, key: term_key) {
            return Int.fromData(eid)
        } else {
            return nil
        }
    }
}


extension DiomedeQuadStore {
    public func loadRDF(from url: URL) throws {
        let parser = RDFParserCombined()
        let graph = Term(iri: url.absoluteString)

        try env.write { (txn) -> Int in
            let nextData = try stats.get(txn: txn, key: "next_unassigned_id")
            var next = nextData.map { Int.fromData($0) } ?? 1
            var next_quad_id = try stats.get(txn: txn, key: "next_quad_id").map { Int.fromData($0) } ?? 1

            var graphIds = Set<Int>()
            var quadIds = [[Int]]()
            var terms = Set<Term>()
            try parser.parse(file: url.path, base: graph.value) { (s, p, o) in
                let q = Quad(subject: s, predicate: p, object: o, graph: graph)
                do {
                    var termIds = [Int]()
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

                        termIds.append(tid)
                        if (i == 3) {
                            graphIds.insert(tid)
                        }
                    }
                    assert(termIds.count == 4)
                    quadIds.append(termIds)
                } catch {}
            }
            
            let graphIdPairs = graphIds.map { ($0, Data()) }
            try self.graphs.insert(txn: txn, uniqueKeysWithValues: graphIdPairs)
            
            try stats.insert(txn: txn, uniqueKeysWithValues: [
                ("next_unassigned_id", next),
            ])

            let quadKeys = quadIds.map { (q) in q.map { $0.asData() }.reduce(Data()) { $0 + $1 } }
            let emptyValue = Data()
            
            var quadPairs = [(Int, Data)]()
            for qkey in quadKeys {
                let qid = next_quad_id
                next_quad_id += 1
                quadPairs.append((qid, qkey))
            }
            try stats.insert(txn: txn, uniqueKeysWithValues: [
                ("next_quad_id", next_quad_id),
            ])
            try self.quads.insert(txn: txn, uniqueKeysWithValues: quadPairs)
            
            for (_, pair) in self.fullIndexes {
                let (index, order) = pair
                let indexOrderedKeys = quadIds.map { (q) in order.map({ q[$0].asData() }).reduce(Data()) { $0 + $1 } }
                let indexOrderedPairs = indexOrderedKeys.map { ($0, emptyValue) }
                try index.insert(txn: txn, uniqueKeysWithValues: indexOrderedPairs)
            }
            
            return 0
        }
    }
}

extension DiomedeQuadStore {
    public var _private_quads: Environment.Database {
        return self.quads
    }
    
    public func _private_bestIndex(matchingBoundPositions positions: Set<Int>, txn: OpaquePointer) throws -> IndexOrder? {
        return try self.bestIndex(matchingBoundPositions: positions, txn: txn)
    }
    
    public func _private_iterateQuadIds(txn: OpaquePointer, usingIndex indexOrder: IndexOrder? = nil, handler: (Int, [Int]) throws -> ()) throws {
        return try self.iterateQuadIds(txn: txn, usingIndex: indexOrder, handler: handler)
    }

    public func _private_iterateQuads(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: (Quad) throws -> ()) throws {
        return try self.iterateQuads(txn: txn, usingIndex: indexOrder, handler: handler)
    }
}

extension DiomedeQuadStore {
    public func addFullIndex(order indexOrder: IndexOrder) throws {
        if let _ = self.fullIndexes[indexOrder] {
            return
        }
        let indexName = indexOrder.rawValue
        let order = indexOrder.order()

        var quadIds = [(Int, [Int])]()
        try self.read { (txn) throws -> Int in
            try self.iterateQuadIds(txn: txn) { (qid, tids) in
                quadIds.append((qid, tids))
            }
            return 0
        }

        
        var indexOrderedPairs = [(Data, Int)]()
        for (qid, tids) in quadIds {
            let indexOrderedKey = order.map({ tids[$0].asData() }).reduce(Data()) { $0 + $1 }
            indexOrderedPairs.append((indexOrderedKey, qid))
        }
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
    
    public func namedGraphs() throws -> AnyIterator<Term> {
        let i = try self.graphs.iterator { (txn, data, _) -> Term? in
            do {
                let gid = Int.fromData(data)
                if let tdata = try self.i2t.get(txn: txn, key: gid) {
                    let term = try Term.fromData(tdata)
                    return term
                } else {
                    print("no term for graph ID \(gid)")
                    return nil
                }
            } catch {
                return nil
            }
        }
        return AnyIterator(i.lazy.compactMap { $0 }.makeIterator())
    }

    public func quadsIterator() throws -> AnyIterator<Quad> {
        let cache = LRUCache<Int, Term>(capacity: 4_096)
        let i = try self.quads.iterator { (txn, k, v) -> Quad? in
            let tids = [Int].fromData(v)
            do {
                var terms = [Term]()
                for tid in tids {
                    if let term = cache[tid] {
                        terms.append(term)
                    } else if let tdata = try self.i2t.get(txn: txn, key: tid) {
                        let term = try Term.fromData(tdata)
                        terms.append(term)
                        cache[tid] = term
                    } else {
                        print("iterateQuads[]: no term for ID \(tid)")
                        return nil
                    }
                }
                if terms.count == 4 {
                    return Quad(subject: terms[0], predicate: terms[1], object: terms[2], graph: terms[3])
                } else {
                    print("*** Bad quad")
                    return nil
                }
            } catch {
                return nil
            }
        }
        
        return AnyIterator(i.lazy.compactMap { $0 }.makeIterator())
    }
    
    public func quads(matching pattern: QuadPattern) throws -> AnyIterator<Quad> {
        var bestIndex: IndexOrder? = nil
        var prefix = [Int]()
        do {
            try self.env.read { (txn) -> Int in
                var boundPositions = Set<Int>()
                for (i, n) in pattern.enumerated() {
                    if case .bound(_) = n {
                        boundPositions.insert(i)
                    }
                }
                if let index = try self.bestIndex(matchingBoundPositions: boundPositions, txn: txn) {
                    bestIndex = index
                    //                print("Best index order is \(index.rawValue)")
                    let order = index.order()
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
                }
                return 0
            }
        } catch DiomedeError.nonExistentTermError {
            return AnyIterator([].makeIterator())
        }
            
        if let index = bestIndex {
            if prefix.isEmpty {
                print("matching all quads")
                let i = try self.quadsIterator()
                let s = AnySequence(i)
                let matching = s.lazy.filter { pattern.matches($0) }
                return AnyIterator(matching.makeIterator())
            } else {
                print("matching quads with prefix: \(prefix)")
                let cache = LRUCache<Int, Term>(capacity: 4_096)
                let i = try self.quadIdsIterator(usingIndex: index, withPrefix: prefix) { (txn, qid, tids) -> Quad? in
                    do {
                        let quad = try self.quad(from: tids, txn: txn, cache: cache)
                        return quad
                    } catch {
                        return nil
                    }
                }
                return AnyIterator(i.lazy.compactMap { $0 }.makeIterator())
            }
        } else {
            let i = try self.quadsIterator()
            let s = AnySequence(i)
            let matching = s.lazy.filter { pattern.matches($0) }
            return AnyIterator(matching.makeIterator())
        }
    }
}
