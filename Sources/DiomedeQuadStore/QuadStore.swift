//
//  QuadStore.swift
//  Diomede
//
//  Created by Gregory Todd Williams on 5/23/20.
//

import Foundation

import SPARQLSyntax
import Diomede

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
                guard let p = positions[c] else { fatalError() } // this is ensured by all the cases enumerated above only using the characters in "spog"
                order.append(p)
            }
            return order
        }
    }
    
    public enum NextIDKey: String {
        case term = "next_unassigned_term_id"
        case quad = "next_unassigned_quad_id"
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
    var quads_db: Environment.Database
    var t2i_db: Environment.Database
    var i2t_db: Environment.Database
    var indexes_db: Environment.Database
    var stats_db: Environment.Database
    var graphs_db: Environment.Database
    public var fullIndexes: [IndexOrder: (Environment.Database, [Int])]
    
    private init?(environment e: Environment) {
        self.env = e
        
        guard let quads = e.database(named: StaticDatabases.quads.rawValue),
            let indexes = e.database(named: StaticDatabases.fullIndexes.rawValue),
            let stats = e.database(named: StaticDatabases.stats.rawValue),
            let i2t = e.database(named: StaticDatabases.id_to_term.rawValue),
            let t2i = e.database(named: StaticDatabases.term_to_id.rawValue),
            let graphs = e.database(named: StaticDatabases.graphs.rawValue) else { return nil }
        self.quads_db = quads
        self.i2t_db = i2t
        self.t2i_db = t2i
        self.stats_db = stats
        self.graphs_db = graphs
        self.indexes_db = indexes
        
        self.fullIndexes = [:]
        do {
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
        } catch let e {
            print("*** Environment.init: \(e)")
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
                    print("*** Failed to construct new LMDB Environment")
                    return nil
                }
                
                try e.write { (txn) -> Int in
                    try e.createDatabase(txn: txn, named: StaticDatabases.quads.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.stats.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.fullIndexes.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.term_to_id.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.id_to_term.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.graphs.rawValue)
//                    try e.createDatabase(txn: txn, named: "spog")
//                    try e.createDatabase(txn: txn, named: "gpso")
                    let stats = e.database(txn: txn, named: StaticDatabases.stats.rawValue)!
//                    let indexes = e.database(txn: txn, named: StaticDatabases.fullIndexes.rawValue)!
//                    try indexes.insert(uniqueKeysWithValues: [
//                        ("spog", [0,1,2,3]),
//                        ("gpso", [3,1,0,2])
//                    ])
                    
                    let now = ISO8601DateFormatter().string(from: Date.init())
                    try stats.insert(txn: txn, uniqueKeysWithValues: [
                        ("Diomede-Version", "0.0.13"),
                        ("meta", ""),
                        ("Last-Modified", now)
                    ])
                    try stats.insert(txn: txn, uniqueKeysWithValues: [
                        (NextIDKey.term.rawValue, 1),
                        (NextIDKey.quad.rawValue, 1),
                    ])
                    return 0
                }
                
                self.init(environment: e)
            } catch let e {
                print("*** Environment.init: \(e)")
                return nil
            }
        } else {
            guard let e = Environment(path: path) else {
                print("*** Failed to open LMDB Environment")
                return nil
            }
            self.init(environment: e)
        }
    }

    func read(handler: (OpaquePointer) throws -> Int) throws {
        try self.env.read(handler: handler)
    }

    func write(handler: (OpaquePointer) throws -> Int) throws {
        let now = ISO8601DateFormatter().string(from: Date())
        try self.env.write { (txn) throws -> Int in
            let r = try handler(txn)
            try stats_db.insert(txn: txn, uniqueKeysWithValues: [
                ("Last-Modified", now)
            ])
            return r
        }
    }

    func bestIndex(matchingBoundPositions positions: Set<Int>) throws -> IndexOrder? {
        var order: IndexOrder? = nil
        try self.env.read { (txn) -> Int in
            order = try self.bestIndex(matchingBoundPositions: positions, txn: txn)
            return 0
        }
        return order
    }
    
    func bestIndex(matchingBoundPositions: Set<Int>, txn: OpaquePointer) throws -> IndexOrder? {
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

    public func quadIds(usingIndex indexOrder: IndexOrder, withPrefix prefix: [Int]) throws -> [[Int]] {
        guard let (index, order) = self.fullIndexes[indexOrder] else {
            throw DiomedeError.indexError
        }
        //        print("using index \(indexOrder.rawValue) with order \(order)")
        let empty = Array(repeating: 0, count: 4)
        let lower = Array((prefix + empty).prefix(4))
        var upper = lower
        if prefix.isEmpty {
            var quadIds = [[Int]]()
            try index.iterate { (qidsData, _) in
                var tids = Array<Int>(repeating: 0, count: 4)
                let strideBy = qidsData.count / 4
                for (pos, i) in zip(order, stride(from: 0, to: qidsData.count, by: strideBy)) {
                    let data = qidsData[i..<(i+strideBy)]
                    let tid = Int.fromData(data)
                    tids[pos] = tid
                }
                quadIds.append(tids)
            }
            return quadIds
        } else {
            upper[prefix.count - 1] += 1
            let lowerKey = lower.asData()
            let upperKey = upper.asData()
            //        print("from \(lowerKey._hexValue)")
            //        print("to   \(upperKey._hexValue)")
            
            var quadIds = [[Int]]()
            try index.iterate(between: lowerKey, and: upperKey) { (qidsData, _) in
                var tids = Array<Int>(repeating: 0, count: 4)
                let strideBy = qidsData.count / 4
                for (pos, i) in zip(order, stride(from: 0, to: qidsData.count, by: strideBy)) {
                    let data = qidsData[i..<(i+strideBy)]
                    let tid = Int.fromData(data)
                    tids[pos] = tid
                }
                quadIds.append(tids)
            }
            return quadIds
        }
    }
    
    public func iterateQuadIds(txn: OpaquePointer, usingIndex indexOrder: IndexOrder? = nil, handler: (Int, [Int]) throws -> ()) throws {
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
            try self.quads_db.iterate(txn: txn) { (qidData, qidsData) in
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

    func quad(from tids: [Int], txn: OpaquePointer, cache: LRUCache<Int, Term>? = nil) throws -> Quad? {
        var terms = [Term]()
        for tid in tids {
            if let cache = cache, let term = cache[tid] {
                terms.append(term)
            } else if let tdata = try i2t_db.get(txn: txn, key: tid) {
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

    func term(from tid: Int, txn: OpaquePointer, cache: LRUCache<Int, Term>? = nil) throws -> Term? {
        if let cache = cache, let term = cache[tid] {
            return term
        } else if let tdata = try i2t_db.get(txn: txn, key: tid) {
            let term = try Term.fromData(tdata)
            if let cache = cache {
                cache[tid] = term
            }
            return term
        } else {
            return nil
        }
    }

    func iterateQuads(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: (Quad) throws -> ()) throws {
        let cache = LRUCache<Int, Term>(capacity: 4_096)
        try iterateQuadIds(txn: txn, usingIndex: indexOrder) { (qid, tids) throws in
            var terms = [Term]()
            for tid in tids {
                if let term = cache[tid] {
                    terms.append(term)
                } else if let tdata = try i2t_db.get(txn: txn, key: tid) {
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
    
    func id(for term: Term, txn: OpaquePointer) throws -> Int? {
        let term_key = try term.sha256()
        if let eid = try self.t2i_db.get(txn: txn, key: term_key) {
            return Int.fromData(eid)
        } else {
            return nil
        }
    }
    
    public func id(for term: Term) throws -> Int? {
        let term_key = try term.sha256()
        var id : Int? = nil
        try self.env.read { (txn) -> Int in
            if let eid = try self.t2i_db.get(txn: txn, key: term_key) {
                id = Int.fromData(eid)
            }
            return 0
        }
        return id
    }
}

extension DiomedeQuadStore {
    // This is the public API
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
            try self.indexes_db.insert(txn: txn, uniqueKeysWithValues: [
                (indexName, order),
            ])
            return 0
        }
    }
    
    public mutating func dropFullIndex(order indexOrder: IndexOrder) throws {
        let indexName = indexOrder.rawValue
        try self.write { (txn) -> Int in
            self.fullIndexes.removeValue(forKey: indexOrder)
            try self.indexes_db.delete(txn: txn, key: indexName)
            try self.env.dropDatabase(txn: txn, named: indexName)
            return 0
        }
    }
    
    public func namedGraphs() throws -> AnyIterator<Term> {
        var termIds = [Int]()
        try self.graphs_db.iterate(handler: { (data, _) in
            let tid = Int.fromData(data)
            termIds.append(tid)
        })
        
        return self.termIterator(fromIds: termIds)
    }

    func quadsIterator(fromIds ids: [[Int]]) -> AnyIterator<Quad> {
        let cache = LRUCache<Int, Term>(capacity: 4_096)
        let chunkSize = 1024

        var quadIds = ids
        let quadChunks = AnyIterator<[Quad]> { () -> [Quad]? in
            guard !quadIds.isEmpty else { return nil }
            do {
                var quads = [Quad]()
                let chunk = quadIds.prefix(chunkSize)
                quadIds.removeFirst(min(chunkSize, quadIds.count))
                try self.env.read { (txn) -> Int in
                    QUAD: for tids in chunk {
                        var terms = [Term]()
                        for tid in tids {
                            if let term = cache[tid] {
                                terms.append(term)
                            } else if let tdata = try self.i2t_db.get(txn: txn, key: tid) {
                                let term = try Term.fromData(tdata)
                                terms.append(term)
                                cache[tid] = term
                            } else {
                                print("iterateQuads[]: no term for ID \(tid)")
                                continue QUAD
                            }
                        }
                        if terms.count == 4 {
                            let q = Quad(subject: terms[0], predicate: terms[1], object: terms[2], graph: terms[3])
                            quads.append(q)
                        } else {
                            print("*** Bad quad")
                        }
                    }
                    return 0
                }
                return quads
            } catch let e {
                print("*** Quad iterator caught internal exception: \(e)")
                return nil
            }
        }
        
        let quads = quadChunks.lazy.flatMap { $0 }
        return AnyIterator(quads.makeIterator())
    }
    
    func termIterator(fromIds ids: [Int]) -> AnyIterator<Term> {
        let cache = LRUCache<Int, Term>(capacity: 4_096)
        let chunkSize = 1024

        var termIds = ids
        let termChunks = AnyIterator<[Term]> { () -> [Term]? in
            guard !termIds.isEmpty else { return nil }
            do {
                var terms = [Term]()
                let chunk = termIds.prefix(chunkSize)
                termIds.removeFirst(min(chunkSize, termIds.count))
                try self.env.read { (txn) -> Int in
                    for tid in chunk {
                        if let term = cache[tid] {
                            terms.append(term)
                        } else if let tdata = try self.i2t_db.get(txn: txn, key: tid) {
                            let term = try Term.fromData(tdata)
                            terms.append(term)
                            cache[tid] = term
                        } else {
                            print("termIterator[]: no term for ID \(tid)")
                            continue
                        }
                    }
                    return 0
                }
                return terms
            } catch let e {
                print("*** Quad iterator caught internal exception: \(e)")
                return nil
            }
        }
        
        let terms = termChunks.lazy.flatMap { $0 }
        return AnyIterator(terms.makeIterator())
    }
    
    public func quadsIterator() throws -> AnyIterator<Quad> {
        var quadIds = [[Int]]()
        try self.quads_db.iterate(handler: { (_, value) in
            let tids = [Int].fromData(value)
            quadIds.append(tids)
        })
        
        return self.quadsIterator(fromIds: quadIds)
    }
    
    public func terms(in graph: Term, positions: Set<Int>) throws -> AnyIterator<Term> {
        var pattern = QuadPattern.all
        pattern.graph = .bound(graph)
        
        var bestIndex: IndexOrder? = nil
        var prefix = [Int]()
        do {
            try self.env.read { (txn) -> Int in
                let boundPositions : Set<Int> = [3]
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
            if !prefix.isEmpty {
                let quadIds = try self.quadIds(usingIndex: index, withPrefix: prefix)
                var termIds = Set<Int>()
                for tids in quadIds {
                    for pos in positions {
                        let tid = tids[pos]
                        termIds.insert(tid)
                    }
                }
                
                return self.termIterator(fromIds: Array(termIds))
            }
        }
        
        let term_key = try graph.sha256()
        var gid: Int = 0
        try self.env.read { (txn) -> Int in
            guard let data = try self.t2i_db.get(txn: txn, key: term_key) else {
                throw DiomedeError.nonExistentTermError
            }
            gid = Int.fromData(data)
            return 0
        }

//        print("finding graph terms for gid \(gid)")

        var quadIds = [[Int]]()
        try self.quads_db.iterate(handler: { (_, value) in
            let tids = [Int].fromData(value)
            guard tids.count == 4 else { return }
            guard tids[3] == gid else { return }
            quadIds.append(tids)
        })
        var termIds = Set<Int>()
        for tids in quadIds {
            for pos in positions {
                let tid = tids[pos]
                termIds.insert(tid)
            }
        }
        
        return self.termIterator(fromIds: Array(termIds))
    }

    func quadIds(matching pattern: QuadPattern) throws -> [[Int]] {
//        print("matching: \(pattern)")
        var bestIndex: IndexOrder? = nil
        var prefix = [Int]()
        var restrictions = [Int: Int]()
        do {
            try self.env.read { (txn) -> Int in
                var boundPositions = Set<Int>()
                for (i, n) in pattern.enumerated() {
                    if case .bound(let term) = n {
                        boundPositions.insert(i)
                        guard let tid = try self.id(for: term, txn: txn) else {
                            throw DiomedeError.nonExistentTermError
                        }
                        restrictions[i] = tid
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
            return []
        }
        
        if let index = bestIndex {
//            print("using index \(index.rawValue)")
            let quadIds = try self.quadIds(usingIndex: index, withPrefix: prefix)
            return quadIds.filter { (tids) -> Bool in
                for (i, value) in restrictions {
                    if tids[i] != value {
                        return false
                    }
                }
                return true
            }
        } else {
            var quadIds = [[Int]]()
            try self.quads_db.iterate { (_, spog) in
                let tids = [Int].fromData(spog)
                for (i, value) in restrictions {
                    if tids[i] != value {
                        return
                    }
                }
                quadIds.append(tids)
            }
            return quadIds
        }
    }

    public func quads(matching pattern: QuadPattern) throws -> AnyIterator<Quad> {
        let quadIds = try self.quadIds(matching: pattern)
        let i = self.quadsIterator(fromIds: quadIds)
        return AnyIterator(i.makeIterator())
    }

    public func quads(using indexOrder: IndexOrder) throws -> AnyIterator<Quad> {
        let quadIds = try self.quadIds(usingIndex: indexOrder, withPrefix: [])
        let i = self.quadsIterator(fromIds: quadIds)
        return AnyIterator(i.makeIterator())
    }

    public func touch() throws {
        try self.write { (_) -> Int in
            return 0
        }
    }
}

//extension DiomedeQuadStore {
//    // This is public code that depends on the Kineo package
//    public func loadRDF(from url: URL) throws {
//        let parser = RDFParserCombined()
//        let graph = Term(iri: url.absoluteString)
//
//        try env_db.write { (txn) -> Int in
//            let cache = LRUCache<Term, Int>(capacity: 4_096)
//            var next_term_id = try stats_db.get(txn: txn, key: "next_unassigned_term_id").map { Int.fromData($0) } ?? 1
//            var next_quad_id = try stats_db.get(txn: txn, key: "next_unassigned_quad_id").map { Int.fromData($0) } ?? 1
//
//            var graphIds = Set<Int>()
//            var quadIds = [[Int]]()
//            var terms = Set<Term>()
//            try parser.parse(file: url.path, base: graph.value) { (s, p, o) in
//                let q = Quad(subject: s, predicate: p, object: o, graph: graph)
//                do {
//                    var termIds = [Int]()
//                    for (i, t) in q.enumerated() {
//                        terms.insert(t)
//                        let d = try t.asData()
//
//                        let term_key = Data(SHA256.hash(data: d))
//                        var tid: Int
//                        if let cached_id = cache[t] {
//                            tid = cached_id
//                        } else if let eid = try self.t2i_db.get(txn: txn, key: term_key) {
//                            tid = Int.fromData(eid)
//                        } else {
//                            tid = next_term_id
//                            let i2t_pair = (tid, d)
//                            let t2i_pair = (Data(SHA256.hash(data: d)), tid)
//                            try self.i2t_db.insert(txn: txn, uniqueKeysWithValues: [i2t_pair])
//                            try self.t2i_db.insert(txn: txn, uniqueKeysWithValues: [t2i_pair])
//                            next_term_id += 1
//                        }
//
//                        cache[t] = tid
//                        termIds.append(tid)
//                        if (i == 3) {
//                            graphIds.insert(tid)
//                        }
//                    }
//                    assert(termIds.count == 4)
//                    quadIds.append(termIds)
//                } catch {}
//            }
//
//            let graphIdPairs = graphIds.map { ($0, Data()) }
//            try self.graphs_db.insert(txn: txn, uniqueKeysWithValues: graphIdPairs)
//
//            try stats_db.insert(txn: txn, uniqueKeysWithValues: [
//                ("next_unassigned_term_id", next_term_id),
//            ])
//
//            let quadKeys = quadIds.map { (q) in q.map { $0.asData() }.reduce(Data()) { $0 + $1 } }
//            let emptyValue = Data()
//
//            var quadPairs = [(Int, Data)]()
//            for qkey in quadKeys {
//                let qid = next_quad_id
//                next_quad_id += 1
//                quadPairs.append((qid, qkey))
//            }
//
//            try stats_db.insert(txn: txn, uniqueKeysWithValues: [
//                ("next_unassigned_quad_id", next_quad_id),
//            ])
//
//            let now = ISO8601DateFormatter().string(from: Date.init())
//            try stats_db.insert(txn: txn, uniqueKeysWithValues: [
//                ("Last-Modified", now)
//            ])
//
//            try self.quads_db.insert(txn: txn, uniqueKeysWithValues: quadPairs)
//
//            for (_, pair) in self.fullIndexes {
//                let (index, order) = pair
//                let indexOrderedKeys = quadIds.map { (q) in order.map({ q[$0].asData() }).reduce(Data()) { $0 + $1 } }
//                let indexOrderedPairs = indexOrderedKeys.map { ($0, emptyValue) }
//                try index.insert(txn: txn, uniqueKeysWithValues: indexOrderedPairs)
//            }
//
//            return 0
//        }
//    }
//}

extension DiomedeQuadStore {
    // private functions that are used in the developer CLI tool diomede-cli
    public var _private_quads: Environment.Database {
        return self.quads_db
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

    public func _private_quadIds(usingIndex indexOrder: IndexOrder, withPrefix prefix: [Int]) throws -> [[Int]] {
        return try self.quadIds(usingIndex: indexOrder, withPrefix: prefix)
    }
    
}

extension DiomedeQuadStore {
    // These allow DiomedeQuadStore to conform to QuadStoreProtocol,
    
    public typealias Version = UInt64
    public func effectiveVersion() throws -> Version? {
        return try effectiveVersion(matching: QuadPattern.all)
    }
    
    public func effectiveVersion(matching pattern: QuadPattern) throws -> Version? {
        let f = ISO8601DateFormatter()
        if let d = try self.stats_db.get(key: "Last-Modified") {
            let string = try String.fromData(d)
            if let date = f.date(from: string) {
                let seconds = UInt64(date.timeIntervalSince1970)
//                print("effective version: \(seconds)")
                return seconds
            }
        }
        return nil
    }
    
    public var count: Int {
        var count = 0
        try? self.read { (txn) -> Int in
            count = self.quads_db.count(txn: txn)
            return 0
        }
        return count
    }
    
    public func graphs() -> AnyIterator<Term> {
        do {
            return try self.namedGraphs()
        } catch let e {
            print("*** Failed to access named graphs: \(e)")
            return AnyIterator([].makeIterator())
        }
    }
    
    public func graphTerms(in graph: Term) -> AnyIterator<Term> {
        do {
            return try self.terms(in: graph, positions: [0, 2])
        } catch {
            return AnyIterator([].makeIterator())
        }
    }
    
    public func makeIterator() -> AnyIterator<Quad> {
        do {
            return try self.quadsIterator()
        } catch {
            return AnyIterator([].makeIterator())
        }
    }
    
    public func bindings(matching pattern: QuadPattern) throws -> AnyIterator<[String:Term]> {
        var bindings : [String: KeyPath<Quad, Term>] = [:]
        for (node, path) in zip(pattern, QuadPattern.groundKeyPaths) {
            if case .variable(let name, binding: _) = node {
                bindings[name] = path
            }
        }
        let quads = try self.quads(matching: pattern)
        let results = quads.lazy.map { (q) -> [String:Term] in
            var b = [String: Term]()
            for (name, path) in bindings {
                b[name] = q[keyPath: path]
            }
            return b
        }
        return AnyIterator(results.makeIterator())
    }
    
    public func results(matching pattern: QuadPattern) throws -> AnyIterator<SPARQLResultSolution<Term>> {
        var bindings : [String: KeyPath<Quad, Term>] = [:]
        for (node, path) in zip(pattern, QuadPattern.groundKeyPaths) {
            if case .variable(let name, binding: _) = node {
                bindings[name] = path
            }
        }
        let quads = try self.quads(matching: pattern)
        let results = quads.lazy.map { (q) -> SPARQLResultSolution<Term> in
            var b = [String: Term]()
            for (name, path) in bindings {
                b[name] = q[keyPath: path]
            }
            return SPARQLResultSolution(bindings: b)
        }
        return AnyIterator(results.makeIterator())
    }

    public func countQuads(matching pattern: QuadPattern) throws -> Int {
        
        
        
        
        
        
        var bestIndex: IndexOrder? = nil
        var prefix = [Int]()
        var restrictions = [Int: Int]()
        var boundPositions = Set<Int>()
        do {
            try self.env.read { (txn) -> Int in
                for (i, n) in pattern.enumerated() {
                    if case .bound(let term) = n {
                        boundPositions.insert(i)
                        
                        restrictions[i] = try self.id(for: term, txn: txn)
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
            return 0
        }
        
        if let indexOrder = bestIndex, prefix.count == boundPositions.count {
            // index completely covers the bound terms; matching on this index will give a precise count
            //            print("using index \(index.rawValue)")
//            print("using optimized count on index \(indexOrder.rawValue): \(pattern)")
            guard let (index, _) = self.fullIndexes[indexOrder] else {
                throw DiomedeError.indexError
            }
            let empty = Array(repeating: 0, count: 4)
            let lower = Array((prefix + empty).prefix(4))
            var upper = lower
            upper[prefix.count - 1] += 1
            let lowerKey = lower.asData()
            let upperKey = upper.asData()
            let count = try index.count(between: lowerKey, and: upperKey, inclusive: false)
//            print("-> \(count)")
            return count
        } else {
//            print("using non-optimized count for: \(pattern)")
            var count = 0
            for _ in try self.quadIds(matching: pattern) {
                count += 1
            }
//            print("-> \(count)")
            return count
        }
    }
    
    
}

private func humanReadable(count: Int) -> String {
    var names = ["", "k", "m", "b"]
    var unit = names.remove(at: 0)
    var size = count
    while !names.isEmpty && size >= 1024 {
        unit = names.remove(at: 0)
        size /= 1024
    }
    return "\(size)\(unit)"
}


extension DiomedeQuadStore {
    // These allow DiomedeQuadStore to conform to MutableQuadStoreProtocol
    public func load<S>(version: Version, quads: S) throws where S : Sequence, S.Element == Quad {
        let start = CFAbsoluteTimeGetCurrent()
        try self.write { (txn) -> Int in
            var next_term_id = try stats_db.get(txn: txn, key: NextIDKey.term.rawValue).map { Int.fromData($0) } ?? 1
            var next_quad_id = try stats_db.get(txn: txn, key: NextIDKey.quad.rawValue).map { Int.fromData($0) } ?? 1

            var graphIds = Set<Int>()
            var quadIds = [[Int]]()
            var terms = Set<Term>()
            for (i, q) in quads.enumerated() {
//                if i % 10000 == 0 {
//                    let elapsed = CFAbsoluteTimeGetCurrent() - start
//                    let tps = Double(i) / elapsed
//                    print("\(i) (\(tps) T/s)")
//                    //                    print("\r\(humanReadable(count: i))) (\(tps) t/s)", terminator: "")
//                }
                do {
                    var termIds = [Int]()
                    for (i, t) in q.enumerated() {
                        terms.insert(t)
                        let d = try t.asData()
                        let term_key = try t.sha256()
                        var tid: Int
                        if let eid = try self.t2i_db.get(txn: txn, key: term_key) {
                            tid = Int.fromData(eid)
                        } else {
                            tid = next_term_id
                            let i2t_pair = (tid, d)
                            let t2i_pair = (term_key, tid)
                            try self.i2t_db.insert(txn: txn, uniqueKeysWithValues: [i2t_pair])
                            try self.t2i_db.insert(txn: txn, uniqueKeysWithValues: [t2i_pair])
                            next_term_id += 1
                        }

                        termIds.append(tid)
                        if (i == 3) {
                            graphIds.insert(tid)
                        }
                    }
                    assert(termIds.count == 4)
                    quadIds.append(termIds)
                } catch DiomedeError.mapFullError {
                    print("Failed to load data.")
                    throw DiomedeError.mapFullError
                } catch DiomedeError.insertError {
                    print("Failed to load data.")
                    throw DiomedeError.insertError
                } catch {}
            }
            
            let graphIdPairs = graphIds.map { ($0, Data()) }
            try self.graphs_db.insert(txn: txn, uniqueKeysWithValues: graphIdPairs)
            
            try stats_db.insert(txn: txn, uniqueKeysWithValues: [
                (NextIDKey.term.rawValue, next_term_id),
            ])

            let quadKeys = quadIds.map { (q) in q.map { $0.asData() }.reduce(Data()) { $0 + $1 } }
            let emptyValue = Data()
            
            var quadPairs = [(Int, Data)]()
            for qkey in quadKeys {
                let qid = next_quad_id
                next_quad_id += 1
                quadPairs.append((qid, qkey))
            }

            try stats_db.insert(txn: txn, uniqueKeysWithValues: [
                (NextIDKey.quad.rawValue, next_quad_id),
            ])

            try self.quads_db.insert(txn: txn, uniqueKeysWithValues: quadPairs)
            
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

