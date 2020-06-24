//
//  QuadStore.swift
//  Diomede
//
//  Created by Gregory Todd Williams on 5/23/20.
//

import Foundation

import SPARQLSyntax
import Diomede

public class DiomedeQuadStore {
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
    
    public enum NextIDKey: String, CustomStringConvertible {
        case term = "next_unassigned_term_id"
        case quad = "next_unassigned_quad_id"
        
        public var description: String {
            switch self {
            case .term:
                return "Next unassigned term ID"
            case .quad:
                return "Next unassigned quad ID"
            }
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
    
    public enum StatusUpdateType {
        case loadProgress(count: Int, rate: Double)
    }
    
    public typealias StatusUpdateHandler = (StatusUpdateType) -> ()
    
    var env: Environment
    var quads_db: Environment.Database
    var t2i_db: Environment.Database
    var i2t_db: Environment.Database
    var indexes_db: Environment.Database
    var stats_db: Environment.Database
    var graphs_db: Environment.Database
    public var progressHandler: StatusUpdateHandler?
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
        self.progressHandler = nil
        
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

    public convenience init?(path: String, create: Bool = false) {
        self.init(path: path, create: create, configuration: nil)
    }
    
    public convenience init?(path: String, create: Bool = false, configuration: DiomedeConfiguration?) {
        if create {
            do {
                let f = FileManager.default
                if !f.fileExists(atPath: path) {
                    try f.createDirectory(atPath: path, withIntermediateDirectories: true, attributes: nil)
                }
                guard let e = Environment(path: path, configuration: configuration) else {
                    print("*** Failed to construct new LMDB Environment")
                    return nil
                }

                let now = ISO8601DateFormatter().string(from: Date.init())
                let defaultIndex = IndexOrder.gpso

                try e.write { (txn) -> Int in
                    try e.createDatabase(txn: txn, named: StaticDatabases.quads.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.term_to_id.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.id_to_term.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.graphs.rawValue)
                    try e.createDatabase(txn: txn, named: defaultIndex.rawValue)
                    try e.createDatabase(txn: txn, named: StaticDatabases.fullIndexes.rawValue, withSortedKeysAndValues: [
                        (defaultIndex.rawValue, [3,1,0,2])
                    ])
                    
                    try e.createDatabase(txn: txn, named: StaticDatabases.stats.rawValue, withSortedKeysAndValues: [
                        ("Diomede-Version", "0.0.43".asData()),
                        ("Last-Modified", now.asData()),
                        ("meta", "".asData()),
                        (NextIDKey.quad.rawValue, 1.asData()),
                        (NextIDKey.term.rawValue, 1.asData()),
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

    public func bestIndex(matchingBoundPositions positions: Set<Int>) throws -> IndexOrder? {
        let bestIndexes = try self.indexes(matchingBoundPositions: positions)
        return bestIndexes.first
    }
    
    public func indexes(matchingBoundPositions positions: Set<Int>) throws -> [IndexOrder] {
        var orderes = [IndexOrder]()
        try self.env.read { (txn) -> Int in
            orderes = try self.indexes(matchingBoundPositions: positions, txn: txn)
            return 0
        }
        return orderes
    }
    
    func indexes(matchingBoundPositions: Set<Int>, txn: OpaquePointer) throws -> [IndexOrder] {
        let bound = matchingBoundPositions
        var scores = [(Int, IndexOrder)]()
        var bestScore = 0
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
            bestScore = max(bestScore, score)
            scores.append((score, k))
        }
        
        let bestOrders = scores.filter { $0.0 == bestScore }.map { $0.1 }
        return bestOrders
    }
    
    func bestIndex(matchingBoundPositions positions: Set<Int>, txn: OpaquePointer) throws -> IndexOrder? {
        let bestIndexes = try self.indexes(matchingBoundPositions: positions, txn: txn)
        return bestIndexes.first
    }

    public func quadIds(usingIndex indexOrder: IndexOrder, withPrefix prefix: [UInt64], restrictedBy restrictions: [Int: UInt64]) throws -> [[UInt64]] {
        let quadIds = try self.quadIds(usingIndex: indexOrder, withPrefix: prefix)
        let filtered = quadIds.filter { (tids) -> Bool in
            for (i, value) in restrictions {
                if tids[i] != value {
                    return false
                }
            }
            return true
        }
        return filtered

    }
    
    public func quadIds(usingIndex indexOrder: IndexOrder, withPrefix prefix: [UInt64]) throws -> [[UInt64]] {
        guard let (index, order) = self.fullIndexes[indexOrder] else {
            throw DiomedeError.indexError
        }
        //        print("using index \(indexOrder.rawValue) with order \(order)")
        let empty = Array(repeating: UInt64(0), count: 4)
        let lower = Array((prefix + empty).prefix(4))
        var upper = lower
        if prefix.isEmpty {
            var quadIds = [[UInt64]]()
            try index.iterate { (qidsData, _) in
                var tids = Array<Int>(repeating: 0, count: 4)
                let strideBy = qidsData.count / 4
                for (pos, i) in zip(order, stride(from: 0, to: qidsData.count, by: strideBy)) {
                    let data = qidsData[i..<(i+strideBy)]
                    let tid = Int.fromData(data)
                    tids[pos] = tid
                }
                let ids = tids.map { UInt64($0 )}
                quadIds.append(ids)
            }
            return quadIds
        } else {
            upper[prefix.count - 1] += 1
            let lowerKey = lower.map { Int($0) }.asData()
            let upperKey = upper.map { Int($0) }.asData()
            //        print("from \(lowerKey._hexValue)")
            //        print("to   \(upperKey._hexValue)")
            
            var quadIds = [[UInt64]]()
            try index.iterate(between: lowerKey, and: upperKey) { (qidsData, _) in
                var tids = Array<Int>(repeating: 0, count: 4)
                let strideBy = qidsData.count / 4
                for (pos, i) in zip(order, stride(from: 0, to: qidsData.count, by: strideBy)) {
                    let data = qidsData[i..<(i+strideBy)]
                    let tid = Int.fromData(data)
                    tids[pos] = tid
                }
                let ids = tids.map { UInt64($0 )}
                quadIds.append(ids)
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
            
            try index.unescapingIterate(txn: txn, handler: iterationHandler)
        } else {
            // no index given, just use the quads table
            try self.quads_db.unescapingIterate(txn: txn) { (qidData, qidsData) in
                let qid = Int.fromData(qidData)
                let tids = try QuadID.fromData(qidsData)
//                var tids = [Int]()
//                let strideBy = qidsData.count / 4
//                for i in stride(from: 0, to: qidsData.count, by: strideBy) {
//                    let data = qidsData[i..<(i+strideBy)]
//                    let tid = Int.fromData(data)
//                    tids.append(tid)
//                }
                try handler(qid, tids.values.map { Int($0) })
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

    public func term(from id: UInt64) throws -> Term? {
        let tid = Int(id)
        if let tdata = try i2t_db.get(key: tid) {
            return try Term.fromData(tdata)
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
    
    func id(for term: Term, txn: OpaquePointer) throws -> UInt64? {
        let term_key = try term.sha256()
        if let eid = try self.t2i_db.get(txn: txn, key: term_key) {
            let tid = Int.fromData(eid)
            return UInt64(tid)
        } else {
            return nil
        }
    }
    
    public func id(for term: Term) throws -> UInt64? {
        let term_key = try term.sha256()
        var id : Int? = nil
        try self.env.read { (txn) -> Int in
            if let eid = try self.t2i_db.get(txn: txn, key: term_key) {
                id = Int.fromData(eid)
            }
            return 0
        }
        return id.map { UInt64($0) }
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

        var quadIds = [(Data, Int)]()
        print("loading and re-ordering quad terms to match index order")
        try self.read { (txn) throws -> Int in
            try self.iterateQuadIds(txn: txn) { (qid, tids) in
                let indexOrderedValue = order.map({ tids[$0].asData() }).reduce(Data()) { $0 + $1 }
                quadIds.append((indexOrderedValue, qid))
            }
            return 0
        }
        
        print("sorting quads")
        quadIds.sort { (a, b) -> Bool in
            return a.0.lexicographicallyPrecedes(b.0)
        }
        
        try self.write { (txn) -> Int in
            print("bulk loading new index")
            try self.env.createDatabase(txn: txn, named: indexName, withSortedKeysAndValues: quadIds)
//            let index = self.env.database(txn: txn, named: indexName)!
//            try index.insert(txn: txn, uniqueKeysWithValues: indexOrderedPairs)
            try self.indexes_db.insert(txn: txn, uniqueKeysWithValues: [
                (indexName, order),
            ])
            return 0
        }
    }
    
    public func dropFullIndex(order indexOrder: IndexOrder) throws {
        let indexName = indexOrder.rawValue
        try self.write { (txn) -> Int in
            self.fullIndexes.removeValue(forKey: indexOrder)
            try self.indexes_db.delete(txn: txn, key: indexName)
            try self.env.dropDatabase(txn: txn, named: indexName)
            return 0
        }
    }
    
    public func namedGraphs() throws -> AnyIterator<Term> {
        var termIds = [UInt64]()
        try self.graphs_db.iterate(handler: { (data, _) in
            let tid = Int.fromData(data)
            let id = UInt64(tid)
            termIds.append(id)
        })
        
        return self.termIterator(fromIds: termIds)
    }

    public func quadsIterator(fromIds uids: [[UInt64]]) -> AnyIterator<Quad> {
        let ids = uids.map { $0.map { Int($0) } }
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
    
    func termIterator(fromIds ids: [UInt64]) -> AnyIterator<Term> {
        let cache = LRUCache<UInt64, Term>(capacity: 4_096)
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
        var quadIds = [[UInt64]]()
        try self.quads_db.iterate(handler: { (_, value) in
            let tids = [Int].fromData(value)
            let ids = tids.map { UInt64($0) }
            quadIds.append(ids)
        })
        
        return self.quadsIterator(fromIds: quadIds)
    }
    
    public func prefix(for pattern: QuadPattern, in index: IndexOrder) throws -> (prefix: [UInt64], restriction: [Int: UInt64]) {
        var prefix = [UInt64]()
        var restrictions = [Int: UInt64]()
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
                let id = UInt64(tid)
                prefix.append(id)
            }
            return 0
        }
        return (prefix, restrictions)
    }

    public func termIDs(in graph: UInt64, positions: Set<Int>) throws -> AnyIterator<UInt64> {
        let pattern : [UInt64] = [0,0,0,graph]
        
        var bestIndex: IndexOrder? = nil
        var prefix = [UInt64]()
        do {
            try self.env.read { (txn) -> Int in
                let boundPositions : Set<Int> = [3]
                if let index = try self.bestIndex(matchingBoundPositions: boundPositions, txn: txn) {
                    bestIndex = index
                    //                print("Best index order is \(index.rawValue)")
                    let order = index.order()
                    
                    for i in order {
                        let tid = pattern[i]
                        if tid == 0 {
                            break
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
                var termIds = Set<UInt64>()
                for tids in quadIds {
                    for pos in positions {
                        let tid = tids[pos]
                        let id = UInt64(tid)
                        termIds.insert(id)
                    }
                }
                
                return AnyIterator(termIds.makeIterator())
            }
        }
        
        let gid = Int(graph)
        
        //        print("finding graph terms for gid \(gid)")
        
        var quadIds = [[UInt64]]()
        try self.quads_db.iterate(handler: { (_, value) in
            let tids = [Int].fromData(value)
            guard tids.count == 4 else { return }
            guard tids[3] == gid else { return }
            let ids = tids.map { UInt64($0) }
            quadIds.append(ids)
        })
        var termIds = Set<UInt64>()
        for tids in quadIds {
            for pos in positions {
                let tid = tids[pos]
                termIds.insert(tid)
            }
        }
        
        return AnyIterator(termIds.makeIterator())
    }
    
    public func terms(in graph: Term, positions: Set<Int>) throws -> AnyIterator<Term> {
        var pattern = QuadPattern.all
        pattern.graph = .bound(graph)
        
        var bestIndex: IndexOrder? = nil
        var prefix = [UInt64]()
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
                        let id = UInt64(tid)
                        prefix.append(id)
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
                var termIds = Set<UInt64>()
                for tids in quadIds {
                    for pos in positions {
                        let tid = tids[pos]
                        let id = UInt64(tid)
                        termIds.insert(id)
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

        var quadIds = [[UInt64]]()
        try self.quads_db.iterate(handler: { (_, value) in
            let tids = [Int].fromData(value)
            guard tids.count == 4 else { return }
            guard tids[3] == gid else { return }
            let ids = tids.map { UInt64($0) }
            quadIds.append(ids)
        })
        var termIds = Set<UInt64>()
        for tids in quadIds {
            for pos in positions {
                let tid = tids[pos]
                termIds.insert(tid)
            }
        }
        
        return self.termIterator(fromIds: Array(termIds))
    }

    public func quadExists(withIds match_tids: [UInt64]) throws -> Bool {
        var bestIndex: IndexOrder? = nil
        var prefix = [UInt64]()
        var restrictions = [Int: UInt64]()
        do {
            try self.env.read { (txn) -> Int in
                for i in 0..<4 {
                    restrictions[i] = match_tids[i]
                }
                let boundPositions = Set<Int>(0..<4)
                if let index = try self.bestIndex(matchingBoundPositions: boundPositions, txn: txn) {
                    bestIndex = index
                    //                print("Best index order is \(index.rawValue)")
                    let order = index.order()
                    
                    for i in order {
                        let tid = match_tids[i]
                        prefix.append(tid)
                    }
                }
                return 0
            }
        } catch DiomedeError.nonExistentTermError {
            return false
        }

        var seen = false
        do {
            if let indexOrder = bestIndex {
                guard let (index, order) = self.fullIndexes[indexOrder] else {
                    throw DiomedeError.indexError
                }
                let empty = Array(repeating: UInt64(0), count: 4)
                let lower = Array((prefix + empty).prefix(4))
                var upper = lower
                upper[prefix.count - 1] += 1
                let lowerKey = lower.map { Int($0) }.asData()
                let upperKey = upper.map { Int($0) }.asData()
                
                try index.iterate(between: lowerKey, and: upperKey) { (qidsData, _) in
                    let qids = try QuadID.fromData(qidsData)
                    var tids = Array<UInt64>(repeating: 0, count: 4)
                    for (pos, tid) in zip(order, qids.values) {
                        tids[pos] = tid
                    }
                    if tids == match_tids {
                        seen = true
                        throw DiomedeError.getError
                    }
                }
            } else {
                try self.quads_db.unescapingIterate { (_, spog) in
                    let qid = try QuadID.fromData(spog)
                    let tids = qid.values
                    if tids == match_tids {
                        seen = true
                        throw DiomedeError.getError
                    }
                }
            }
        } catch {}
        return seen
    }
    
    public func quadIds(matchingIDs pattern: [UInt64]) throws -> AnyIterator<[UInt64]> {
        // here the elements of pattern are either =0 which indicates a variable, or >0 indicating a term ID
        // this means that there is no provision for patterns like ?a ?a ?b ?c (with repeated variable usage)
        var bestIndex: IndexOrder? = nil
        var prefix = [UInt64]()
        var restrictions = [Int: UInt64]()
        do {
            try self.env.read { (txn) -> Int in
                var boundPositions = Set<Int>()
                for (i, n) in pattern.enumerated() {
                    switch n {
                    case let tid where tid > 0:
                        boundPositions.insert(i)
                        restrictions[i] = tid
                    default:
                        break
                    }
                }
                if let index = try self.bestIndex(matchingBoundPositions: boundPositions, txn: txn) {
                    bestIndex = index
                    //                print("Best index order is \(index.rawValue)")
                    let order = index.order()
                    let nodes = pattern
                    
                    for i in order {
                        let tid = nodes[i]
                        guard tid > 0 else {
                            break
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
            //            print("using index \(index.rawValue)")
            let quadIds = try self.quadIds(usingIndex: index, withPrefix: prefix)
            let filtered = quadIds.filter { (tids) -> Bool in
                for (i, value) in restrictions {
                    if tids[i] != value {
                        return false
                    }
                }
                return true
            }
            return AnyIterator(filtered.map { $0.map { UInt64($0) } }.makeIterator())
        } else {
            var quadIds = [QuadID]()
            try self.quads_db.unescapingIterate { (_, spog) in
                let tids = try QuadID.fromData(spog)
                for (i, value) in restrictions {
                    if tids[i] != value {
                        return
                    }
                }
                quadIds.append(tids)
            }
            return AnyIterator(quadIds.map { $0.values }.makeIterator())
        }
    }
    
    public func quadIds(matching pattern: QuadPattern) throws -> [[UInt64]] {
//        print("matching: \(pattern)")
        var bestIndex: IndexOrder? = nil
        var prefix = [UInt64]()
        var restrictions = [Int: UInt64]()
        var variableUsage = [String: Set<Int>]()
        do {
            try self.env.read { (txn) -> Int in
                var boundPositions = Set<Int>()
                for (i, n) in pattern.enumerated() {
                    switch n {
                    case .bound(let term):
                        boundPositions.insert(i)
                        guard let tid = try self.id(for: term, txn: txn) else {
                            throw DiomedeError.nonExistentTermError
                        }
                        restrictions[i] = tid
                    case .variable(let name, binding: _):
                        variableUsage[name, default: []].insert(i)
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
                        let id = UInt64(tid)
                        prefix.append(id)
                    }
                }
                return 0
            }
        } catch DiomedeError.nonExistentTermError {
            return []
        }
        
        let dups = variableUsage.filter { (u) -> Bool in u.value.count > 1 }
        let dupCheck = { (qids: [UInt64]) -> Bool in
            for (_, positions) in dups {
                let values = positions.map { qids[$0] }.sorted()
                if let f = values.first, let l = values.last {
                    if f != l {
                        return false
                    }
                }
            }
            return true
        }
        if let index = bestIndex {
            //            print("using index \(index.rawValue)")
            let quadIds = try self.quadIds(usingIndex: index, withPrefix: prefix)
            let filtered = quadIds.filter { (tids) -> Bool in
                for (i, value) in restrictions {
                    if tids[i] != value {
                        return false
                    }
                }
                return true
            }
            if dups.isEmpty {
                return filtered.map { $0.map { UInt64($0) } }
            } else {
                let f = filtered.filter(dupCheck)
                return f.map { $0.map { UInt64($0) } }
            }
        } else {
            var quadIds = [QuadID]()
            try self.quads_db.unescapingIterate { (_, spog) in
                let tids = try QuadID.fromData(spog)
                for (i, value) in restrictions {
                    if tids[i] != value {
                        return
                    }
                }
                quadIds.append(tids)
            }
            if dups.isEmpty {
                return quadIds.map { $0.values }
            } else {
                let qids = quadIds.map { $0.values }
                let f = qids.filter(dupCheck)
                return f.map { $0.map { UInt64($0) } }
            }
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

public enum DiomedeQuadStoreError: Error {
    case uniqueConstraintError(String)
    case countError(String)
    case indexError(String)
}

extension DiomedeQuadStore {
    public func verify() throws {
        var seen = [QuadID: Int]()

        print("Verifying quads table ...")
        try self.env.read { (txn) -> Int in
            var i = 0
            try self.quads_db.unescapingIterate(txn: txn) { (qidData, qidsData) in
                i += 1
                if i % 1000 == 0 {
                    print("\r\(i)", terminator: "")
                }
                let qid = Int.fromData(qidData)
                let tids = try QuadID.fromData(qidsData)
                if let q = seen[tids] {
                    throw DiomedeQuadStoreError.uniqueConstraintError("Quads table has non-unique quad (\(q), \(qid)): \(tids.values)")
                }
                seen[tids] = qid
            }
            print("\r", terminator: "")
            return 0
        }
        
        print("Verifying counts ...")
        let quads = self.count
        if quads != seen.count {
            throw DiomedeQuadStoreError.countError("Count is invalid (\(quads) <=> \(seen.count))")
        }
        
        print("Verifying indexes ...")
        for (indexOrder, pair) in self.fullIndexes {
            let (index, order) = pair
            print("- \(indexOrder.rawValue) ...")
            let entries = try index.count()
            if quads != entries {
                throw DiomedeQuadStoreError.countError("Index \(indexOrder.rawValue) has invalid count (\(entries) <=> \(quads))")
            }
            
            try index.unescapingIterator { (key, value) in
                let indexQid = Int.fromData(value)
                let indexOrderedIDs = try QuadID.fromData(key)
                var tids = Array<UInt64>(repeating: 0, count: 4)
                for (pos, tid) in zip(order, indexOrderedIDs.values) {
                    tids[pos] = tid
                }
                let q = QuadID(tids[0], tids[1], tids[2], tids[3])
                if let qid = seen[q] {
                    if indexQid != qid {
                        throw DiomedeQuadStoreError.indexError("Index \(indexOrder.rawValue) has wrong Quad identifier value for \(q.values): \(indexQid) <=> \(qid)")
                    }
                } else {
                    throw DiomedeQuadStoreError.indexError("Index \(indexOrder.rawValue) contains a non-existent quad: \(q.values)")
                }
            }
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

    public func _private_quadIds(usingIndex indexOrder: IndexOrder, withPrefix prefix: [UInt64]) throws -> [[UInt64]] {
        return try self.quadIds(usingIndex: indexOrder, withPrefix: prefix)
    }
    
}

extension DiomedeQuadStore {
    // These allow DiomedeQuadStore to conform to Kineo.QuadStoreProtocol and Kineo.LazyMaterializingQuadStore
    
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
    
    public func iterateTerms(handler: (UInt64, Term) -> ()) throws {
        let i2t = env.database(named: DiomedeQuadStore.StaticDatabases.id_to_term.rawValue)!
        try i2t.unescapingIterate { (k, v) in
            let key = UInt64(Int.fromData(k))
            let value = try Term.fromData(v)
            handler(key, value)
        }
    }
    
    public func graphTermIDs(in graph: UInt64) -> AnyIterator<UInt64> {
        do {
            return try self.termIDs(in: graph, positions: [0,2])
        } catch {
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
        var prefix = [UInt64]()
        var restrictions = [Int: UInt64]()
        var boundPositions = Set<Int>()
        var variableUsage = [String: Set<Int>]()
        do {
            try self.env.read { (txn) -> Int in
                for (i, n) in pattern.enumerated() {
                    switch n {
                    case .bound(let term):
                        boundPositions.insert(i)
                        restrictions[i] = try self.id(for: term, txn: txn)
                    case .variable(let name, binding: _):
                        variableUsage[name, default: []].insert(i)
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

        let dups = variableUsage.filter { (u) -> Bool in u.value.count > 1 }
        let dupCheck = { (qids: [UInt64]) -> Bool in
            for (_, positions) in dups {
                let values = positions.map { qids[$0] }.sorted()
                if let f = values.first, let l = values.last {
                    if f != l {
                        return false
                    }
                }
            }
            return true
        }

        if let indexOrder = bestIndex, dups.isEmpty, !prefix.isEmpty && prefix.count == boundPositions.count {
            // index completely covers the bound terms (of which there are more than zero),
            // and there are no repeated variables; matching on this index will give a precise count
//            print("using optimized count on index \(indexOrder.rawValue): \(pattern)")
            guard let (index, _) = self.fullIndexes[indexOrder] else {
                throw DiomedeError.indexError
            }
            let empty = Array(repeating: UInt64(0), count: 4)
            let lower = Array((prefix + empty).prefix(4))
            var upper = lower
            upper[prefix.count - 1] += 1
            let lowerKey = lower.map { Int($0) }.asData()
            let upperKey = upper.map { Int($0) }.asData()
            let count = try index.count(between: lowerKey, and: upperKey, inclusive: false)
            return count
        } else {
//            print("using non-optimized count for: \(pattern)")
            var count = 0
            for qids in try self.quadIds(matching: pattern) {
                if dups.isEmpty {
                    count += 1
                } else if dupCheck(qids) {
                    count += 1
                }
            }

            return count
        }
    }
    
    public func availableOrders(matching pattern: QuadPattern) throws -> [(order: [Quad.Position], fullOrder: [Quad.Position])] {
        var boundPositions = Set<Int>()
        for (i, n) in pattern.enumerated() {
            if case .bound = n {
                boundPositions.insert(i)
            }
        }
        
        var results = [(order: [Quad.Position], fullOrder: [Quad.Position])]()
        for i in try self.indexes(matchingBoundPositions: boundPositions) {
            let order = i.order()
            let removePrefixCount = order.prefix { boundPositions.contains($0) }.count
            let unbound = order.dropFirst(removePrefixCount)
            var positions = [Int: Quad.Position]()
            for (i, p) in Quad.Position.allCases.enumerated() {
                positions[i] = p
            }
            var ordering = [Quad.Position]()
            var fullOrdering = [Quad.Position]()
            for i in unbound {
                guard let p = positions[i] else { break }
                ordering.append(p)
            }
            for i in order {
                guard let p = positions[i] else { break }
                fullOrdering.append(p)
            }
            
            results.append((order: ordering, fullOrder: fullOrdering))
        }
        return results
    }
    
    public func quadIds(matching pattern: QuadPattern, orderedBy order: [Quad.Position]) throws -> [[UInt64]] {
        let chars : [String] = order.map {
            switch $0 {
            case .subject:
                return "s"
            case .predicate:
                return "p"
            case .object:
                return "o"
            case .graph:
                return "g"
            }
        }
        let name = chars.joined()
        guard let bestIndex = IndexOrder.init(rawValue: name) else {
            throw DiomedeQuadStoreError.indexError("No such index \(name)")
        }

        var variableUsage = [String: Set<Int>]()
        for (i, n) in pattern.enumerated() {
            if case .variable(let name, binding: _) = n {
                variableUsage[name, default: []].insert(i)
            }
        }

        let dups = variableUsage.filter { (u) -> Bool in u.value.count > 1 }
        let dupCheck = { (qids: [UInt64]) -> Bool in
            for (_, positions) in dups {
                let values = positions.map { qids[$0] }.sorted()
                if let f = values.first, let l = values.last {
                    if f != l {
                        return false
                    }
                }
            }
            return true
        }

        let (prefix, restrictions) = try self.prefix(for: pattern, in: bestIndex)
        let quadids = try self.quadIds(usingIndex: bestIndex, withPrefix: prefix, restrictedBy: restrictions)
        if dups.isEmpty {
            return quadids
        } else {
            return quadids.filter(dupCheck)
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
            var quadIds_verifyUnique = [[Int]: Bool]()
            var terms = Set<Term>()
            for (i, q) in quads.enumerated() {
                if i % 1000 == 0 {
                    let elapsed = CFAbsoluteTimeGetCurrent() - start
                    let tps = Double(i) / elapsed
                    if let progressHandler = self.progressHandler {
                        progressHandler(.loadProgress(count: i, rate: tps))
                    }
                    //                    print("\r\(humanReadable(count: i))) (\(tps) t/s)", terminator: "")
                }
                do {
                    var termIds = [Int]()
                    var newTerms = 0
                    for (i, t) in q.enumerated() {
                        terms.insert(t)
                        let d = try t.asData()
                        let term_key = try t.sha256()
                        var tid: Int
                        if let eid = try self.t2i_db.get(txn: txn, key: term_key) {
                            tid = Int.fromData(eid)
                        } else {
                            newTerms += 1
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
                    
                    // if newTerms == 0, all the terms in this quad were already in the database,
                    // and so we need to check below if this quad is already in the quads table
                    // (and indexes) and prevent it from being inserted twice.
                    // if newTerms > 0, then it necessarily can't be in the quads table or indexes
                    // because at least one term ID did not exist until just now.
                    quadIds_verifyUnique[termIds] = (newTerms == 0)
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

            let quadIds = try quadIds_verifyUnique.filter { (pair) throws -> Bool in
                if pair.value {
                    let tids = pair.key.map { UInt64($0) }
                    let exists = try self.quadExists(withIds: tids)
                    if exists {
//                        print("*** quad alread exits in the database: \(tids)")
                    }
                    return !exists
                } else {
                    return true
                }
            }.map { $0.key }
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

