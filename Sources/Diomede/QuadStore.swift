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
    }
    var env: Environment
    var t2i: Environment.Database
    var i2t: Environment.Database
    var stats: Environment.Database
    var next_unassigned_id: Int
    var fullIndexes: [IndexOrder: (Environment.Database, [Int])]
    
    init?(environment e: Environment) {
        self.env = e
        
        guard let indexes = e.database(named: "fullIndexes"),
            let stats = e.database(named: "stats"),
            let i2t = e.database(named: "id_to_term"),
            let t2i = e.database(named: "term_to_id") else { return nil }
        self.i2t = i2t
        self.t2i = t2i
        self.stats = stats
        
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
                try e.createDatabase(named: "stats")
                try e.createDatabase(named: "fullIndexes")
                try e.createDatabase(named: "term_to_id")
                try e.createDatabase(named: "id_to_term")
                let stats = e.database(named: "stats")!
                try e.createDatabase(named: "spog")
                try e.createDatabase(named: "gpso")
                
                let indexes = e.database(named: "fullIndexes")!
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
                    ("graphs", 0),
                    ("next_unassigned_id", 1)
                ])
                self.init(environment: e)
            } catch {
                return nil
            }
        } else {
            guard let e = Environment(path: path) else { return nil }
            self.init(environment: e)
        }
    }

    public func read(handler: (OpaquePointer) throws -> Int) throws {
        try self.env.read(handler: handler)
    }

    public func write(handler: (OpaquePointer) throws -> Int) throws {
        try self.env.write(handler: handler)
    }

    public func bestIndex(matchingBoundPositions: Set<Int>, txn: OpaquePointer) throws -> String {
        let bound = matchingBoundPositions
        var scores = [(Int, String)]()
        for (k, v) in self.fullIndexes {
            let name = k.rawValue
            let order = v.1
            var score = 0
            for pos in order {
                if bound.contains(pos) {
                    score += 1
                } else {
                    break
                }
            }
            scores.append((score, name))
        }
        
        scores.sort(by: { $0.0 > $1.0 })
        guard let first = scores.first else {
            throw DiomedeError.indexError
        }
        return first.1
    }

    // TODO: shouldn't be public
    public func iterateQuadIds(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: ([Int]) throws -> ()) throws {
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
        
        if false {
            // assuming we're using the spog index, this will place a range restriction on the values of the subject: ids in the range [0, 100) (exclusive upper bound)
            let lower = [0,0,0,0].asData() // .suffix(32) // .suffix because asData uses the first 8 bytes to encode the array length
            let upper = [100,0,0,0].asData() // .suffix(32) // .suffix because asData uses the first 8 bytes to encode the array length
            print("from \(lower._hexValue)")
            print("to   \(upper._hexValue)")
            try index.iterate(txn: txn, between: lower, and: upper, handler: iterationHandler)
        } else {
            try index.iterate(txn: txn, handler: iterationHandler)
        }
    }

    public func iterateQuads(txn: OpaquePointer, usingIndex indexOrder: IndexOrder, handler: (Quad) throws -> ()) throws {
        let cache = LRUCache<Int, Term>(capacity: 4_096)
    //    var qids = [[Int]]()
        try iterateQuadIds(txn: txn, usingIndex: indexOrder) { (tids) throws in
    //        qids.append(tids)
            var terms = [Term]()
            for tid in tids {
                if let term = cache[tid] {
                    terms.append(term)
                } else if let tdata = try i2t.get(txn: txn, key: tid) {
                    let term = try Term.fromData(tdata)
                    terms.append(term)
                    cache[tid] = term
                } else {
                    print("no term for ID \(tid)")
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

    public func loadRDF(from url: URL) throws {
        let parser = RDFParserCombined()
        let graph = Term(iri: url.absoluteString)
        guard let (spog, _) = self.fullIndexes[.spog] else {
            throw DiomedeError.indexError
        }

//        var secondaryIndexes = [IndexOrder: Environment.Database]()
//        var secondaryIndexOrders = [IndexOrder: [Int]]()
//        try indexes.iterate { (k, v) in
//            let name = try String.fromData(k)
//            guard name != "spog" else { return }
//            let order = Array<Int>.fromData(v)
//            secondaryIndexOrders[name] = order
//        }

        try env.write { (txn) -> Int in
            let nextData = try stats.get(txn: txn, key: "next_unassigned_id")
            var next = nextData.map { Int.fromData($0) } ?? 1
            
            var newGraphs = 0
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
                        if let eid = try self.t2i.get(txn: txn, key: term_key) {
                            let tid = Int.fromData(eid)
                            quadIds.append(tid)
                        } else {
                            let tid = next
                            if (i == 3) {
                                newGraphs += 1
                            }
                            let i2t_pair = (tid, d)
                            let t2i_pair = (Data(SHA256.hash(data: d)), tid)
                            try self.i2t.insert(txn: txn, uniqueKeysWithValues: [i2t_pair])
                            try self.t2i.insert(txn: txn, uniqueKeysWithValues: [t2i_pair])
                            quadIds.append(tid)
                            next += 1
                        }
                    }
                    quads.append(quadIds)
                } catch {}
            }

            
            var graphs = try stats.get(txn: txn, key: "graphs").map { Int.fromData($0) } ?? 0
            graphs += newGraphs
            
            try stats.insert(txn: txn, uniqueKeysWithValues: [
                ("next_unassigned_id", next),
                ("graphs", graphs)
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
