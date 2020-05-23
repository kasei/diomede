import Foundation
import CryptoKit

import Diomede
import SPARQLSyntax
import Kineo

func bestIndex(matchingBoundPositions: Set<Int>, txn: OpaquePointer, from e: Environment, indexes: Environment.Database) throws -> String {
    let bound = matchingBoundPositions
    var scores = [(Int, String)]()
    try indexes.iterate(txn: txn) { (k, v) in
        let name = try String.fromData(k)
        print("evaluating \(name)")
        let order = Array<Int>.fromData(v)
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
    return scores[0].1
}

func iterateQuadIds(txn: OpaquePointer, from e: Environment, usingIndex index: Environment.Database, order: [Int], handler: ([Int]) throws -> ()) throws {
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

func iterateQuads(txn: OpaquePointer, from e: Environment, usingIndex index: Environment.Database, order: [Int], i2t: Environment.Database, handler: (Quad) throws -> ()) throws {
    let cache = LRUCache<Int, Term>(capacity: 4_096)
//    var qids = [[Int]]()
    try iterateQuadIds(txn: txn, from: e, usingIndex: index, order: order) { (tids) throws in
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

func loadRDF(from url: URL, into e: Environment) throws {
    let parser = RDFParserCombined()
    let graph = Term(iri: url.absoluteString)

    let i2t = e.database(named: "id_to_term")!
    let t2i = e.database(named: "term_to_id")!
    let stats = e.database(named: "stats")!
    let spog = e.database(named: "spog")!
    let indexes = e.database(named: "indexes")!
    var secondaryIndexes = [String: Environment.Database]()
    var secondaryIndexOrders = [String: [Int]]()
    try indexes.iterate { (k, v) in
        let name = try String.fromData(k)
        guard name != "spog" else { return }
        let order = Array<Int>.fromData(v)
        secondaryIndexOrders[name] = order
    }

    for name in secondaryIndexOrders.keys {
        let db = e.database(named: name)!
        secondaryIndexes[name] = db
    }

    try e.write { (txn) -> Int in

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
                    if let eid = try t2i.get(txn: txn, key: term_key) {
                        let tid = Int.fromData(eid)
                        quadIds.append(tid)
                    } else {
                        let tid = next
                        if (i == 3) {
                            newGraphs += 1
                        }
                        let i2t_pair = (tid, d)
                        let t2i_pair = (Data(SHA256.hash(data: d)), tid)
                        try i2t.insert(txn: txn, uniqueKeysWithValues: [i2t_pair])
                        try t2i.insert(txn: txn, uniqueKeysWithValues: [t2i_pair])
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
        
        for (name, index) in secondaryIndexes {
            let order = secondaryIndexOrders[name]!
            let indexOrderedKeys = quads.map { (q) in order.map({ q[$0].asData() }).reduce(Data()) { $0 + $1 } }
            let indexOrderedPairs = indexOrderedKeys.map { ($0, emptyValue) }
            try index.insert(txn: txn, uniqueKeysWithValues: indexOrderedPairs)
        }
        
        return 0
    }
}

let args = Array(CommandLine.arguments.dropFirst())
let cmd = CommandLine.arguments[0]
guard args.count >= 2 else {
    print("Usage: \(cmd) OP")

    exit(0)
}

let path = args[0]
let op = args[1]

if op == "create" {
    let f = FileManager.default
    if !f.fileExists(atPath: path) {
        try f.createDirectory(atPath: path, withIntermediateDirectories: true, attributes: nil)
    }
    guard let e = Environment(path: path) else {
        fatalError()
    }
    try e.createDatabase(named: "stats")
    try e.createDatabase(named: "indexes")
    try e.createDatabase(named: "term_to_id")
    try e.createDatabase(named: "id_to_term")
    let stats = e.database(named: "stats")!
    let indexes = e.database(named: "indexes")!
    try e.createDatabase(named: "spog")
    try e.createDatabase(named: "gpso")

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
    exit(0)
}


guard let e = Environment(path: path) else {
    fatalError()
}

let indexes = e.database(named: "indexes")!
var availableIndexes = Set<String>()
try indexes.iterate { (k, v) in
    let name = try String.fromData(k)
    availableIndexes.insert(name)
}

if op == "stats" {
    let stats = e.database(named: "stats")!
    for k in (["Version", "meta", "Last-Modified"]) {
        if let d = try stats.get(key: k) {
            let value = try String.fromData(d)
            print("\(k): \(value)")
        }
    }
    for k in (["graphs", "next_unassigned_id"]) {
        if let d = try stats.get(key: k) {
            let value = Int.fromData(d)
            print("\(k): \(value)")
        }
    }

    let spog = e.database(named: "spog")!
    try e.read { (txn) -> Int in
        let count = spog.count(txn: txn)
        print("Quads: \(count)")
        return 0
    }

    print("Indexes:")
    let indexes = e.database(named: "indexes")!
    try indexes.iterate { (k, v) in
        let name = try String.fromData(k)
        print("  - \(name)")
    }
} else if op == "import" {
    let filename = args[2]
    let url = URL(fileURLWithPath: filename)
    try loadRDF(from: url, into: e)
} else if op == "terms" {
    let i2t = e.database(named: "id_to_term")!
    try i2t.iterate { (k, v) in
        let key = Int.fromData(k)
        let value = try Term.fromData(v)
        
        let term_hash = try Data(SHA256.hash(data: value.asData()))
        print("\(key): \(value) (\(term_hash._hexValue))")
    }
    } else if op == "hashes" {
    let t2i = e.database(named: "term_to_id")!
    try t2i.iterate { (k, v) in
        let key = k
        let value = Int.fromData(v)
        print("\(key._hexValue): \(value)")
    }
} else if op == "qids" {
    let spog = e.database(named: "spog")!
    try spog.iterate { (qids, _) in
        print("quad entry: \(qids._hexValue)")
        var ids = [Int]()
        let strideBy = qids.count / 4
        for i in stride(from: 0, to: qids.count, by: strideBy) {
            let data = qids[i..<(i+strideBy)]
            let tid = Int.fromData(data)
            ids.append(tid)
        }
        print(ids.map { "\($0)" }.joined(separator: " "))
    }
} else if availableIndexes.contains(op) {
    let indexName = op
    var indexOrders = [String: [Int]]()
    let indexes = e.database(named: "indexes")!
    try indexes.iterate { (k, v) in
        let name = try String.fromData(k)
        let order = Array<Int>.fromData(v)
        indexOrders[name] = order
    }
    let order = indexOrders[indexName]!
    let index = e.database(named: indexName)!
    let i2t = e.database(named: "id_to_term")!

    try e.read { (txn) -> Int in
        try iterateQuads(txn: txn, from: e, usingIndex: index, order: order, i2t: i2t) { (q) in
            print("\(q) .")
        }
        return 0
    }
} else if op == "indexes" {
    for name in availableIndexes {
        print(name)
    }
} else if op == "bestIndex" {
    // -- bestIndex s p g
    let indexes = e.database(named: "indexes")!
    let positions = args[2...].joined()
    var bound = Set<Int>()
    for p in args[2...] {
        switch p.lowercased() {
        case "s":
            bound.insert(0)
        case "p":
            bound.insert(1)
        case "o":
            bound.insert(2)
        case "g":
            bound.insert(3)
        default:
            break
        }
    }
    try e.read(handler: { (txn) -> Int in
        let index = try bestIndex(matchingBoundPositions: bound, txn: txn, from: e, indexes: indexes)
        print("Best index for <\(positions)>: \(index)")
        return 0
    })
}

extension Term: DataEncodable {
    public func asData() throws -> Data {
        let s: String
        switch self.type {
        case .blank:
            s = "B\"" + self.value
        case .iri:
            s = "I\"" + self.value
        case .language(let lang):
            s = "L\(lang)\"" + self.value
        case .datatype(let dt):
            s = "D\(dt.value)\"" + self.value
        }
        return try s.asData()
    }
    
    public static func fromData(_ data: Data) throws -> Term {
        let s = try String.fromData(data)
        guard s.count > 2 else {
            throw DiomedeError.encodingError
        }
        let c = s.first!
        guard let i = s.firstIndex(of: "\"") else {
            throw DiomedeError.encodingError
        }
        let value = String(s.suffix(from: s.index(after: i)))
        switch c {
        case "B":
            return Term(value: value, type: .blank)
        case "I":
            return Term(value: value, type: .iri)
        case "L":
            let lang = String(s.dropFirst().prefix(while: { $0 != "\"" }))
            return Term(value: value, type: .language(lang))
        case "D":
            let dt = String(s.dropFirst(1).prefix(while: { $0 != "\"" }))
            return Term(value: value, type: .datatype(.custom(dt)))
        default:
            throw DiomedeError.unknownError
        }
    }
    
    
}
