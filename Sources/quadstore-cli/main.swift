import Foundation
import CryptoKit

import Diomede
import DiomedeQuadStore
import SPARQLSyntax

let args = Array(CommandLine.arguments.dropFirst())
let cmd = CommandLine.arguments[0]
guard args.count >= 2 else {
    print("Usage: \(cmd) OP")

    exit(0)
}


//struct Stdout: TextOutputStream {
//  internal init() {}
//
//  internal mutating func write(_ string: String) {
//    if string.isEmpty { return }
//    var string = string
//    _ = string.withUTF8 { utf8 in
//        fwrite(utf8.baseAddress!, 1, utf8.count, stdout)
//    }
//  }
//}
//
//func printQuads(_ iter: AnyIterator<Quad>) {
//    var out = Stdout()
//    let seq = AnySequence(iter)
//    for q in seq {
//        for t in q {
//            t.printNTriplesString(to: &out)
//        }
//        print(" .")
//    }
//}

let path = args[0]
let op = args[1]

if op == "create" {
    guard let _ = DiomedeQuadStore(path: path, create: true) else {
        print("Failed to create QuadStore")
        exit(1)
    }
    exit(0)
}

guard let e = Environment(path: path) else {
    fatalError("failed to open environment: \(path)")
}

let indexes = e.database(named: DiomedeQuadStore.StaticDatabases.fullIndexes.rawValue)!
var availableIndexes = Set<String>()
try indexes.iterate { (k, v) in
    let name = try String.fromData(k)
    availableIndexes.insert(name)
}

if op == "stats" {
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }

    let stats = e.database(named: DiomedeQuadStore.StaticDatabases.stats.rawValue)!
    let graphs = e.database(named: DiomedeQuadStore.StaticDatabases.graphs.rawValue)!
    for k in (["Version", "meta", "Last-Modified"]) {
        if let d = try stats.get(key: k) {
            let value = try String.fromData(d)
            print("\(k): \(value)")
        }
    }
    
    if let version = try qs.effectiveVersion() {
        print("Effective version: \(version)")
    }
    
    for k in (["next_unassigned_term_id"]) {
        if let d = try stats.get(key: k) {
            let value = Int.fromData(d)
            print("\(k): \(value)")
        }
    }
    let gcount = try graphs.count()
    print("graphs: \(gcount)")

    let quads = e.database(named: "quads")!
    try e.read { (txn) -> Int in
        let count = quads.count(txn: txn)
        print("Quads: \(count)")
        return 0
    }

    print("Indexes:")
    let indexes = e.database(named: DiomedeQuadStore.StaticDatabases.fullIndexes.rawValue)!
    try indexes.iterate { (k, v) in
        let name = try String.fromData(k)
        print("  - \(name)")
    }
    
    let databases = Set(try e.databases())
    if databases.contains("cs") {
        print("  - Characteristic Sets")
    }
    
//} else if op == "load" || op == "import" {
//    let filename = args[2]
//    let url = URL(fileURLWithPath: filename)
//    guard let qs = DiomedeQuadStore(path: path) else {
//        fatalError("Failed to construct quadstore")
//    }
//
//
//    let parser = RDFParserCombined()
//    let graph = Term(iri: url.absoluteString)
//    var quads = [Quad]()
//    try parser.parse(file: url.path, base: graph.value) { (s, p, o) in
//        let q = Quad(subject: s, predicate: p, object: o, graph: graph)
//        quads.append(q)
//    }
//
//    let now = UInt64(Date().timeIntervalSince1970)
//    try qs.load(version: now, quads: quads)
//} else if op == "match" {
//    let line = args[2]
//    guard let qs = DiomedeQuadStore(path: path) else {
//        fatalError("Failed to construct quadstore")
//    }
//    let p = NTriplesPatternParser(reader: "")
//    guard let qp = p.parseQuadPattern(line: line) else {
//        fatalError("Bad quad pattern")
//    }
//    let i = try qs.quads(matching: qp)
//    printQuads(i)
} else if op == "terms" {
    let i2t = e.database(named: DiomedeQuadStore.StaticDatabases.id_to_term.rawValue)!
    try i2t.iterate { (k, v) in
        let key = Int.fromData(k)
        let value = try Term.fromData(v)
        
        let term_hash = try Data(SHA256.hash(data: value.asData()))
        print("\(key): \(value) (\(term_hash._hexValue))")
    }
} else if op == "hashes" {
    let t2i = e.database(named: DiomedeQuadStore.StaticDatabases.term_to_id.rawValue)!
    try t2i.iterate { (k, v) in
        let key = k
        let value = Int.fromData(v)
        print("\(key._hexValue): \(value)")
    }
} else if op == "qids" {
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    try e.read { (txn) throws -> Int in
        try qs._private_iterateQuadIds(txn: txn) { (qid, ids) in
            let tids = ids.map { "\($0)" }.joined(separator: " ")
            print("\(qid): \(tids)")
        }
        return 0
    }
} else if availableIndexes.contains(op) {
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    guard let indexOrder = DiomedeQuadStore.IndexOrder(rawValue: op) else {
        throw DiomedeError.indexError
    }
    try e.read { (txn) throws -> Int in
        try qs._private_iterateQuads(txn: txn, usingIndex: indexOrder) { (q) in
            print("\(q)")
        }
        return 0
    }
} else if op == "index" {
    let name = args[2]
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }

    if name == "cs" {
        print("Generating Characteristic Sets")
        try qs.computeCharacteristicSets()
    } else {
        guard let indexOrder = DiomedeQuadStore.IndexOrder(rawValue: name) else {
            throw DiomedeError.indexError
        }
        try qs.addFullIndex(order: indexOrder)
    }
} else if op == "dropindex" {
    let name = args[2]
    guard var qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    guard let indexOrder = DiomedeQuadStore.IndexOrder(rawValue: name) else {
        throw DiomedeError.indexError
    }
    try qs.dropFullIndex(order: indexOrder)
} else if op == "triples" {
    let line = args[2]
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    let graph = Term(iri: line)
    var qp = QuadPattern.all
    qp.graph = .bound(graph)
    
    for q in try qs.quads(matching: qp) {
        let t = q.triple
        print(t)
    }
} else if op == "quads" {
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    let qp = QuadPattern.all
    let i = try qs.quads(matching: qp)
    for q in i {
        print(q)
    }
} else if op == "graphterms" {
    let line = args[2]
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    let term = Term(iri: line)
    for o in qs.graphTerms(in: term) {
        print(o)
    }
} else if op == "graphs" {
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    for g in try qs.namedGraphs() {
        print(g)
    }
} else if op == "indexes" {
    for name in availableIndexes {
        print(name)
    }
} else if op == "bestIndex" {
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    // -- bestIndex s p g
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
    
    try e.read { (txn) -> Int in
        if let index = try qs._private_bestIndex(matchingBoundPositions: bound, txn: txn) {
            print("Best index for <\(positions)>: \(index.rawValue)")
        } else {
            print("No index available for <\(positions)>")
        }
        return 0
    }
} else if op == "cs" {
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }

    let graph = Array(qs.graphs()).first!
    guard let sets = try qs.characteristicSets(for: graph) else {
        fatalError("No characteristic sets index found")
    }

    var t1 = TriplePattern.all
    t1.predicate = .bound(Term.rdf("type"))
    t1.object = .bound(Term(iri: "http://xmlns.com/foaf/0.1/Person"))

    var t2 = TriplePattern.all
    t2.predicate = .bound(Term(iri: "http://xmlns.com/foaf/0.1/nick"))
    
//    let c1 = try sets.starCardinality(matching: [t1], in: graph, store: qs)
//    print("[t1] Cardinality: \(c1)")
    
    let c2 = try sets.starCardinality(matching: [t1, t2], in: graph, store: qs)
    print("[t1] Cardinality: \(c2)")
}
