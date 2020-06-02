import Foundation
import CryptoKit

import Diomede
import DiomedeQuadStore
import SPARQLSyntax

func getCurrentTime() -> CFAbsoluteTime {
    return CFAbsoluteTimeGetCurrent()
}

func time(_ name: String, block: () throws -> ()) rethrows {
    let start = getCurrentTime()
    try block()
    let end = getCurrentTime()
    let elapsed = end - start
    print("[\(name)] Elapsed time: \(elapsed)s")
}

func humanReadable(bytes: Int) -> String{
    var names = [" bytes", "kB", "MB", "GB"]
    var unit = names.remove(at: 0)
    var size = bytes
    while !names.isEmpty && size >= 1024 {
        unit = names.remove(at: 0)
        size /= 1024
    }
    return "\(size)\(unit)"
}

func printCharacteristicSets(for graph: Term, in dataset: CharacteristicDataSet) {
    let sets = dataset.sets.sorted { $0.count >= $1.count }
    print("")
    for set in sets {
        print("Characteristic Set: count = \(set.count)")
        for pred in set.predicates.sorted() {
            let occurences = set.predCounts[pred]!
            print(String(format: "    %4d \(pred)", occurences))
        }
        print("")
    }
}

let args = Array(CommandLine.arguments.dropFirst())
let pname = CommandLine.arguments[0]
guard args.count >= 2 else {
    print("""
        Usage:
        
        \(pname) -q DATABASE.db COMMAND [ARGUMENTS]
        
        Commands:
        
        create
            Create an empty quadstore database.

        stats
            Print metadata and statistics about the quadstore.

        terms
            Print all RDF terms in the quadstore with each term's
            associated term ID.

        hashes
            Print all the SHA256 hashes of all RDF terms in the
            quadstore with ID of the corresponding term.

        INDEXNAME
            (Where INDEXNAME is any permutation of "spog".)
            
            If INDEXNAME is a quad index available in the quadstore,
            prints all the quads in the quadstore in the index's
            sort order.
        
            Otherwise, an error is reported and a non-zero value is
            returned.

        addindex NAME
            Add an index to the quadstore.
            NAME may be one of:
                * a permutation of "spog" to add a quad ordering index
                * "cs" to add a Characteristic Sets index

        dropindex NAME
            Drop an index from the quadstore.

        graphs
            Print the IRIs of all graphs in the quadstore.
        
        graphterms GRAPH-IRI
            Print all the terms (used as either subject or object)
            in specified graph.

        triples GRAPH-IRI
            Print all the triples in the specified graph.

        quads
            Print all the quads in the quadstore.

        indexes
            Print the name of all indexes in the quadstore.

        bestIndex POSITION1 [POSITION2...]
            Print the name of the index best suited to match quads
            with the named positions bound. Position names are one
            of: "subject", "predicate", "object", or "graph".
        
            If no quad indexes are available, an error is reported
            and a non-zero value is returned.

        cs GRAPH-IRI
            Print the Characteristic Sets for the specified graph.
        
        """)
    
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
    print("failed to open environment: \(path)")
    exit(1)
}

let csDatabaseName = "characteristicSets"

let indexes = e.database(named: DiomedeQuadStore.StaticDatabases.fullIndexes.rawValue)!
var availableQuadIndexes = Set<String>()
try indexes.iterate { (k, v) in
    let name = try String.fromData(k)
    availableQuadIndexes.insert(name)
}

if op == "stats" {
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }
    
    let stats = e.database(named: DiomedeQuadStore.StaticDatabases.stats.rawValue)!
    let graphs = e.database(named: DiomedeQuadStore.StaticDatabases.graphs.rawValue)!
    for k in (["Diomede-Version", "meta", "Last-Modified"]) {
        if let d = try stats.get(key: k) {
            let value = try String.fromData(d)
            if !value.isEmpty {
                print("\(k): \(value)")
            }
        }
    }
    
    if let version = try qs.effectiveVersion() {
        print("Effective version: \(version)")
    }
    
    for k in ([DiomedeQuadStore.NextIDKey.term, DiomedeQuadStore.NextIDKey.quad]) {
        if let d = try stats.get(key: k.rawValue) {
            let value = Int.fromData(d)
            print("\(k): \(value)")
        }
    }
    let gcount = try graphs.count()
    print("graphs: \(gcount)")
    
    let quads = e.database(named: "quads")!
    try e.read { (txn) -> Int in
        let count = quads.count(txn: txn)
        let bytes = quads.size(txn: txn)
        print("Quads: \(count) (\(humanReadable(bytes: bytes)))")
        return 0
    }
    
    let indexes = e.database(named: DiomedeQuadStore.StaticDatabases.fullIndexes.rawValue)!
    let indexCount = try indexes.count()
    if indexCount == 0 {
        print("No indexes")
    } else {
        print("Indexes:")
        let indexNames = qs.fullIndexes.keys.map { $0.rawValue }.sorted()
        if !indexNames.isEmpty {
            print("  - Quad Orderings:")
            for name in indexNames {
                try e.read { (txn) in
                    guard let index = e.database(txn: txn, named: name) else {
                        return 1
                    }
                    let bytes = index.size(txn: txn)
                    print("    - \(name) (\(humanReadable(bytes: bytes)))")
                    return 0
                }
            }
        }
    }
    
    let databases = Set(try e.databases())
    if databases.contains(csDatabaseName) {
        if let db = e.database(named: csDatabaseName) {
            try e.read { (txn) in
                let bytes = db.size(txn: txn)
                print("  - Characteristic Sets (\(humanReadable(bytes: bytes)))")
                return 0
            }
            let count = try db.count()
            let avg = count / gcount
            print("    - \(count) sets (~\(avg) per graph)")
        }
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
} else if let indexOrder = DiomedeQuadStore.IndexOrder(rawValue: op) {
    guard availableQuadIndexes.contains(op) else {
        print("Index \(op) not in quadstore")
        exit(1)
    }
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }
    
    let i = try qs.quads(using: indexOrder)
    for q in i {
        print("\(q)")
    }
} else if op == "addindex" {
    guard args.count > 2 else {
        print("Index name argument required")
        exit(1)
    }
    let name = args[2]
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }
    
    if name == "cs" {
        print("Generating Characteristic Sets index")
        try qs.computeCharacteristicSets()
    } else {
        guard let indexOrder = DiomedeQuadStore.IndexOrder(rawValue: name) else {
            throw DiomedeError.indexError
        }
        print("Generating \(indexOrder.rawValue) index")
        try qs.addFullIndex(order: indexOrder)
    }
} else if op == "dropindex" {
    guard args.count > 2 else {
        print("Index name argument required")
        exit(1)
    }
    let name = args[2]
    guard var qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }
    if name == "cs" {
        print("Dropping Characteristic Sets index")
        try qs.dropCharacteristicSets()
    } else {
        guard let indexOrder = DiomedeQuadStore.IndexOrder(rawValue: name) else {
            throw DiomedeError.indexError
        }
        print("Dropping \(indexOrder.rawValue) index")
        try qs.dropFullIndex(order: indexOrder)
    }
} else if op == "triples" {
    let line = args[2]
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
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
        print("Failed to construct quadstore")
        exit(1)
    }
    let qp = QuadPattern.all
    let i = try qs.quads(matching: qp)
    for q in i {
        print(q)
    }
} else if op == "graphterms" {
    let line = args[2]
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }
    let term = Term(iri: line)
    for o in qs.graphTerms(in: term) {
        print(o)
    }
} else if op == "graphs" {
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }
    for g in try qs.namedGraphs() {
        print(g.value)
    }
} else if op == "indexes" {
    for name in availableQuadIndexes {
        print(name)
    }
    let databases = Set(try e.databases())
    if databases.contains(csDatabaseName) {
        print("cs")
    }
} else if op == "bestIndex" {
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }
    // -- bestIndex s p g
    let positions = args[2...].map { $0.lowercased().prefix(1) }.joined()
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
            print("\(index.rawValue)")
        } else {
            print("No index available for <\(positions)>")
            exit(1)
        }
        return 0
    }
    
} else if op == "cs" {
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }

    let databases = Set(try e.databases())
    if !databases.contains(csDatabaseName) {
        print("No Characteristic Sets index found")
        exit(1)
    }


    do {
        if args.count <= 2 {
            var count = 0
            for graph in qs.graphs() {
                print("Graph: \(graph)")
                let dataset = try qs.characteristicSets(for: graph)
                count += dataset.sets.count
                printCharacteristicSets(for: graph, in: dataset)
            }
            print("Total Number of Characteristic Sets: \(count) ")
        } else {
            let line = args[2]
            let graph = Term(iri: line)
            do {
                let dataset = try qs.characteristicSets(for: graph)
                
                print("Graph: \(graph)")
                printCharacteristicSets(for: graph, in: dataset)
                print("Number of Characteristic Sets: \(dataset.sets.count) ")
            } catch DiomedeError.nonExistentTermError {
                print("No characteristic set found for graph \(graph)")
            }
        }
    } catch DiomedeError.indexError {
        print("No characteristic sets index found")
    }
} else if op == "pred-card" {
    guard args.count > 3 else {
        print("Index name argument required")
        exit(1)
    }
    let graph = Term(iri: args[2])
    let pred = Term(iri: args[3])
    
    guard let qs = DiomedeQuadStore(path: path) else {
        print("Failed to construct quadstore")
        exit(1)
    }

    let databases = Set(try e.databases())
    if !databases.contains(csDatabaseName) {
        print("No Characteristic Sets index found")
        exit(1)
    }

    do {
        var t1 = TriplePattern.all
        var q1 = QuadPattern.all
        t1.predicate = .bound(pred)

        q1.predicate = .bound(pred)
        q1.graph = .bound(graph)
        
        try time("Estimated") {
            var estimatedCardinality = 0.0
//            for graph in qs.graphs() {
                let dataset = try qs.characteristicSets(for: graph)
                let c1 = try dataset.starCardinality(matching: [t1], in: graph, store: qs)
                estimatedCardinality += c1
//            }
            print("Estimated: \(estimatedCardinality)")
        }

        try time("Actual") {
            let actualCardinality = try qs.countQuads(matching: q1)
            print("Actual   : \(actualCardinality)")
        }
    } catch DiomedeError.indexError {
        print("No characteristic sets index found")
    }
}

