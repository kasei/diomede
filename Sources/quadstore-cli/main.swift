import Foundation
import CryptoKit

import Diomede
import SPARQLSyntax
import Kineo



let args = Array(CommandLine.arguments.dropFirst())
let cmd = CommandLine.arguments[0]
guard args.count >= 2 else {
    print("Usage: \(cmd) OP")

    exit(0)
}

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
    fatalError()
}

let indexes = e.database(named: "fullIndexes")!
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
    let indexes = e.database(named: "fullIndexes")!
    try indexes.iterate { (k, v) in
        let name = try String.fromData(k)
        print("  - \(name)")
    }
} else if op == "load" || op == "import" {
    let filename = args[2]
    let url = URL(fileURLWithPath: filename)
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    try qs.loadRDF(from: url)
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
    guard let qs = DiomedeQuadStore(path: path) else {
        fatalError("Failed to construct quadstore")
    }
    try qs.read { (txn) throws -> Int in
        try qs.iterateQuadIds(txn: txn, usingIndex: .spog) { (ids) in
            print(ids.map { "\($0)" }.joined(separator: " "))
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
    try qs.read { (txn) throws -> Int in
        try qs.iterateQuads(txn: txn, usingIndex: indexOrder) { (q) in
            print("\(q)")
        }
        return 0
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
    //    let indexes = e.database(named: "fullIndexes")!
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
    
    try qs.read { (txn) -> Int in
        let index = try qs.bestIndex(matchingBoundPositions: bound, txn: txn)
        print("Best index for <\(positions)>: \(index)")
        return 0
    }
}
