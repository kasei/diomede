import Diomede
import Foundation

let args = Array(CommandLine.arguments.dropFirst())
let cmd = CommandLine.arguments[0]

guard args.count >= 1 else {
    print("Usage: \(cmd) ENVPATH DATABASE OP")
    exit(1)
}

let path = args[0]
guard let e = Environment(path: path) else {
    fatalError()
}

guard args.count >= 2 else {
    print("Usage: \(cmd) ENVPATH DATABASE OP")
    for d in try e.databases() {
        print("Database: \(d)")
    }
    exit(1)
}


let dbname = args[1]
let op = args[2]


if op == "create" {
    try e.createDatabase(named: dbname)
} else if op == "drop" {
    try e.dropDatabase(named: dbname)
} else {
    if let db = e.database(named: dbname) {
        if args.count == 1 {
            try db.iterate { (k, v) in
                guard let key = String(data: k, encoding: .utf8),
                    let value = String(data: v, encoding: .utf8) else { return }
                print("\(key) -> \(value)")
            }
        } else {
            if op == "get" {
                guard let k = args[3].data(using: .utf8) else {
                    print("Invalid key")
                    exit(1)
                }
                guard let data = try db.get(key: k), let value = String(data: data, encoding: .utf8) else {
                    print("Invalid value")
                    exit(1)
                }
                print("Value: \(value)")
            } else if op == "add" {
                    guard let k = args[3].data(using: .utf8),
                        let v = args[4].data(using: .utf8) else {
                            print("Invalid key-value pair")
                            exit(1)
                    }
                    try db.insert(uniqueKeysWithValues: [(k,v)])
            } else if op == "between" {
                guard let l = args[3].data(using: .utf8),
                    let u = args[4].data(using: .utf8) else {
                        print("Invalid bounds")
                        exit(1)
                }
                
                try db.iterate(between: l, and: u) { (k, v) in
                    guard let key = String(data: k, encoding: .utf8),
                        let value = String(data: v, encoding: .utf8) else { return }
                    print("\(key) -> \(value)")
                }
            } else if op == "dump" {
                try db.iterate { (k, v) in
                    guard let key = String(data: k, encoding: .utf8),
                        let value = String(data: v, encoding: .utf8) else { return }
                    print("\(key) -> \(value)")
                }
            } else {
                print("OP: \(op)")
            }
        }
    }
}
