import Foundation
import LMDB

public protocol DataEncodable {
    func asData() throws -> Data
    static func fromData(_ data: Data) throws -> Self
}

extension Data : DataEncodable {
    public func asData() -> Data { return self }
    public static func fromData(_ data: Data) -> Self { return data }
}

extension Data {
    public var _hexValue: String {
        var s = "0x"
        for b in self {
            s += String(format: "%02x", b)
        }
        return s
    }
    
    public var _stringValue: String {
        if let s = String(data: self, encoding: .utf8) {
            return s
        } else {
            return _hexValue
        }
    }
}

extension Array: DataEncodable where Element == Int {
    public func asData() -> Data {
        let data = self.map { $0.asData() }
        let combined = data.reduce(Data(), { $0 + $1 })
        return combined
    }
    public static func fromData(_ data: Data) -> Self {
        let stride = 8
        let count = data.count / stride
        var i = data.startIndex
        var values = [Int]()
        for _ in 0..<count {
            let v = Int.fromData(data[i...])
            values.append(v)
            i = data.index(i, offsetBy: stride)
        }
        return values
    }
}

extension Int : DataEncodable {
    public func asData() -> Data {
        var be = Int64(self).bigEndian
        return Data(bytes: &be, count: 8)
    }
    public static func fromData(_ data: Data) -> Self {
        let be: Int64 = data.withUnsafeBytes { $0.pointee }
        return Int(Int64(bigEndian: be))
    }
}

extension String : DataEncodable {
    public func asData() throws -> Data {
        guard let data = self.data(using: .utf8) else {
            throw DiomedeError.encodingError
        }
        return data
    }
    public static func fromData(_ data: Data) throws -> Self {
        guard let s = String(data: data, encoding: .utf8) else {
            throw DiomedeError.encodingError
        }
        return s
    }
}

public enum DiomedeError: Error {
    case unknownError
    case encodingError
    case databaseOpenError
    case cursorOpenError
    case cursorError
    case insertError
    case getError
}

public class Environment {
    var env : OpaquePointer?
    
    public init?(path: String) {
        if (mdb_env_create(&env) != 0) {
            return nil
        }
        
        if (mdb_env_set_maxreaders(env, 1) != 0) {
            mdb_env_close(env)
            return nil
        }
        
        if (mdb_env_set_mapsize(env, 8192000000) != 0) {
            mdb_env_close(env)
            return nil
        }
        
        if (mdb_env_set_maxdbs(env, 256) != 0) {
            mdb_env_close(env)
            return nil
        }
        
        
        if (mdb_env_open(env, path, UInt32(MDB_NOSYNC), 0o0640) != 0) {
            mdb_env_close(env)
            return nil
        }
    }
    
    deinit {
        mdb_env_close(env)
    }
    
    public func read(handler: (OpaquePointer) throws -> Int) throws {
        try run(flags: UInt32(MDB_RDONLY), handler: handler)
    }
    
    public func write(handler: (OpaquePointer) throws -> Int) throws {
        try run(flags: 0, handler: handler)
    }
    
    private func run(flags: UInt32, handler: (OpaquePointer) throws -> Int) rethrows {
        var txn : OpaquePointer?
        if (mdb_txn_begin(env, nil, flags, &txn) == 0) {
            if let txn = txn {
                do {
                    let _ = try handler(txn)
                    mdb_txn_commit(txn)
                } catch let e {
                    mdb_txn_abort(txn)
                    throw e
                }
            }
        }
    }
    
    public func databases() throws -> [String] {
        var names = [String]()
        try self.read { (txn) throws -> Int in
            var dbi: MDB_dbi = 0
            let r = withUnsafeMutablePointer(to: &dbi) { (dbip) -> Int in
                if (mdb_dbi_open(txn, nil, 0, dbip) != 0) {
                    return 1
                }
                return 0
            }
            if (r != 0) {
                throw DiomedeError.databaseOpenError
            }
            
            var key = MDB_val(mv_size: 0, mv_data: nil)
            var data = MDB_val(mv_size: 0, mv_data: nil)

            var cursor: OpaquePointer?
            guard (mdb_cursor_open(txn, dbi, &cursor) == 0) else {
                throw DiomedeError.cursorOpenError
            }
            
            var op = MDB_FIRST
            while (mdb_cursor_get(cursor, &key, &data, op) == 0) {
                op = MDB_NEXT
                let data = Data(bytes: key.mv_data, count: key.mv_size)
                if let name = String(data: data, encoding: .utf8) {
                    names.append(name)
                }
            }
            mdb_cursor_close(cursor)
            return 0
        }
        return names
    }
    
    public func createDatabase(named name: String) throws {
        try self.write { (txn) -> Int in
            var dbi: MDB_dbi = 0
            if (mdb_dbi_open(txn, name, UInt32(MDB_CREATE), &dbi) != 0) {
                throw DiomedeError.databaseOpenError
            }
            return 0
        }
    }
    
    public func dropDatabase(named name: String) throws {
        guard let db = self.database(named: name) else {
            throw DiomedeError.databaseOpenError
        }
        try self.write { (txn) -> Int in
            let r =  mdb_drop(txn, db.dbi, 1)
            return Int(r)
        }
    }
    
    public func database(named name: String) -> Database? {
        let d = Database(environment: self, name: name)
        return d
    }

    public class Database {
        var env: Environment
        var dbi: MDB_dbi = 0

        init?(environment: Environment, name: String) {
            self.env = environment
            do {
                try env.read { (txn) throws -> Int in
                    var dbi: MDB_dbi = 0
                    let r = withUnsafeMutablePointer(to: &dbi) { (dbip) -> Int in
                        if (mdb_dbi_open(txn, name, 0, dbip) != 0) {
                            return 1
                        }
                        return 0
                    }
                    if (r != 0) {
                        throw DiomedeError.databaseOpenError
                    }
                    self.dbi = dbi
                    return 0
                }
            } catch {
                return nil
            }
        }
        
        public func iterate(handler: (Data, Data) throws -> ()) throws {
            var key = MDB_val(mv_size: 0, mv_data: nil)
            var data = MDB_val(mv_size: 0, mv_data: nil)

            try env.read { (txn) -> Int in
                var cursor: OpaquePointer?
                guard (mdb_cursor_open(txn, dbi, &cursor) == 0) else {
                    throw DiomedeError.cursorOpenError
                }
                
                var op = MDB_FIRST
                while (mdb_cursor_get(cursor, &key, &data, op) == 0) {
                    op = MDB_NEXT
                    let keyData = Data(bytes: key.mv_data, count: key.mv_size)
                    let valueData = Data(bytes: data.mv_data, count: data.mv_size)
                    try handler(keyData, valueData)
                }
                mdb_cursor_close(cursor)
                return 0
            }
        }

        public func iterate(txn: OpaquePointer, handler: (Data, Data) throws -> ()) throws {
            var key = MDB_val(mv_size: 0, mv_data: nil)
            var data = MDB_val(mv_size: 0, mv_data: nil)
            
            var cursor: OpaquePointer?
            guard (mdb_cursor_open(txn, dbi, &cursor) == 0) else {
                throw DiomedeError.cursorOpenError
            }
            
            var op = MDB_FIRST
            while (mdb_cursor_get(cursor, &key, &data, op) == 0) {
                op = MDB_NEXT
                let keyData = Data(bytes: key.mv_data, count: key.mv_size)
                let valueData = Data(bytes: data.mv_data, count: data.mv_size)
                try handler(keyData, valueData)
            }
            mdb_cursor_close(cursor)
        }
        
        public func iterate(between lower: Data, and upper: Data, handler: (Data, Data) throws -> ()) throws {
            try lower.withUnsafeBytes { (lowerPtr) in
                try upper.withUnsafeBytes { (upperPtr) in
                    var key = MDB_val(mv_size: lower.count, mv_data: UnsafeMutableRawPointer(mutating: lowerPtr.baseAddress))
                    var data = MDB_val(mv_size: 0, mv_data: nil)

                    var upperBound = MDB_val(mv_size: upper.count, mv_data: UnsafeMutableRawPointer(mutating: upperPtr.baseAddress))

                    try env.read { (txn) -> Int in
                        var cursor: OpaquePointer?
                        guard (mdb_cursor_open(txn, dbi, &cursor) == 0) else {
                            throw DiomedeError.cursorOpenError
                        }
                        
                        var op = MDB_SET_RANGE
                        while (mdb_cursor_get(cursor, &key, &data, op) == 0) {
                            op = MDB_NEXT
                            let keyData = Data(bytes: key.mv_data, count: key.mv_size)
                            let valueData = Data(bytes: data.mv_data, count: data.mv_size)

                            let cmp = mdb_cmp(txn, dbi, &key, &upperBound)
                            if (cmp > 0) {
                                break
                            }
                            
                            try handler(keyData, valueData)
                        }
                        mdb_cursor_close(cursor)
                        return 0
                    }
                }
            }
        }
        
        public func count(txn: OpaquePointer) -> Int {
            var stat = MDB_stat(ms_psize: 0, ms_depth: 0, ms_branch_pages: 0, ms_leaf_pages: 0, ms_overflow_pages: 0, ms_entries: 0)
            mdb_stat(txn, dbi, &stat)
            return stat.ms_entries
        }
        
        public func iterate(txn: OpaquePointer, between lower: Data, and upper: Data, handler: (Data, Data) throws -> ()) throws {
            try lower.withUnsafeBytes { (lowerPtr) in
                try upper.withUnsafeBytes { (upperPtr) in
                    var key = MDB_val(mv_size: lower.count, mv_data: UnsafeMutableRawPointer(mutating: lowerPtr.baseAddress))
                    var data = MDB_val(mv_size: 0, mv_data: nil)
                    
                    var upperBound = MDB_val(mv_size: upper.count, mv_data: UnsafeMutableRawPointer(mutating: upperPtr.baseAddress))
                    
                    var cursor: OpaquePointer?
                    guard (mdb_cursor_open(txn, dbi, &cursor) == 0) else {
                        throw DiomedeError.cursorOpenError
                    }
                    
                    var op = MDB_SET_RANGE
                    while (mdb_cursor_get(cursor, &key, &data, op) == 0) {
                        op = MDB_NEXT
                        let keyData = Data(bytes: key.mv_data, count: key.mv_size)
                        let valueData = Data(bytes: data.mv_data, count: data.mv_size)
                        
                        let cmp = mdb_cmp(txn, dbi, &key, &upperBound)
                        if (cmp > 0) {
                            break
                        }
                        
                        try handler(keyData, valueData)
                    }
                    mdb_cursor_close(cursor)
                }
            }
        }

        public func contains(key k: DataEncodable) throws -> Bool {
            var exists = false
            try env.read { (txn) throws -> Int in
                let kData = try k.asData()
                try kData.withUnsafeBytes { (kPtr) throws in
                    var value = MDB_val(mv_size: 0, mv_data: nil)
                    var key = MDB_val(mv_size: kData.count, mv_data: UnsafeMutableRawPointer(mutating: kPtr.baseAddress))
                    let rc = mdb_get(txn, dbi, &key, &value)
                    if (rc == 0) {
                        exists = true
                    }
                }
                return 0
            }
            return exists
        }
        
        public func contains(txn: OpaquePointer, key k: DataEncodable) throws -> Bool {
            var exists = false
            let kData = try k.asData()
            try kData.withUnsafeBytes { (kPtr) throws in
                var value = MDB_val(mv_size: 0, mv_data: nil)
                var key = MDB_val(mv_size: kData.count, mv_data: UnsafeMutableRawPointer(mutating: kPtr.baseAddress))
                let rc = mdb_get(txn, dbi, &key, &value)
                if (rc == 0) {
                    exists = true
                }
            }
            return exists
        }
        
        public func get(key k: DataEncodable) throws -> Data? {
            var result: Data? = nil
            try env.read { (txn) throws -> Int in
                let kData = try k.asData()
                try kData.withUnsafeBytes { (kPtr) throws in
                    var value = MDB_val(mv_size: 0, mv_data: nil)
                    var key = MDB_val(mv_size: kData.count, mv_data: UnsafeMutableRawPointer(mutating: kPtr.baseAddress))
                    let rc = mdb_get(txn, dbi, &key, &value)
                    if (rc == MDB_NOTFOUND) {
                    } else if (rc != 0) {
                        throw DiomedeError.getError
                    } else {
                        result = Data(bytes: value.mv_data, count: value.mv_size)
                    }
                }
                return 0
            }
            return result
        }

        public func get(txn: OpaquePointer, key k: DataEncodable) throws -> Data? {
            var result: Data? = nil
            let kData = try k.asData()
            try kData.withUnsafeBytes { (kPtr) throws in
                var value = MDB_val(mv_size: 0, mv_data: nil)
                var key = MDB_val(mv_size: kData.count, mv_data: UnsafeMutableRawPointer(mutating: kPtr.baseAddress))
                let rc = mdb_get(txn, dbi, &key, &value)
                if (rc == MDB_NOTFOUND) {
                } else if (rc != 0) {
                    print("*** \(String(cString: mdb_strerror(rc)))")
                    print("*** \(String(cString: strerror(rc)))")
                    throw DiomedeError.getError
                } else {
                    result = Data(bytes: value.mv_data, count: value.mv_size)
                }
            }
            return result
        }
        
        public func insert<S, K: DataEncodable, V: DataEncodable>(uniqueKeysWithValues keysAndValues: S) throws where S : Sequence, S.Element == (K, V) {
            try env.write { (txn) throws -> Int in
                for (k, v) in keysAndValues {
                    let kData = try k.asData()
                    let vData = try v.asData()
                    try kData.withUnsafeBytes { (kPtr) throws in
                        try vData.withUnsafeBytes { (vPtr) throws in
                            var key = MDB_val(mv_size: kData.count, mv_data: UnsafeMutableRawPointer(mutating: kPtr.baseAddress))
                            var value = MDB_val(mv_size: vData.count, mv_data: UnsafeMutableRawPointer(mutating: vPtr.baseAddress))
                            let rc = mdb_put(txn, dbi, &key, &value, 0); // MDB_NOOVERWRITE
                            if (rc != 0) {
                                print("*** \(String(cString: mdb_strerror(rc)))")
                                throw DiomedeError.insertError
                            }
                        }
                    }
                }
                return 0
            }
        }
        
        public func insert<S, K: DataEncodable, V: DataEncodable>(txn: OpaquePointer, uniqueKeysWithValues keysAndValues: S) throws where S : Sequence, S.Element == (K, V) {
            for (k, v) in keysAndValues {
                let kData = try k.asData()
                let vData = try v.asData()
                try kData.withUnsafeBytes { (kPtr) throws in
                    try vData.withUnsafeBytes { (vPtr) throws in
                        var key = MDB_val(mv_size: kData.count, mv_data: UnsafeMutableRawPointer(mutating: kPtr.baseAddress))
                        var value = MDB_val(mv_size: vData.count, mv_data: UnsafeMutableRawPointer(mutating: vPtr.baseAddress))
                        let rc = mdb_put(txn, dbi, &key, &value, 0); // MDB_NOOVERWRITE
                        if (rc != 0) {
                            print("*** \(String(cString: mdb_strerror(rc)))")
                            throw DiomedeError.insertError
                        }
                    }
                }
            }
        }
    }

}
