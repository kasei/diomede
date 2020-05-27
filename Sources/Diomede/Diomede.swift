import Foundation
import LMDB

public enum DiomedeError: Error {
    case unknownError
    case encodingError
    case databaseOpenError
    case cursorOpenError(Int32)
    case cursorError
    case insertError
    case getError
    case deleteError
    case indexError
    case nonExistentTermError
    case transactionError(Int32)
}

public class Environment {
    var env : OpaquePointer?
    
    public init?(path: String) {
        if (mdb_env_create(&env) != 0) {
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
            let txid = mdb_txn_id(txn)
            print("BEGIN \(txid)")
            if let txn = txn {
                do {
                    let r = try handler(txn)
                    if (r == 0) {
                        mdb_txn_commit(txn)
                        print("COMMIT \(txid)")
                    } else {
                        mdb_txn_abort(txn)
                        print("ROLLBACK \(txid)")
                    }
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
            print("databases() read")
            var dbi: MDB_dbi = 0
            let r = withUnsafeMutablePointer(to: &dbi) { (dbip) -> Int in
                if (mdb_dbi_open(txn, nil, 0, dbip) != 0) {
                    print("Failed to open database")
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
            let rc = mdb_cursor_open(txn, dbi, &cursor)
            guard (rc == 0) else {
                throw DiomedeError.cursorOpenError(rc)
            }
            defer { mdb_cursor_close(cursor) }

            var op = MDB_FIRST
            while (mdb_cursor_get(cursor, &key, &data, op) == 0) {
                op = MDB_NEXT
                let data = Data(bytes: key.mv_data, count: key.mv_size)
                if let name = String(data: data, encoding: .utf8) {
                    names.append(name)
                }
            }
            return 0
        }
        return names
    }
    
    private func createDatabase(named name: String) throws {
        try self.write { (txn) -> Int in
            var dbi: MDB_dbi = 0
            if (mdb_dbi_open(txn, name, UInt32(MDB_CREATE), &dbi) != 0) {
                throw DiomedeError.databaseOpenError
            }
            return 0
        }
    }
    
    public func createDatabase(txn: OpaquePointer, named name: String) throws {
        var dbi: MDB_dbi = 0
        if (mdb_dbi_open(txn, name, UInt32(MDB_CREATE), &dbi) != 0) {
            throw DiomedeError.databaseOpenError
        }
    }
    
    private func dropDatabase(named name: String) throws {
        guard let db = self.database(named: name) else {
            throw DiomedeError.databaseOpenError
        }
        try self.write { (txn) -> Int in
            let r =  mdb_drop(txn, db.dbi, 1)
            return Int(r)
        }
    }
    
    public func dropDatabase(txn: OpaquePointer, named name: String) throws {
        guard let db = self.database(named: name) else {
            throw DiomedeError.databaseOpenError
        }
        mdb_drop(txn, db.dbi, 1)
    }
    
    public func database(named name: String) -> Database? {
        let d = Database(environment: self, name: name)
        return d
    }
    
    public func database(txn: OpaquePointer, named name: String) -> Database? {
        let d = Database(txn: txn, environment: self, name: name)
        return d
    }

    public class Cursor: IteratorProtocol {
        public typealias Element = (OpaquePointer, Data, Data)
        var txn : OpaquePointer
        var cursor: OpaquePointer
        var key : MDB_val
        var data : MDB_val
        var upperBound : Data?
        var inclusive : Bool
        var rc: Int32

        init?(txn : OpaquePointer, cursor: OpaquePointer, lowerBound: DataEncodable? = nil, upperBound: DataEncodable? = nil, inclusive: Bool = false) {
            self.txn = txn
            self.cursor = cursor
            self.key = MDB_val(mv_size: 0, mv_data: nil)
            self.data = MDB_val(mv_size: 0, mv_data: nil)
            self.upperBound = nil
            self.inclusive = inclusive
            self.rc = 0
            
            var lower: Data? = nil
            do {
                self.upperBound = try upperBound?.asData()
                lower = try lowerBound?.asData()
                
                if let lower = lower {
                    
                    lower.withUnsafeBytes { (lowerPtr) in
                        self.key = MDB_val(mv_size: lower.count, mv_data: UnsafeMutableRawPointer(mutating: lowerPtr.baseAddress))
                    }
                    self.rc = mdb_cursor_get(cursor, &self.key, &self.data, MDB_SET_RANGE)
                } else {
                    self.rc = mdb_cursor_get(cursor, &self.key, &self.data, MDB_FIRST)
                }
            } catch {
                return nil
            }
        }

        deinit {
            let txid = mdb_txn_id(txn)
            print("COMMIT \(txid)")
            mdb_cursor_close(cursor)
            mdb_txn_commit(txn)
        }

        public func next() -> (OpaquePointer, Data, Data)? {
            guard self.rc == 0 else {
                return nil
            }
            let keyData = Data(bytes: key.mv_data, count: key.mv_size)
            let valueData = Data(bytes: data.mv_data, count: data.mv_size)
            let pair = (txn, keyData, valueData)
            defer {
                self.rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)
            }
            if let upper = self.upperBound {
                let stop = upper.withUnsafeBytes { (upperPtr) -> Bool in
                    var upperBound = MDB_val(mv_size: upper.count, mv_data: UnsafeMutableRawPointer(mutating: upperPtr.baseAddress))
                    let cmp = mdb_cmp(txn, mdb_cursor_dbi(self.cursor), &self.key, &upperBound)
                    if (cmp > 0) {
                        return true
                    }
                    return false
                }
                if stop {
                    return nil
                }
                return pair
            } else {
                return pair
            }
        }
    }
    
    public class Database: CustomStringConvertible {
        public typealias Iterator = AnyIterator<(Data, Data)>
        
        var env: Environment
        var dbi: MDB_dbi = 0
        var name: String

        public var description: String {
            return "Database(\(self.dbi))"
        }
        
        init?(txn: OpaquePointer, environment: Environment, name: String) {
            self.env = environment
            self.name = name
            do {
                var dbi: MDB_dbi = 0
                let r = withUnsafeMutablePointer(to: &dbi) { (dbip) -> Int in
                    let rc = mdb_dbi_open(txn, name, 0, dbip)
                    if (rc != 0) {
                        print("mdb_dbi_open returned [\(rc)]")
                        return 1
                    }
                    return 0
                }
                if (r != 0) {
                    print("*** databaseOpenError")
                    throw DiomedeError.databaseOpenError
                }
                //                    print("loaded dbi \(dbi)")
                self.dbi = dbi
            } catch {
                return nil
            }
            guard dbi != 0 else {
                return nil
            }
        }
        
        init?(environment: Environment, name: String) {
            self.env = environment
            self.name = name
            do {
                try env.read { (txn) throws -> Int in
                    var dbi: MDB_dbi = 0
                    let r = withUnsafeMutablePointer(to: &dbi) { (dbip) -> Int in
                        let rc = mdb_dbi_open(txn, name, 0, dbip)
                        if (rc != 0) {
                            print("mdb_dbi_open returned [\(rc)]")
                            return 1
                        }
                        return 0
                    }
                    if (r != 0) {
                        print("*** databaseOpenError")
                        throw DiomedeError.databaseOpenError
                    }
                    //                    print("loaded dbi \(dbi)")
                    self.dbi = dbi
                    return 0
                }
            } catch {
                return nil
            }
            guard dbi != 0 else {
                return nil
            }
        }
        
        public func iterator<T>(handler: @escaping (OpaquePointer, Data, Data) -> T) throws -> AnyIterator<T> {
            var _txn : OpaquePointer?
            var rc = mdb_txn_begin(self.env.env, nil, UInt32(MDB_RDONLY), &_txn)
            guard rc == 0 else {
                throw DiomedeError.transactionError(rc)
            }
            
            guard let txn = _txn else {
                throw DiomedeError.transactionError(0)
            }
            
            let txid = mdb_txn_id(txn)
            print("BEGIN \(txid)")

            var cursor: OpaquePointer?
            rc = mdb_cursor_open(txn, dbi, &cursor)
            guard (rc == 0) else {
                throw DiomedeError.cursorOpenError(rc)
            }
            
            if let cursor = cursor, let c = Cursor(txn: txn, cursor: cursor) {
                return AnyIterator {
                    guard let (txn, k, v) = c.next() else {
                        return nil
                    }
                    return handler(txn, k, v)
                }
            } else {
                return AnyIterator([].makeIterator())
            }
        }
        
        public func iterator<T>(between lower: Data, and upper: Data, inclusive: Bool = false, handler: @escaping (OpaquePointer, Data, Data) -> T) throws -> AnyIterator<T> {
            var _txn : OpaquePointer?
            var rc = mdb_txn_begin(self.env.env, nil, UInt32(MDB_RDONLY), &_txn)
            guard rc == 0 else {
                throw DiomedeError.transactionError(rc)
            }

            guard let txn = _txn else {
                throw DiomedeError.transactionError(0)
            }
            
            let txid = mdb_txn_id(txn)
            print("BEGIN \(txid)")
            
            var cursor: OpaquePointer?
            rc = mdb_cursor_open(txn, dbi, &cursor)
            guard (rc == 0) else {
                throw DiomedeError.cursorOpenError(rc)
            }
            
            if let cursor = cursor, let c = Cursor(txn: txn, cursor: cursor, lowerBound: lower, upperBound: upper, inclusive: inclusive) {
                return AnyIterator {
                    guard let (txn, k, v) = c.next() else {
                        return nil
                    }
                    return handler(txn, k, v)
                }
            } else {
                return AnyIterator([].makeIterator())
            }
        }
        
        public func iterate(handler: (Data, Data) throws -> ()) throws {
            let i = try self.iterator { ($1, $2) }
            let c = AnySequence { return i }
            for (k, v) in c {
                try handler(k, v)
            }
        }

        public func iterate(txn: OpaquePointer, handler: (Data, Data) throws -> ()) throws {
//            print("iterating on database \(name)")
            var key = MDB_val(mv_size: 0, mv_data: nil)
            var data = MDB_val(mv_size: 0, mv_data: nil)
            
            var cursor: OpaquePointer?
            var rc = mdb_cursor_open(txn, dbi, &cursor)
            guard (rc == 0) else {
                print("failed to open cursor on database \(dbi)")
                throw DiomedeError.cursorOpenError(rc)
            }
            defer { mdb_cursor_close(cursor) }

            rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST)
            if rc == 0 {
                repeat {
                    let keyData = Data(bytes: key.mv_data, count: key.mv_size)
                    let valueData = Data(bytes: data.mv_data, count: data.mv_size)
                    try handler(keyData, valueData)
                    rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)
                } while (rc == 0)
//                print("mdb_cursor_get: \(rc)")
            } else {
//                print("mdb_cursor_get: \(rc)")
            }
        }
        
        public func iterate(between lower: Data, and upper: Data, handler: (Data, Data) throws -> ()) throws {
            print("pipelined iteration")
            let i = try self.iterator(between: lower, and: upper, inclusive: false) { ($1, $2) }
            let c = AnySequence { return i }
            for (k, v) in c {
                try handler(k, v)
            }
        }

//        public func iterate(between lower: Data, and upper: Data, handler: (Data, Data) throws -> ()) throws {
////            print("iterating on database \(name)")
//            try lower.withUnsafeBytes { (lowerPtr) in
//                try upper.withUnsafeBytes { (upperPtr) in
//                    var key = MDB_val(mv_size: lower.count, mv_data: UnsafeMutableRawPointer(mutating: lowerPtr.baseAddress))
//                    var data = MDB_val(mv_size: 0, mv_data: nil)
//
//                    var upperBound = MDB_val(mv_size: upper.count, mv_data: UnsafeMutableRawPointer(mutating: upperPtr.baseAddress))
//
//                    try env.read { (txn) -> Int in
//                        var cursor: OpaquePointer?
//                        let rc = mdb_cursor_open(txn, dbi, &cursor)
//                        guard (rc == 0) else {
//                            throw DiomedeError.cursorOpenError(rc)
//                        }
//                        defer { mdb_cursor_close(cursor) }
//
//                        var op = MDB_SET_RANGE
//                        while (mdb_cursor_get(cursor, &key, &data, op) == 0) {
//                            op = MDB_NEXT
//                            let keyData = Data(bytes: key.mv_data, count: key.mv_size)
//                            let valueData = Data(bytes: data.mv_data, count: data.mv_size)
//
//                            let cmp = mdb_cmp(txn, dbi, &key, &upperBound)
//                            if (cmp > 0) {
//                                break
//                            }
//
//                            try handler(keyData, valueData)
//                        }
//                        return 0
//                    }
//                }
//            }
//        }
        
        public func count(txn: OpaquePointer) -> Int {
            var stat = MDB_stat(ms_psize: 0, ms_depth: 0, ms_branch_pages: 0, ms_leaf_pages: 0, ms_overflow_pages: 0, ms_entries: 0)
            mdb_stat(txn, dbi, &stat)
            return stat.ms_entries
        }
        
        public func count() throws -> Int {
            var count = 0
            try self.env.read { (txn) -> Int in
                var stat = MDB_stat(ms_psize: 0, ms_depth: 0, ms_branch_pages: 0, ms_leaf_pages: 0, ms_overflow_pages: 0, ms_entries: 0)
                mdb_stat(txn, dbi, &stat)
                count = stat.ms_entries
                return 0
            }
            return count
        }
        
        public func iterate(txn: OpaquePointer, between lower: Data, and upper: Data, inclusive: Bool, handler: (Data, Data) throws -> ()) throws {
            try lower.withUnsafeBytes { (lowerPtr) in
                try upper.withUnsafeBytes { (upperPtr) in
                    var key = MDB_val(mv_size: lower.count, mv_data: UnsafeMutableRawPointer(mutating: lowerPtr.baseAddress))
                    var data = MDB_val(mv_size: 0, mv_data: nil)
                    
                    var upperBound = MDB_val(mv_size: upper.count, mv_data: UnsafeMutableRawPointer(mutating: upperPtr.baseAddress))
                    
                    var cursor: OpaquePointer?
                    let rc = mdb_cursor_open(txn, dbi, &cursor)
                    guard (rc == 0) else {
                        throw DiomedeError.cursorOpenError(rc)
                    }
                    defer { mdb_cursor_close(cursor) }

                    var op = MDB_SET_RANGE
                    while (mdb_cursor_get(cursor, &key, &data, op) == 0) {
                        op = MDB_NEXT
                        let keyData = Data(bytes: key.mv_data, count: key.mv_size)
                        let valueData = Data(bytes: data.mv_data, count: data.mv_size)
                        
                        let cmp = mdb_cmp(txn, dbi, &key, &upperBound)
                        if inclusive {
                            if (cmp > 0) {
                                break
                            }
                            try handler(keyData, valueData)
                        } else {
                            if (cmp >= 0) {
                                break
                            }
                            try handler(keyData, valueData)
                        }
                        
                    }
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

        public func delete(key k: DataEncodable) throws {
            try env.write { (txn) throws -> Int in
                let kData = try k.asData()
                try kData.withUnsafeBytes { (kPtr) throws in
                    var value = MDB_val(mv_size: 0, mv_data: nil)
                    var key = MDB_val(mv_size: kData.count, mv_data: UnsafeMutableRawPointer(mutating: kPtr.baseAddress))
                    let rc = mdb_del(txn, dbi, &key, &value)
                    if (rc != 0) {
                        throw DiomedeError.deleteError
                    }
                }
                return 0
            }
        }
        
        public func delete(txn: OpaquePointer, key k: DataEncodable) throws {
            let kData = try k.asData()
            try kData.withUnsafeBytes { (kPtr) throws in
                var value = MDB_val(mv_size: 0, mv_data: nil)
                var key = MDB_val(mv_size: kData.count, mv_data: UnsafeMutableRawPointer(mutating: kPtr.baseAddress))
                let rc = mdb_del(txn, dbi, &key, &value)
                if (rc != 0) {
                    throw DiomedeError.deleteError
                }
            }
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
//                if name.count == 4 {
//                    if kData.count != 32 {
//                        print("Inserting data into quad index with bad length: \(kData.count): \(kData._hexValue)")
//                        assert(false)
//                    }
//                }
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
