//
//  DataEncoding.swift
//  Diomede
//
//  Created by Gregory Todd Williams on 5/25/20.
//

import Foundation

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

public struct QuadID: DataEncodable {
    public var a: UInt64
    public var b: UInt64
    public var c: UInt64
    public var d: UInt64

    public var values: [UInt64] {
        return [a, b, c, d]
    }
    
    public subscript(_ i: Int) -> UInt64 {
        switch i {
        case 0:
            return a
        case 1:
            return b
        case 2:
            return c
        default:
            return d
        }
    }
    
    public func asData() -> Data {
        let data = [a, b, c, d].map { $0.asData() }
        let combined = data.reduce(Data(), { $0 + $1 })
        return combined
    }
    public static func fromData(_ data: Data) throws -> Self {
        guard data.count == 32 else {
            throw DiomedeError.encodingError
        }
        
        let values = data.withUnsafeBytes { (p: UnsafePointer<UInt64>) -> [UInt64] in
            let bp = UnsafeBufferPointer(start: p, count: 4)
            let values = bp.map { UInt64(bigEndian: $0) }
            return values
        }
        
        let a = UInt64.fromData(data)
        let b = UInt64.fromData(data[8...])
        let c = UInt64.fromData(data[16...])
        let d = UInt64.fromData(data[24...])
        
        print("+ \(values)")
        print("- \([a,b,c,d])")
        
        return QuadID(a: a, b: b, c: c, d: d)
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

extension UInt64 : DataEncodable {
    public func asData() -> Data {
        var be = self.bigEndian
        return Data(bytes: &be, count: 8)
    }
    public static func fromData(_ data: Data) -> Self {
        var be: UInt64 = 0
        withUnsafeMutableBytes(of: &be) { bePtr in
            data.withUnsafeBytes {
                bePtr.baseAddress?.copyMemory(from: $0, byteCount: 8)
            }
        }

//        let be: Int64 = data.withUnsafeBytes { $0.load(as: Int64.self) }
        return UInt64(bigEndian: be)
    }
}

extension Int : DataEncodable {
    public func asData() -> Data {
        var be = Int64(self).bigEndian
        return Data(bytes: &be, count: 8)
    }
    public static func fromData(_ data: Data) -> Self {
        var be: Int64 = 0
        withUnsafeMutableBytes(of: &be) { bePtr in
            data.withUnsafeBytes {
                bePtr.baseAddress?.copyMemory(from: $0, byteCount: 8)
            }
        }

//        let be: Int64 = data.withUnsafeBytes { $0.load(as: Int64.self) }
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

