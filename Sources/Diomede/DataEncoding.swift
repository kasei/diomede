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

public struct QuadID: Hashable, Comparable, DataEncodable {
    public var a: UInt64
    public var b: UInt64
    public var c: UInt64
    public var d: UInt64

    public init(_ a: UInt64, _ b: UInt64, _ c: UInt64, _ d: UInt64) {
        self.a = a
        self.b = b
        self.c = c
        self.d = d
    }
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
        
        // load 4 big-endian UInt64 values from the bytes of the Data object
        let size = MemoryLayout<UInt64>.size
        let values = data.withUnsafeBytes { (bp: UnsafeRawBufferPointer) -> [UInt64] in
            return stride(from: 0, to: size*4, by: size)
                .map { bp.load(fromByteOffset: $0, as: UInt64.self) }
                .map { UInt64(bigEndian: $0) }
        }
        return QuadID(values[0], values[1], values[2], values[3])
    }

    public static func < (lhs: QuadID, rhs: QuadID) -> Bool {
        return lhs.values.lexicographicallyPrecedes(rhs.values)
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
        let be = data.withUnsafeBytes { (bp: UnsafeRawBufferPointer) -> UInt64 in
            return UInt64(bigEndian: bp.load(as: UInt64.self))
        }
        return be
    }
}

extension Int : DataEncodable {
    public func asData() -> Data {
        var be = Int64(self).bigEndian
        return Data(bytes: &be, count: 8)
    }
    public static func fromData(_ data: Data) -> Self {
        let be = data.withUnsafeBytes { (bp: UnsafeRawBufferPointer) -> Int64 in
            return Int64(bigEndian: bp.load(as: Int64.self))
        }
        return Int(be)
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

