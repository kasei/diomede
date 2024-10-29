//
//  RDF-Extensions.swift
//  Diomede
//
//  Created by Gregory Todd Williams on 5/23/20.
//

import Foundation
#if os(macOS)
import Compression
#endif
import CryptoSwift
import SPARQLSyntax
import Diomede

extension Data {
    #if os(macOS)
    func uncompressed(size outputDataSize: Int) throws -> Data {
        let compressedSize = self.count
        let destinationBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: outputDataSize)

        var sourceBuffer = Array<UInt8>(repeating: 0, count: compressedSize)
        self.copyBytes(to: &sourceBuffer, count: compressedSize)

        let decodedSize = compression_decode_buffer(destinationBuffer,
                                                    outputDataSize,
                                                    &sourceBuffer,
                                                    compressedSize,
                                                    nil,
                                                    COMPRESSION_ZLIB)
        guard decodedSize == outputDataSize else {
            destinationBuffer.deallocate()
            throw DiomedeError.encodingError
        }
        return Data(bytesNoCopy: destinationBuffer, count: outputDataSize, deallocator: .free)
    }
    
    func compressed60() -> Data? {
        guard self.count > 100 else { return nil }
        let inputDataSize = self.count
        let byteSize = MemoryLayout<UInt8>.stride
        let bufferSize = inputDataSize / byteSize
        let destinationBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)

        var sourceBuffer = Array<UInt8>(repeating: 0, count: bufferSize)
        self.copyBytes(to: &sourceBuffer, count: inputDataSize)

        let compressedSize = compression_encode_buffer(destinationBuffer,
                                                       inputDataSize,
                                                       &sourceBuffer,
                                                       inputDataSize,
                                                       nil,
                                                       COMPRESSION_ZLIB)
        let ratio = Double(compressedSize) / Double(inputDataSize)
        if ratio < 0.60 {
//            print("compression: \(compressedSize) / \(inputDataSize) (\(100.0 * ratio)%)")
            return Data(bytesNoCopy: destinationBuffer, count: compressedSize, deallocator: .free)
        }
        return nil
    }
    #endif
    
    func uuid() throws -> UUID {
        guard self.count == 16 else {
            throw DiomedeError.encodingError
        }
        
        var uu : uuid_t = UUID().uuid
        Swift.withUnsafeMutableBytes(of: &uu) { (ptr) -> () in
            self.copyBytes(to: ptr, count: 16)
        }
        return UUID(uuid: uu)
    }
}

extension Term: @retroactive DataEncodable {
    public func sha256() throws -> Data {
        let d = try self.asData()
        let term_key = d.sha256()
        return term_key
    }

    public func asData() throws -> Data {
        let s: String
        switch self.type {
        case .blank:
            if let u = UUID(uuidString: self.value), self.value.count == 36 {
                var uu = u.uuid
                let du = Data(bytes: &uu, count: 16)
                
                let d = try "u".asData()
                return d + du
            }
            s = "B\"" + self.value
        case .iri:
            if self.value.count == 45 && self.value.hasPrefix("urn:uuid:") {
                let suffix = String(self.value.dropFirst(9))
                if let u = UUID(uuidString: suffix) {
                    var uu = u.uuid
                    let du = Data(bytes: &uu, count: 16)

                    let d = try "U".asData()
                    return d + du
                }
            }
            s = "I\"" + self.value
        case .language(let lang):
            s = "L\(lang)\"" + self.value
        case .datatype(let dt):
            switch dt {
            case .string:
                // the Compression framework is not cross-platform, so this isn't supported currently
//                if let inputData = self.value.data(using: .utf8), let cd = inputData.compressed60() {
//                    let d = "Z".data(using: .utf8)!
//                    let s = inputData.count.asData()
//                    return d + s + cd
//                }
                
                s = "S\"" + self.value
            case .integer:
                s = "i\"" + self.value
            default:
                s = "D\(dt.value)\"" + self.value
            }
        }
        return try s.asData()
    }
    
    public static func fromData(_ data: Data) throws -> Term {
        guard data.count >= 2 else {
            throw DiomedeError.encodingError
        }
        let c = String(UnicodeScalar(data[0]))
        switch c {
            // these are encodings which do not have a DOUBLE QUOTE followed by a string value
        case "U":
            let bytes = Data(data.dropFirst())
            let uu = try bytes.uuid()
            return Term(iri: "urn:uuid:\(uu.uuidString.lowercased())")
        case "u":
            let bytes = Data(data.dropFirst())
            let uu = try bytes.uuid()
            return Term(value: uu.uuidString.uppercased(), type: .blank)
        // the Compression framework is not cross-platform, so this isn't supported currently
//        case "Z":
//            let bytes = Data(data.dropFirst())
//            let size = Int.fromData(bytes)
//            let buffer = bytes.dropFirst(8)
//            let data = try buffer.uncompressed(size: size)
//            guard let string = String(data: data, encoding: .utf8) else {
//                throw DiomedeError.encodingError
//            }
//            return Term(string: string)
        default:
            break
        }
        
        let s = try String.fromData(data)
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
        case "S":
            return Term(value: value, type: .datatype(.string))
        case "i":
            return Term(value: value, type: .datatype(.integer))
        case "D":
            let dtvalue = String(s.dropFirst(1).prefix(while: { $0 != "\"" }))
            let dt = TermDataType(stringLiteral: dtvalue)
            return Term(value: value, type: .datatype(dt))
        default:
            throw DiomedeError.unknownError
        }
    }
}
