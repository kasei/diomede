//
//  RDF-Extensions.swift
//  Diomede
//
//  Created by Gregory Todd Williams on 5/23/20.
//

import Foundation
import SPARQLSyntax
import Diomede

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
            switch dt {
            case .string:
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
        case "S":
            return Term(value: value, type: .datatype(.string))
        case "i":
            return Term(value: value, type: .datatype(.integer))
        case "D":
            let dt = String(s.dropFirst(1).prefix(while: { $0 != "\"" }))
            return Term(value: value, type: .datatype(.custom(dt)))
        default:
            throw DiomedeError.unknownError
        }
    }
}
