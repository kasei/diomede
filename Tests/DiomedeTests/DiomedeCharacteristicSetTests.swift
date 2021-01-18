import XCTest
import SPARQLSyntax
@testable import DiomedeQuadStore

#if os(Linux)
extension DiomedeCharacteristicSetTests {
    static var allTests : [(String, (DiomedeCharacteristicSetTests) -> () throws -> Void)] {
        return [
            ("testCSStarCardinality", testCSStarCardinality),
        ]
    }
}
#endif

class DiomedeCharacteristicSetTests: XCTestCase {
    var filename: URL!
    var graph: Term!
    var store: DiomedeQuadStore!
    var subjectBase: Int = 0
    
    override func setUp() {
        super.setUp()
        let f = FileManager.default
        let dir = f.temporaryDirectory
        let filename = "kineo-test-\(UUID().uuidString).db"
        print(filename)
        let path = dir.appendingPathComponent(filename)
        self.filename = path
        self.store = DiomedeQuadStore(path: self.filename.path, create: true)
    }
    
    override func tearDown() {
        super.tearDown()
        #if os(macOS)
        let f = FileManager.default
        try? f.trashItem(at: self.filename, resultingItemURL: nil)
        #endif
    }
    
    func generateSimpleStarQuads(subjectCount: Int, predicates: Set<String>, graph: Term) -> [Quad] {
        var count = 0
        var quads = [Quad]()
        for _ in 0..<subjectCount {
            let sid = subjectBase
            subjectBase += 1
            let s = Term(iri: "http://example.org/s\(sid)")
            for plocal in predicates {
                let p = Term(iri: "http://example.org/\(plocal)")
                count += 1
                let o = Term(integer: sid)
                let q = Quad(subject: s, predicate: p, object: o, graph: graph)
                quads.append(q)
            }
        }
        return quads
    }
    
    func generateStarQuads(subjectCount: Int, predicates: [String: Int], graph: Term) -> [Quad] {
        var count = 0
        var quads = [Quad]()
        for _ in 0..<subjectCount {
            let sid = subjectBase
            subjectBase += 1
            let s = Term(iri: "http://example.org/s\(sid)")
            for (plocal, cardinality) in predicates {
                let p = Term(iri: "http://example.org/\(plocal)")
                count += 1
                for x in 0..<cardinality {
                    let o = Term(integer: sid+x)
                    let q = Quad(subject: s, predicate: p, object: o, graph: graph)
                    quads.append(q)
                }
            }
        }
        return quads
    }
    
    func testCSStarCardinality() throws {
        if let qs = store {
            let graph = Term(iri: "http://example.org/graph")
            
            try qs.load(version: 0, quads: generateSimpleStarQuads(subjectCount: 10, predicates: ["type"], graph: graph))
            try qs.load(version: 0, quads: generateSimpleStarQuads(subjectCount: 5, predicates: ["type", "name"], graph: graph))
            try qs.load(version: 0, quads: generateSimpleStarQuads(subjectCount: 5, predicates: ["type", "version"], graph: graph))
            try qs.load(version: 0, quads: generateSimpleStarQuads(subjectCount: 5, predicates: ["type", "name", "version"], graph: graph))
            try qs.computeCharacteristicSets()
            XCTAssertEqual(qs.count, 45)

            let csDataset = try qs.characteristicSets(for: graph)
            
            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .variable("p", binding: true), object: .variable("o", binding: true))
            ], in: graph, store: qs), 45)

            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/type")), object: .variable("o", binding: true))
            ], in: graph, store: qs), 25)

            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/name")), object: .variable("o", binding: true))
            ], in: graph, store: qs), 10)

            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/type")), object: .variable("o1", binding: true)),
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/name")), object: .variable("o2", binding: true))
            ], in: graph, store: qs), 10)

            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/type")), object: .variable("o1", binding: true)),
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/name")), object: .variable("o2", binding: true)),
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/version")), object: .variable("o3", binding: true))
            ], in: graph, store: qs), 5)
        }
    }
    
    func testCSMultiObjectStarCardinality() throws {
        if let qs = store {
            let graph = Term(iri: "http://example.org/graph")
            
            try qs.load(version: 0, quads: generateSimpleStarQuads(subjectCount: 10, predicates: ["type"], graph: graph))
            try qs.load(version: 0, quads: generateStarQuads(subjectCount: 5, predicates: ["type": 1, "name": 2], graph: graph))
            try qs.computeCharacteristicSets()
            XCTAssertEqual(qs.count, 25)

            let csDataset = try qs.characteristicSets(for: graph)
            
            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .variable("p", binding: true), object: .variable("o", binding: true))
            ], in: graph, store: qs), 25)

            // this is the same as COUNT(?s) WHERE { ?s ex:type ?o }
            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/type")), object: .variable("o", binding: true))
            ], in: graph, store: qs), 15)

            // this is the same as COUNT(?s) WHERE { ?s ex:name ?o }
            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/name")), object: .variable("o", binding: true))
            ], in: graph, store: qs), 10)

            // this is the same as COUNT(*) WHERE { ?s ex:type ?type ; ex:name ?name }
            XCTAssertEqual(try csDataset.starCardinality(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/type")), object: .variable("o1", binding: true)),
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/name")), object: .variable("o2", binding: true))
            ], in: graph, store: qs), 10)
            
            
            // this is the same as COUNT(DISTINCT ?s) WHERE { ?s ex:type ?o }
            XCTAssertEqual(try csDataset.aggregatedCharacteristicSet(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/type")), object: .variable("o", binding: true))
            ], in: graph, store: qs).count, 15)

            // this is the same as COUNT(DISTINCT ?s) WHERE { ?s ex:name ?o }
            XCTAssertEqual(try csDataset.aggregatedCharacteristicSet(matching: [
                TriplePattern(subject: .variable("s", binding: true), predicate: .bound(Term(iri: "http://example.org/name")), object: .variable("o", binding: true))
            ], in: graph, store: qs).count, 5)

        }
    }
}
