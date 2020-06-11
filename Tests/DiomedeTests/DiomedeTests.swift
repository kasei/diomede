    import XCTest
    import SPARQLSyntax
    @testable import DiomedeQuadStore

    final class DiomedeQuadStoreTests: XCTestCase {
        var filename: URL!
        var graph: Term!
        var store: DiomedeQuadStore!
        
        override func setUp() {
            super.setUp()
            let f = FileManager.default
            let dir = f.temporaryDirectory
            let filename = "kineo-test-\(UUID().uuidString).db"
            print(filename)
            let path = dir.appendingPathComponent(filename)
            self.filename = path
            self.store = DiomedeQuadStore(path: self.filename.path, create: true)
            self.graph = Term(value: "http://example.org/", type: .iri)
            //            try? self.store.load(version: 1, quads: testQuads)
        }
        
        override func tearDown() {
            super.tearDown()
            let f = FileManager.default
            try? f.trashItem(at: self.filename, resultingItemURL: nil)
        }
        
        func testSimpleLoadQuery() throws {
            if let qs = store {
                try qs.load(version: 0, quads: [
                    Quad(subject: Term(iri: "s"), predicate: Term(iri: "p1"), object: Term(string: "o"), graph: Term(iri: "tag:graph")),
                    Quad(subject: Term(iri: "s"), predicate: Term(iri: "p2"), object: Term(integer: 7), graph: Term(iri: "tag:graph")),
                ])
                XCTAssertEqual(qs.count, 2)

                let qp = QuadPattern.all
                let matchingQuads = try Array(qs.quads(matching: qp))
                XCTAssertEqual(qs.count, 2)
                for q in matchingQuads {
                    XCTAssertEqual(q.subject, Term(iri: "s"))
                    let p = q.predicate
                    switch p.value {
                    case "p1":
                        XCTAssertEqual(q.object, Term(string: "o"))
                    case "p2":
                        XCTAssertEqual(q.object, Term(integer: 7))
                    default:
                        XCTFail()
                    }
                }
            }
        }
        
        func testDuplicateLoad() throws {
            if let qs = store {
                let q = Quad(subject: Term(iri: "s"), predicate: Term(iri: "p1"), object: Term(string: "o"), graph: Term(iri: "tag:graph"))
                try qs.load(version: 0, quads: [q, q])
                XCTAssertEqual(qs.count, 1)
            }
        }

        func testRepeatedDuplicateLoad() throws {
            if let qs = store {
                let q = Quad(subject: Term(iri: "s"), predicate: Term(iri: "p1"), object: Term(string: "o"), graph: Term(iri: "tag:graph"))
                try qs.load(version: 0, quads: [q, q])
                try qs.load(version: 0, quads: [q, q])
                XCTAssertEqual(qs.count, 1)
            }
        }

        static var allTests = [
            ("testSimpleLoadQuery", testSimpleLoadQuery),
            ("testDuplicateLoad", testDuplicateLoad),
            ("testRepeatedDuplicateLoad", testRepeatedDuplicateLoad),
        ]
    }
