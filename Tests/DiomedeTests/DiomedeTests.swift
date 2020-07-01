import XCTest
import SPARQLSyntax
@testable import DiomedeQuadStore

#if os(Linux)
extension DiomedeQuadStoreTests {
    static var allTests : [(String, (DiomedeQuadStoreTests) -> () throws -> Void)] {
        return [
            ("testSimpleLoadQuery", testSimpleLoadQuery),
            ("testDuplicateLoad", testDuplicateLoad),
            ("testRepeatedDuplicateLoad", testRepeatedDuplicateLoad),
            ("testCountQuads", testCountQuads),
            ("testUUIDTerm_toData", testUUIDTerm_toData),
            ("testUUIDTerm_fromData", testUUIDTerm_fromData),
            ("testBlankTerm_toData", testBlankTerm_toData),
            ("testBlankTerm_fromData", testBlankTerm_fromData),
        ]
    }
}
#endif

class DiomedeQuadStoreTests: XCTestCase {
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
        #if os(macOS)
        let f = FileManager.default
        try? f.trashItem(at: self.filename, resultingItemURL: nil)
        #endif
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
    
    func testCountQuads() throws {
        if let qs = store {
            try qs.load(version: 0, quads: [
                Quad(subject: Term(iri: "s"), predicate: Term(iri: "p1"), object: Term(string: "o"), graph: Term(iri: "tag:graph")),
                Quad(subject: Term(iri: "s"), predicate: Term(iri: "p2"), object: Term(integer: 7), graph: Term(iri: "tag:graph")),
                Quad(subject: Term(iri: "s"), predicate: Term(iri: "p3"), object: Term(iri: "s"), graph: Term(iri: "tag:graph")),
                Quad(subject: Term(iri: "s"), predicate: Term(iri: "tag:graph"), object: Term(iri: "s"), graph: Term(iri: "tag:graph")),
            ])
            XCTAssertEqual(qs.count, 4)
            
            let qp1 = QuadPattern.all
            try XCTAssertEqual(qs.countQuads(matching: qp1), 4)
            
            let sVar = Node.variable("s", binding: true)
            let pVar = Node.variable("p", binding: true)
            let oVar = Node.variable("o", binding: true)
            let graph = Node.bound(Term(iri: "tag:graph"))
            
            let qp2 = QuadPattern(subject: sVar, predicate: pVar, object: oVar, graph: graph)
            try XCTAssertEqual(qs.countQuads(matching: qp2), 4)
            
            // repeated variable ?s in both subject and object positions
            let qp3 = QuadPattern(subject: sVar, predicate: pVar, object: sVar, graph: graph)
            try XCTAssertEqual(qs.countQuads(matching: qp3), 2)
            
            // repeated variable ?s in subject, predicate, and object positions
            let qp4 = QuadPattern(subject: sVar, predicate: sVar, object: sVar, graph: graph)
            try XCTAssertEqual(qs.countQuads(matching: qp4), 0)
            
            // repeated variable ?s in subject, and object positions, and ?g in predicate, and graph positions
            let qp5 = QuadPattern(subject: sVar, predicate: pVar, object: sVar, graph: pVar)
            try XCTAssertEqual(qs.countQuads(matching: qp5), 1)
        }
    }
    
    func testUUIDTerm_toData() throws {
        let t = Term(iri: "urn:uuid:08b7a198-7eaf-4a6a-b0f4-258cb7e299fe")
        let d = try t.asData()
        let bytes : [UInt8] = [0x55, 0x08, 0xb7, 0xa1, 0x98, 0x7e, 0xaf, 0x4a, 0x6a, 0xb0, 0xf4, 0x25, 0x8c, 0xb7, 0xe2, 0x99, 0xfe]
        let expected = Data(bytes: bytes, count: 17)
        XCTAssertEqual(d, expected)
    }
    
    func testUUIDTerm_fromData() throws {
        let bytes : [UInt8] = [0x55, 0x08, 0xb7, 0xa1, 0x98, 0x7e, 0xaf, 0x4a, 0x6a, 0xb0, 0xf4, 0x25, 0x8c, 0xb7, 0xe2, 0x99, 0xfe]
        let data = Data(bytes: bytes, count: 17)
        let term = try Term.fromData(data)
        let expected = Term(iri: "urn:uuid:08b7a198-7eaf-4a6a-b0f4-258cb7e299fe")
        XCTAssertEqual(term, expected)
    }

    func testBlankTerm_toData() throws {
        let t = Term(value: "08B7A198-7EAF-4A6A-B0F4-258CB7E299FE", type: .blank)
        let d = try t.asData()
        let bytes : [UInt8] = [0x75, 0x08, 0xb7, 0xa1, 0x98, 0x7e, 0xaf, 0x4a, 0x6a, 0xb0, 0xf4, 0x25, 0x8c, 0xb7, 0xe2, 0x99, 0xfe]
        print(d._hexValue)
        let expected = Data(bytes: bytes, count: 17)
        XCTAssertEqual(d, expected)
    }
    
    func testBlankTerm_fromData() throws {
        let bytes : [UInt8] = [0x75, 0x08, 0xb7, 0xa1, 0x98, 0x7e, 0xaf, 0x4a, 0x6a, 0xb0, 0xf4, 0x25, 0x8c, 0xb7, 0xe2, 0x99, 0xfe]
        let data = Data(bytes: bytes, count: 17)
        let term = try Term.fromData(data)
        let expected = Term(value: "08B7A198-7EAF-4A6A-B0F4-258CB7E299FE", type: .blank)
        XCTAssertEqual(term, expected)
    }

    func testDropGraph() throws {
        if let qs = store {
            for g in ["tag:graph1", "tag:graph2"] {
                let graph = Term(iri: g)
                let q = Quad(subject: Term(iri: "s"), predicate: Term(iri: "p1"), object: Term(string: "o"), graph: graph)
                try qs.load(version: 0, quads: [q])
            }
            XCTAssertEqual(qs.count, 2)
            let graphsPre = Set(qs.graphs().map { $0.value })
            XCTAssertEqual(graphsPre, ["tag:graph1", "tag:graph2"])
            
            let g1 = Term(iri: "tag:graph1")
            try qs.drop(graph: g1)

            XCTAssertEqual(qs.count, 1)
            let graphsPost = Set(qs.graphs().map { $0.value })
            XCTAssertEqual(graphsPost, ["tag:graph2"])
            
            XCTAssertNoThrow(try qs.verify())
        }
    }
    
    
    // the Compression framework is not cross-platform, so this isn't supported currently
//    func testLargeLiteralEncoding() throws {
//        let string = "This was primarily a book sale (2005 lots), although many lots of prints and curiosa as well as some drawings were also included.  Two owners are named on the title page of the catalogue, and it is uncertain which of the two is supposed to have owned the paintings.  Mr. Pieter de Malapert (1740-1806) lived in a house called Plettenberg at Jutphaas near Utrecht, a possession of the Malapert family since the late sixteenth century, after inheriting it from his father, Louis de Malapert, in 1782. He had a law degree from Utrecht University and was canon at Utrecht cathedral. At his death his estate was estimated to be worth fl. 99,286:7:4. He seems to have been active primarily as a collector of naturalia, the bulk of which was described in the Catalogus musei Malaperttiani published in 1806. His heirs elected to sell Plettenberg, so the contents were removed, probably by his only brother and executor, Jan Jacob de Malapert (1743-1816), and given to the Utrecht booksellers Bartholomeus Wild and Johannes Altheer to sell. The other owner can be identified as the late Wolfert Beeldsnijder, an iron merchant and alderman of Ijsselstein who died in 1806.  The paintings consisted mostly of landscapes and genre scenes, but the descriptions are too brief to allow them to be identified. (B. Fredericksen)"
//        let t = Term(string: string)
//        let data = try t.asData()
//        XCTAssertEqual(String(UnicodeScalar(data[0])), "Z")
//        XCTAssertLessThan(data.count, string.count)
//        
//        let term = try Term.fromData(data)
//        XCTAssertEqual(term, Term(string: string))
//        
//    }
}
