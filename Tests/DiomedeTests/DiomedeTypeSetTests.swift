import XCTest
import SPARQLSyntax
@testable import DiomedeQuadStore

#if os(Linux)
extension DiomedeTypeSetTests {
    static var allTests : [(String, (DiomedeTypeSetTests) -> () throws -> Void)] {
        return [
            ("testTypeSets", testTypeSets),
        ]
    }
}
#endif

class DiomedeTypeSetTests: XCTestCase {
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
    
    func testTypeSets() throws {
        if let qs = store {
            let ex = TermNamespace(namespace: Namespace(value: "http://example.org/"))
            let rdf = TermNamespace(namespace: Namespace.rdf)
            let g = ex.graph

            let quads : [Quad] = [
                Quad(subject: ex.s1, predicate: ex.p1, object: Term.trueValue, graph: g),

                Quad(subject: ex.s2, predicate: rdf.type, object: ex.Type1, graph: g),
                Quad(subject: ex.s2, predicate: ex.p1, object: Term.trueValue, graph: g),

                Quad(subject: ex.s3a, predicate: rdf.type, object: ex.Type1, graph: g),
                Quad(subject: ex.s3a, predicate: ex.p2, object: Term.trueValue, graph: g),
                Quad(subject: ex.s3a, predicate: ex.p3, object: Term.trueValue, graph: g),

                Quad(subject: ex.s3b, predicate: rdf.type, object: ex.Type2, graph: g),
                Quad(subject: ex.s3b, predicate: ex.p2, object: Term.trueValue, graph: g),
                Quad(subject: ex.s3b, predicate: ex.p3, object: Term.trueValue, graph: g),
            ]
            try qs.load(version: 0, quads: quads)


            try qs.computeCharacteristicSets(withTypeSets: true)
            XCTAssertEqual(qs.count, 9)

            let cs = try qs.characteristicSets(for: g, includeTypeSets: true)
            
            XCTAssertEqual(cs.sets.count, 3)
            
            let acs1 = try cs.aggregatedCharacteristicSet(matching: bgp(for: [ex.p1]), in: g, store: qs)
            XCTAssertEqual(acs1.count, 2)
            XCTAssertEqual(CharacteristicSet(acs1, from: qs).types, [Set([ex.Type1]): 1])
            XCTAssertEqual(acs1.predicates.count, 1) // [ex.p1]

            let acs2 = try cs.aggregatedCharacteristicSet(matching: bgp(for: [ex.p1, rdf.type]), in: g, store: qs)
            XCTAssertEqual(acs2.count, 1)
            XCTAssertEqual(CharacteristicSet(acs2, from: qs).types, [Set([ex.Type1]): 1])
            XCTAssertEqual(acs2.predicates.count, 2) // [ex.p1, rdf.type]

            let acs3 = try cs.aggregatedCharacteristicSet(matching: bgp(for: [rdf.type]), in: g, store: qs)
            XCTAssertEqual(acs3.count, 3)
            XCTAssertEqual(CharacteristicSet(acs3, from: qs).types, [Set([ex.Type1]): 2, Set([ex.Type2]): 1])
            XCTAssertEqual(acs3.predicates.count, 1) // [rdf.type]
        }
    }
    
    private func bgp(for preds: [Term]) -> [TriplePattern] {
        let preds = preds.map { TriplePattern(subject: .variable("s", binding: true), predicate: .bound($0), object: .variable("o", binding: true)) }
        return preds
    }
}
