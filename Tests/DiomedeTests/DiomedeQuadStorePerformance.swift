import XCTest
import SPARQLSyntax
@testable import DiomedeQuadStore

#if os(Linux)
extension DiomedeQuadStorePerformanceTests {
    static var allTests : [(String, (DiomedeQuadStorePerformanceTests) -> () throws -> Void)] {
        return [
            ("testPerformance_emptyLoad_e3", testPerformance_emptyLoad_e3),
            ("testPerformance_emptyLoad_e4", testPerformance_emptyLoad_e4),
            ("testPerformance_nonEmptyLoad_e3", testPerformance_nonEmptyLoad_e3),
            ("testPerformance_nonEmptyLoad_e4", testPerformance_nonEmptyLoad_e4),
        ]
    }
}
#endif

enum TestError: Error {
    case missingTestData
    case diomedeError
}

class DiomedeQuadStorePerformanceTests: XCTestCase {
    var filemanager : FileManager { return FileManager.default }

    func quadGenerator(_ count: Int) -> AnySequence<Quad> {
        let graph = Term(value: "htp://example.org/graph", type: .iri)
        var j = 0
        let i = AnyIterator { () -> Quad? in
            if j >= count { return nil }
            j += 1
            let s = Term(iri: "http://example.org/s\(j)")
            let p = Term(iri: "http://example.org/p")
            let o = Term(string: "foo")
            let q = Quad(subject: s, predicate: p, object: o, graph: graph)
            return q
        }
        return AnySequence(i.makeIterator())
    }

    func tempStore(in dir: URL) throws -> DiomedeQuadStore {
        let filename = "diomede-test-\(UUID().uuidString).db"
        let path = dir.appendingPathComponent(filename)
//                print(path)
        guard let store = DiomedeQuadStore(path: path.path, create: true) else { throw TestError.diomedeError }
        return store
    }
    
    func testPerformance_emptyLoad_e3() throws {
        let count = 1_000
        let dir = filemanager.temporaryDirectory
        defer { try? filemanager.removeItem(at: dir) }
        let initialCount = 0
        self.measure {
            do {
                let store = try tempStore(in: dir)
                
                XCTAssertEqual(store.count, initialCount)
                
                let quads = quadGenerator(count)
                try store.load(version: 0, quads: quads)
                XCTAssertEqual(store.count, initialCount+count)
            } catch (_) {
                XCTFail()
            }
        }
    }
    
    func testPerformance_emptyLoad_e4() throws {
        let count = 10_000
        let dir = filemanager.temporaryDirectory
        defer { try? filemanager.removeItem(at: dir) }
        let initialCount = 0
        self.measure {
            do {
                let store = try tempStore(in: dir)
                
                XCTAssertEqual(store.count, initialCount)
                
                let quads = quadGenerator(count)
                try store.load(version: 0, quads: quads)
                XCTAssertEqual(store.count, initialCount+count)
            } catch (_) {
                XCTFail()
            }
        }
    }
    
    func testPerformance_nonEmptyLoad_e3() throws {
        let count = 1_000
        let dir = filemanager.temporaryDirectory
        defer { try? filemanager.removeItem(at: dir) }
        let iri = Term(iri: "http://example.org/x")
        let q = Quad(subject: iri, predicate: iri, object: Term.falseValue, graph: iri)
        let initialCount = 1
        self.measure {
            do {
                let store = try tempStore(in: dir)
                try store.load(version: 0, quads: [q])
                XCTAssertEqual(store.count, initialCount)
                
                let quads = quadGenerator(count)
                try store.load(version: 0, quads: quads)
                XCTAssertEqual(store.count, initialCount+count)
            } catch (_) {
                XCTFail()
            }
        }
    }
    
    func testPerformance_nonEmptyLoad_e4() throws {
        let count = 10_000
        let dir = filemanager.temporaryDirectory
        defer { try? filemanager.removeItem(at: dir) }
        let iri = Term(iri: "http://example.org/x")
        let q = Quad(subject: iri, predicate: iri, object: Term.falseValue, graph: iri)
        let initialCount = 1
        self.measure {
            do {
                let store = try tempStore(in: dir)
                try store.load(version: 0, quads: [q])
                XCTAssertEqual(store.count, initialCount)
                
                let quads = quadGenerator(count)
                try store.load(version: 0, quads: quads)
                XCTAssertEqual(store.count, initialCount+count)
            } catch (_) {
                XCTFail()
            }
        }
    }
}
