    import XCTest
    @testable import SwiftLMDB

    final class SwiftLMDBTests: XCTestCase {
        func testExample() {
            let e = LMDBEnvironment(path: "/Users/greg/Desktop/lmdb-test/test-db")
            print(e)
        }

        static var allTests = [
            ("testExample", testExample),
        ]
    }
