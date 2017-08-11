import XCTest
@testable import SSDB

class SSDBTests: XCTestCase {
    func testExample() {
        do {
            let connection = SSDB(host: "127.0.0.1", port: 8888)
            dump(try connection.info())
        } catch let error {
            dump(error)
            XCTFail()
        }
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
