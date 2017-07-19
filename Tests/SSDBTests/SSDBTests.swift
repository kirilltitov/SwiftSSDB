import XCTest
@testable import SSDB

class SSDBTests: XCTestCase {
    func testExample() {
        let connection = SSDB(host: "127.0.0.1", port: 8888, password: "zuD6mgCusMf6qpSGJ6iKukU28ztIyjL1", keepAlive: true)
//        dump(String(data: (try! connection.get(key: "Character:6226657c-23cf-455d-aeb2-ab29eefd34c9"))!, encoding: .ascii))
        print(try! connection.get(key: "Character:6226657c-23cf-455d-aeb2-ab29eefd34c9", encoding: .isoLatin1))
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
