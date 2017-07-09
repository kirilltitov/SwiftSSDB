import Foundation
import Socks

open class SSDB {
    typealias Bytes = [UInt8]

    enum E: Error {
        case ConnectError(String)
        case AuthError
        case NoAuthError
        case SendError(String)
        case ResponseError(String)
        case CommandError(String)
    }

    enum Command {
        case Auth(password: String)
        case Set(key: String, value: Data)
        case Get(key: String)
        case Delete(key: String)
        case HashSet(name: String, key: String, value: Data)
        case HashGet(name: String, key: String)
        case HashDelete(name: String, key: String)
        case Increment(key: String)

        func getCompiledPacket() -> Data {
            var result = Data()
            switch self {
            case .Auth(let password):
                result = Command.compile(blocks: ["auth", password])
            case .Set(let key, let value):
                result = Command.compile(
                    blocks: [
                        "set".data(using: .utf8)!,
                        key.data(using: .utf8)!,
                        value
                    ]
                )
            case .Get(let key):
                result = Command.compile(blocks: ["get", key])
            case .Delete(let key):
                result = Command.compile(blocks: ["del", key])
            case .HashSet(let name, let key, let value):
                result = Command.compile(
                    blocks: [
                        "hset".data(using: .utf8)!,
                        name.data(using: .utf8)!,
                        key.data(using: .utf8)!,
                        value
                    ]
                )
            case .HashGet(let name, let key):
                result = Command.compile(blocks: ["hget", name, key])
            case .HashDelete(let name, let key):
                result = Command.compile(blocks: ["hdel", name, key])
            case .Increment(let key):
                result = Command.compile(blocks: ["incr", key])
            }
            return result
        }

        func getDescription() -> String {
            var result = ""
            switch self {
            case .Auth(let password):
                result = "auth\n\(password)"
            case .Set(let key, let value):
                result = "set\n\(key)\n\(value)"
            case .Get(let key):
                result = "get\n\(key)"
            case .Delete(let key):
                result = "del\n\(key)"
            default:
                break
            }
            return result
        }

        func isAuth() -> Bool {
            switch self {
            case .Auth:
                return true
            default:
                return false
            }
        }

        static func compile(blocks: [String]) -> Data {
            return Command.compile(blocks: blocks.map { $0.data(using: .utf8)! })
        }

        static func compile(blocks: [Data]) -> Data {
            var result = Data()
            let newLine = "\n".data(using: .utf8)!
            for block in blocks {
                result.append("\(block.count)\n".data(using: .utf8)!)
                result.append(block)
                result.append(newLine)
            }
            result.append(newLine)
            return result
        }
    }

    struct Response {
        static let NEW_LINE: UInt8 = 10

        var status: String
        var details: [Data]
        var isOK: Bool {
            return self.status == "ok"
        }

        init(status: String, details: [Data]) {
            self.status = status
            self.details = details
        }

        init(_ data: Data) throws {
            let bytes = Bytes(data)
            guard bytes.count > 0 else {
                throw E.ResponseError("Empty response")
            }
            guard bytes[bytes.count - 1] == Response.NEW_LINE, bytes[bytes.count - 2] == Response.NEW_LINE else {
                throw E.ResponseError("Packet must end with two new lines (recieved bytes: \"\(bytes)\")")
            }
            var result: [Data] = []
            var data = bytes
            while data.count != 1 && data[0] != Response.NEW_LINE {
                guard let end = data.index(of: Response.NEW_LINE) else {
                    break
                }
                guard
                    let blockSizeString = try? data.prefix(upTo: end).toString(),
                    let blockSize = Int(blockSizeString)
                else {
                    throw E.ResponseError("Could not parse size (recieved bytes: \"\(bytes)\")")
                }
                var valueBytes: Bytes = []
                let valueEnd: Int = end + blockSize
                guard data[valueEnd + 1] == Response.NEW_LINE else {
                    throw E.ResponseError("No newline at block end, invalid block format (recieved: \"\(bytes)\")")
                }
                for i in (end + 1)...valueEnd {
                    valueBytes.append(data[i])
                }
                result.append(Data(bytes:valueBytes))
                if valueEnd + 2 == Int(Response.NEW_LINE) {
                    break
                }
                data = Array(data.suffix(from: valueEnd + 2))
            }
            guard result.count > 0 else {
                throw E.ResponseError("Could not find status in response")
            }
            guard let status = String(bytes: result.removeFirst(), encoding: .utf8) else {
                throw E.ResponseError("Could not find status in response \"\(result)\"")
            }
            self.init(status: status, details: result)
        }
    }

    let host: String
    let port: UInt16
    let password: String?
    let keepAlive: Bool
    var connection: TCPClient? = nil

    init(
        host: String,
        port: UInt16,
        password: String? = nil,
        keepAlive: Bool = true
    ) {
        self.host = host
        self.port = port
        self.password = password
        self.keepAlive = keepAlive
    }

    deinit {
        try? self.connection?.close()
    }

    @discardableResult private func connect() throws -> TCPClient {
        if self.connection != nil {
            return self.connection!
        }
        do {
            self.connection = try TCPClient(
                address: InternetAddress(
                    hostname: self.host,
                    port: self.port
                ),
                connectionTimeout: 2
            )
            self.connection?.socket.keepAlive = self.keepAlive
            if let password = self.password {
                guard try self.send(command: .Auth(password: password), to: self.connection!).isOK else {
                    throw E.AuthError
                }
            }
        } catch let error {
            throw E.ConnectError("Could not connect to \(self.host):\(self.port) \(error)")
        }
        return self.connection!
    }

    @discardableResult func send(
        command: Data,
        to connection: TCPClient? = nil,
        description: String? = nil,
        isAuth: Bool = false
    ) throws -> Response {
        let connection = try connection ?? self.connect()
        let description = description ?? String(data: command, encoding: .isoLatin1)!
        do {
            try connection.send(bytes: Bytes(command))
        } catch let error {
            throw E.SendError("Could not send command \"\(description)\" (error \"\(error)\")")
        }
        do {
            let response = try Response(Data(bytes: try connection.receiveAll()))
            if !isAuth && response.status == "noauth" {
                throw E.NoAuthError
            }
            return response
        } catch let error {
            throw E.ResponseError("Could not receive result for command \"\(description)\" (error \"\(error)\")")
        }
    }

    @discardableResult func send(command: Command, to connection: TCPClient? = nil) throws -> Response {
        return try self.send(
            command: command.getCompiledPacket(),
            to: try connection ?? self.connect(),
            description: command.getDescription(),
            isAuth: command.isAuth()
        )
    }

    func set(key: String, value: Data) throws {
        try self.connect()
        let response = try self.send(command: .Set(key: key, value: value))
        guard response.isOK else {
            throw E.CommandError("Could not set value by key \"\(key)\" (response: \(response))")
        }
    }

    func get(key: String) throws -> Data? {
        try self.connect()
        return try self.send(command: .Get(key: key)).details.first
    }

    func delete(key: String) throws {
        try self.connect()
        try self.send(command: .Delete(key: key))
    }

    func hashSet(name: String, key: String, value: Data) throws {
        try self.connect()
        let response = try self.send(command: .HashSet(name: name, key: key, value: value))
        guard response.isOK else {
            throw E.CommandError("Could not set has value by name \(name) and key \"\(key)\" (response: \(response))")
        }
    }

    func hashGet(name: String, key: String) throws -> Data? {
        try self.connect()
        return try self.send(command: .HashGet(name: name, key: key)).details.first
    }

    func hashDelete(name: String, key: String) throws {
        try self.connect()
        try self.send(command: .HashDelete(name: name, key: key))
    }

    func increment(key: String) throws -> Int {
        try self.connect()
        let response = try self.send(command: .Increment(key: key))
        guard let result = response.details.first else {
            throw E.CommandError("Could not increment key \"\(key)\" (response: \(response)")
        }
        return Int(String(data: result, encoding: .utf8)!)!
    }
}
