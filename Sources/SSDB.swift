import Foundation
import Socket

open class SSDB {
    public typealias Bytes = [UInt8]

    public enum E: Error {
        case SocketError(String)
        case ConnectError(String)
        case AuthError
        case NoAuthError
        case SendError(String)
        case ResponseError(String)
        case CommandError(String)
    }

    public enum Command {
        case Auth(password: String)
        case Set(key: String, value: Data)
        case Get(key: String)
        case Delete(key: String)
        case HashSet(name: String, key: String, value: Data)
        case HashGet(name: String, key: String)
        case HashDelete(name: String, key: String)
        case Increment(key: String)
        case Info

        public func getCompiledPacket() -> Data {
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
            case .Info:
                result = Command.compile(blocks: ["info"])
            }
            return result
        }

        public func getDescription() -> String {
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
            case .Info:
                result = "info"
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

        public static func compile(blocks: [String]) -> Data {
            return Command.compile(blocks: blocks.map { $0.data(using: .utf8)! })
        }

        public static func compile(blocks: [Data]) -> Data {
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

    public struct Response {
        static let NEW_LINE: UInt8 = 10

        public var status: String
        public var details: [Data]
        public var isOK: Bool {
            return self.status == "ok"
        }

        public init(status: String, details: [Data]) {
            self.status = status
            self.details = details
        }

        public init(_ data: Data) throws {
            guard data.count > 0 else {
                throw E.ResponseError("Empty response")
            }
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
                guard let sizeBlockPositionEnd = data.index(of: Response.NEW_LINE) else {
                    break
                }
                guard
                    let blockSizeString = String(bytes: data.prefix(upTo: sizeBlockPositionEnd), encoding: .utf8),
                    let blockSize = Int(blockSizeString)
                else {
                    throw E.ResponseError("Could not parse size (recieved bytes: \"\(bytes)\")")
                }
                var valueBytes: Bytes = []
                let valueBlockPositionEnd: Int = sizeBlockPositionEnd + blockSize
                guard data[valueBlockPositionEnd + 1] == Response.NEW_LINE else {
                    throw E.ResponseError("No newline at block end, invalid block format (recieved: \"\(bytes)\")")
                }
                for i in (sizeBlockPositionEnd + 1)...valueBlockPositionEnd {
                    valueBytes.append(data[i])
                }
                result.append(Data(bytes:valueBytes))
                if data[valueBlockPositionEnd + 2] == Response.NEW_LINE {
                    break
                }
                data = Array(data.suffix(from: valueBlockPositionEnd + 2))
            }
            guard result.count > 0 else {
                throw E.ResponseError("Could not find status in response")
            }
            guard let status = String(bytes: result.removeFirst(), encoding: .utf8) else {
                throw E.ResponseError("Could not find status in response \"\(result)\"")
            }
            self.init(status: status, details: result)
        }

        public func toString(as encoding: String.Encoding = .ascii) -> String {
            return self.details
                .map { String(data: $0, encoding: encoding) }
                .flatMap { $0 }
                .joined(separator: "\n")
        }
    }

    public let host: String
    public let port: UInt16
    public let password: String?
    public let timeout: UInt
    public var connection: Socket? = nil

    public init(
        host: String,
        port: UInt16,
        password: String? = nil,
        timeout: UInt = 1000
    ) {
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout
    }

    deinit {
        self.connection?.close()
    }

    @discardableResult private func connect() throws -> Socket {
        if self.connection != nil {
            return self.connection!
        }
        do {
            self.connection = try Socket.create(family: .inet, type: .stream, proto: .tcp)
            try self.connection!.connect(to: self.host, port: Int32(self.port), timeout: self.timeout)
            try self.connection?.setReadTimeout(value: self.timeout)
            try self.connection?.setWriteTimeout(value: self.timeout)
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

    @discardableResult public func send(
        command: Data,
        to connection: Socket? = nil,
        description: String? = nil,
        isAuth: Bool = false
    ) throws -> Response {
        let connection = try connection ?? self.connect()
        let description = description ?? String(data: command, encoding: .isoLatin1)!
        do {
            guard try connection.write(from: command) > 0 else {
                throw E.SendError("Zero bytes sent")
            }
        } catch let error {
            throw E.SendError("Could not send command \"\(description)\" (error \"\(error)\")")
        }
        do {
            var responseData = Data()
            let _ = try connection.read(into: &responseData)
            let response = try Response(responseData)
            if !isAuth && response.status == "noauth" {
                throw E.NoAuthError
            }
            return response
        } catch let error {
            throw E.ResponseError("Could not receive result for command \"\(description)\" (error \"\(error)\")")
        }
    }

    @discardableResult public func send(command: Command, to connection: Socket? = nil) throws -> Response {
        return try self.send(
            command: command.getCompiledPacket(),
            to: try connection ?? self.connect(),
            description: command.getDescription(),
            isAuth: command.isAuth()
        )
    }

    public func set(key: String, value: Data) throws {
        let response = try self.send(command: .Set(key: key, value: value))
        guard response.isOK else {
            throw E.CommandError("Could not set value by key \"\(key)\" (response: \(response))")
        }
    }

    public func get(key: String) throws -> Data? {
        return try self.send(command: .Get(key: key)).details.first
    }

    public func get(key: String, encoding: String.Encoding = .ascii) throws -> String? {
        guard let result = try self.get(key: key) else {
            return nil
        }
        return String(data: result, encoding: encoding)
    }

    public func delete(key: String) throws {
        try self.send(command: .Delete(key: key))
    }

    public func hashSet(name: String, key: String, value: Data) throws {
        let response = try self.send(command: .HashSet(name: name, key: key, value: value))
        guard response.isOK else {
            throw E.CommandError("Could not set has value by name \(name) and key \"\(key)\" (response: \(response))")
        }
    }

    public func hashGet(name: String, key: String) throws -> Data? {
        return try self.send(command: .HashGet(name: name, key: key)).details.first
    }

    public func hashDelete(name: String, key: String) throws {
        try self.send(command: .HashDelete(name: name, key: key))
    }

    public func increment(key: String) throws -> Int {
        let response = try self.send(command: .Increment(key: key))
        guard let result = response.details.first else {
            throw E.CommandError("Could not increment key \"\(key)\" (response: \(response)")
        }
        return Int(String(data: result, encoding: .utf8)!)!
    }

    public func info() throws -> String {
        return try self.send(command: .Info).toString()
    }
}
