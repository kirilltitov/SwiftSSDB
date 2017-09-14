import Foundation
import Socket

public class SSDB {
    public typealias Bytes = [UInt8]

    private enum Stage {
        case Size, Payload
    }

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
        public let status: String
        public let payload: [Data]
        public var result: Data? {
            return payload.first
        }
        public var isOK: Bool {
            return self.status == "ok"
        }

        private init(status: String, payload: [Data]) {
            self.status = status
            self.payload = payload
        }

        init(from data: [Data]) throws {
            guard let statusData = data.first else {
                throw E.ResponseError("Empty response")
            }
            guard let status = String(data: statusData, encoding: .ascii) else {
                throw E.ResponseError("Could not parse status from \(statusData)")
            }
            self.init(
                status: status,
                payload: [Data](data.count > 1 ? data[1...] : [])
            )
        }

        public func toString(as encoding: String.Encoding = .ascii) -> String {
            return self.payload
                .map { String(data: $0, encoding: encoding) }
                .flatMap { $0 }
                .joined(separator: "\n")
        }
    }

    private static let NEW_LINE: UInt8 = 10

    public let host: String
    public let port: UInt32
    public let password: String?
    public let timeout: UInt
    private var connection: Socket? = nil
    private var stage: Stage = .Size
    private var recvBuffer = Bytes()
    private var response: [Data] = []
    private var blockSize: Int = 0
    private let queue = DispatchQueue(label: "com.ssdb", qos: .userInteractive)

    public init(host: String, port: UInt32 = 8888, password: String? = nil, timeout: UInt = 1000) {
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout
    }

    private func getConnection() throws -> Socket {
        if let connection = self.connection {
            return connection
        }
        do {
            let connection: Socket = try Socket.create(family: .inet, type: .stream, proto: .tcp)
            try connection.setReadTimeout(value: self.timeout)
            try connection.setWriteTimeout(value: self.timeout)
            try connection.connect(to: self.host, port: Int32(self.port), timeout: self.timeout)
            if let password = self.password {
                guard try self.send(command: .Auth(password: password), to: self.connection!).isOK else {
                    throw E.AuthError
                }
            }
            self.connection = connection
            return connection
        } catch let error {
            throw E.ConnectError("Could not connect to \(self.host):\(self.port) \(error)")
        }
    }

    @discardableResult public func send(
        command: Data,
        to connection: Socket? = nil,
        description: String? = nil,
        isAuth: Bool = false
    ) throws -> Response {
        return try self.queue.sync {
            let description = description ?? String(data: command, encoding: .isoLatin1)!
            let socket = try connection ?? self.getConnection()
            do {
                guard try socket.write(from: command) > 0 else {
                    throw E.SendError("Zero bytes sent")
                }
            } catch let error {
                throw E.SendError("Could not send command \"\(description)\" (error \"\(error)\")")
            }
            do {
                let response = try self.read(from: socket)
                if !isAuth && response.status == "noauth" {
                    throw E.NoAuthError
                }
                return response
            } catch let error {
                throw E.ResponseError("Could not receive result for command \"\(description)\" (error \"\(error)\")")
            }
        }
    }

    @discardableResult public func send(command: Command, to connection: Socket? = nil) throws -> Response {
        return try self.send(
            command: command.getCompiledPacket(),
            to: connection,
            description: command.getDescription(),
            isAuth: command.isAuth()
        )
    }

    private func read(from socket: Socket) throws -> Response {
        self.stage = .Size
        while true {
            if let result = self.parse() {
                return try Response(from: result)
            }
            var data: Data = Data()
            let _ = try socket.read(into: &data)
            self.recvBuffer.append(contentsOf: Bytes(data))
        }
    }

    private func parse() -> [Data]? {
        var left = 0, right = 0
        let bufferSize = self.recvBuffer.count
        loop: while true {
            left = right
            switch self.stage {
            case .Size:
                guard let index = self.recvBuffer[left...].index(of: SSDB.NEW_LINE) else {
                    break loop
                }
                right = index
                right += 1
                let line = self.recvBuffer[left ..< (right - 1)]
                left = right
                if line.count == 0 {
                    self.recvBuffer = Bytes(self.recvBuffer[left...])
                    let result = self.response
                    self.response = []
                    return result
                }
                self.blockSize = Int(String(bytes: line, encoding: .ascii)!.trimmingCharacters(in: .whitespacesAndNewlines))!
                self.stage = .Payload
                fallthrough
            case .Payload:
                right = left + self.blockSize
                if right <= bufferSize, let n = self.recvBuffer[right...].index(of: SSDB.NEW_LINE) {
                    self.response.append(Data(bytes: self.recvBuffer[left ..< right]))
                    right = n + 1
                    self.stage = .Size
                    continue loop
                }
                break loop
            }
        }
        if left > 0 {
            self.recvBuffer = Bytes(self.recvBuffer[left...])
        }
        return nil
    }

    public func set(key: String, value: Data) throws {
        let response = try self.send(command: .Set(key: key, value: value))
        guard response.isOK else {
            throw E.CommandError("Could not set value by key \"\(key)\" (response: \(response))")
        }
    }

    public func get(key: String) throws -> Data? {
        return try self.send(command: .Get(key: key)).result
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
        return try self.send(command: .HashGet(name: name, key: key)).result
    }

    public func hashDelete(name: String, key: String) throws {
        try self.send(command: .HashDelete(name: name, key: key))
    }

    public func increment(key: String) throws -> Int {
        let response = try self.send(command: .Increment(key: key))
        guard let result = response.result else {
            throw E.CommandError("Could not increment key \"\(key)\" (response: \(response)")
        }
        return Int(String(data: result, encoding: .utf8)!)!
    }

    public func info() throws -> String {
        return try self.send(command: .Info).toString()
    }
}

