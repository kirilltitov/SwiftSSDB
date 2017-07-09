#  SSDB driver for Swift

This is experimental package for communication with [SSDB](http://ssdb.io) database.
This version currently supports just some basic commands: `auth`, `get`, `set`, `del`, `hset`, `hget`, `hdel` and `incr`.

## Example usage:

```
import SSDB

let db = SSDB(
    host: "127.0.0.1",
    port: 8888,
    password: "passwd", // optional, no need to auth explicitly
    keepAlive: true     // optional
)

try! db.set(key: "foo", value: "bar".data(using: .utf8)!)
if let result: Data = try! db.get(key: "foo") {
    print(String(data: result, encoding: .utf8)!)
}

```

Don't forget to do/catch errors pls :) Using exclamation points is really bad practice.

## FAQ
*Q*: Why Data as value type? Why not String?

*A*: Mainly because in SSDB all strings are binary safe. But secondly it's because I use SSDB to store binary data packed with MessagePack.


*Q*: Why then keys are not Data?

*A*: Now that is really exotic IMO :) No need, to be honest. Maybe once.


*Q*: What about the rest of commands?

*A*: One day, sure. For now I don't need anything else. But in case you *really* want to use unimplemented command, you may always call `SSDB.send(command: Data) throws -> SSDB.Response` method with help of `SSDB.Command.compile(blocks: [String]) -> Data` and manually handle the response.
