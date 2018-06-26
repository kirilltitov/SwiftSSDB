// swift-tools-version:4.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SSDB",
    products: [
        .library(name: "SSDB", targets: [ "SSDB" ]),
    ],
    dependencies: [
        .package(url: "https://github.com/IBM-Swift/BlueSocket.git", .upToNextMajor(from: "1.0.0"))
    ],
    targets: [
        .target(
            name: "SSDB",
            dependencies: [ "Socket" ]
        ),
        .testTarget(
            name: "SSDBTests",
            dependencies: [ "SSDB" ]
        ),
    ]
)
