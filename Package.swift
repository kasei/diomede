// swift-tools-version:5.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Diomede",
    platforms: [.macOS(.v10_15)],
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "Diomede",
            targets: ["Diomede"]),
        .library(
            name: "DiomedeQuadStore",
            targets: ["DiomedeQuadStore"]),
        .executable(
        	name: "diomede-db-util",
        	targets: ["diomede-db-util"]),
        .executable(
        	name: "diomede",
        	targets: ["diomede-cli"]),
    ],
    dependencies: [
        .package(url: "https://github.com/agisboye/CLMDB.git", from: "0.9.24"),
		.package(name: "CryptoSwift", url: "https://github.com/krzyzanowskim/CryptoSwift.git", .upToNextMinor(from: "1.0.0")),
		.package(name: "SPARQLSyntax", url: "https://github.com/kasei/swift-sparql-syntax.git", .upToNextMinor(from: "0.0.108")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "Diomede",
            dependencies: [
            	.product(name: "LMDB", package: "CLMDB"),
            	"SPARQLSyntax",
            ]),
        .target(
            name: "DiomedeQuadStore",
            dependencies: [
            	"Diomede",
            	"CryptoSwift",
            	"SPARQLSyntax"
            ]),
        .target(
            name: "diomede-db-util",
            dependencies: ["Diomede"]
        ),
        .target(
            name: "diomede-cli",
            dependencies: ["DiomedeQuadStore"]
        ),
        .testTarget(
            name: "DiomedeTests",
            dependencies: ["Diomede", "DiomedeQuadStore"]),
    ]
)
