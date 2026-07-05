// Package mycel is the MycelDB module root.
//
// MycelDB is daemon-first and is being refactored to be daemon-only. Run
// cmd/myceld as the database process and use the protobuf/gRPC Admin and Client
// APIs from api/proto or gen/go. The historical public engine and session
// runtimes have been removed; remaining file-backed runtime code is internal
// daemon implementation scaffolding and should not be used by application code.
package mycel
