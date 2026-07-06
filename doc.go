// Package mycel is the MycelDB module root.
//
// This module is not an embeddable application library. It contains the myceld
// daemon, the mycel CLI, and internal implementation packages. Applications
// should talk to myceld through the protobuf/gRPC Admin and Client APIs from
// github.com/myceldb/mycel-api, preferably via github.com/myceldb/mycel-go-sdk.
//
// The historical public engine, session, domain, query, and store implementation
// packages have been removed or internalized. Do not open or mutate a Mycel data
// directory from an application process; myceld owns the runtime and storage.
package mycel
