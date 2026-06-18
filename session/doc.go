// Package session defines the scoped MycelDB graph-space interaction API.
//
// A Session is opened by the engine for one authenticated user and one space.
// It carries read/write/admin permissions and is the public home for graph
// operations, template operations, and future query/transaction APIs.
package session
