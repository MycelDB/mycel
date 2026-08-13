package identity

// PrincipalID uniquely identifies an authenticated or system actor.
//
// Principal IDs are immutable stable identifiers. Human principals created by
// the daemon use UUID strings, while reserved system/service principals may use
// stable non-UUID identifiers.
type PrincipalID string

func (id PrincipalID) String() string { return string(id) }

// PrincipalRef is a human-readable login/reference for a principal.
type PrincipalRef string
