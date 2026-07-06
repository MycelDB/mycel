package identity

// Owner is the top-level storage tenant boundary.
//
// Owners are always users, so ownership is represented by the user's immutable
// internal identifier.
type Owner = UserID
