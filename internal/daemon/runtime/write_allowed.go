package runtime

// RequireLocalWriteAllowed is the daemon-level write gate used by modules before
// local mutation paths. Standalone writes are allowed, and clustered writes are
// routed/forwarded by module-specific Raft executors.
func (r *Runtime) RequireLocalWriteAllowed() error {
	return nil
}
