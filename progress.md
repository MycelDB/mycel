WAL propagation audit complete. Findings written to /tmp/wal-prop-audit.md

Key results:
- WAL read APIs: Manager.ReadFrom(ctx, lsn) is inclusive; Iterator.Next() scans records; ReadNextBlocking(ctx, lsn) waits for committed target and returns one record.
- WAL apply APIs: Registry.Register/Applier plus Applier.ApplyWAL; Recovery.Recover contains the current registry application/progress loop.
- Backend construction: clustering.NewManager constructs backend.NewService(...).WithMembership(...).WithAuthority(...); app.Run passes rt.ClusterManager.BackendService() to server.Start; server.New registers ClusterBackendService.
- Current backend proto/service has no WAL propagation RPC/messages; WatchClusterUpdates is unimplemented cluster metadata stream.
