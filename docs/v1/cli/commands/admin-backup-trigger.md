# mycel admin backup trigger

Trigger a manual quiesced daemon backup.

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup trigger --reason 'before upgrade'
```

A successful manual backup updates status and pushes the next scheduled run out by the configured interval. Only one backup runs at a time; concurrent triggers return an already-running conflict (`codes.Aborted` from the Admin API), which operators may retry after the active backup finishes.

During backup, `myceld` stops new non-exempt work, drains admitted work, snapshots the data directory, writes the archive and sidecar manifest atomically, applies retention, then releases quiesce. New non-exempt RPCs while quiesced return gRPC `codes.Unavailable`; applications should retry with bounded backoff.

Use `--output json` to get the full trigger response, including status and backup summary.
