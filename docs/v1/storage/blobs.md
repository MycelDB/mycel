# Blob Storage

MycelDB stores binary/blob payloads under the top-level `blobs/` directory, separate from graph segment storage.

```text
<data-root>/
  blobs/
    <space_id>/
      objects/
        <aa>/
          <sha256-hex>
      tmp/
```

Blob storage is per-space but physically separate from `graphs/<space_id>/` so large binary data can be backed up, tiered, or restored independently of graph structure.

## Directory Responsibilities

### `blobs/<space_id>/objects/`

Stores immutable content-addressed blob objects.

Object path:

```text
blobs/<space_id>/objects/<first-two-sha256-hex-chars>/<sha256-hex>
```

Example:

```text
blobs/1099742f-6001-4354-b3a2-0477def9d40d/objects/ab/abcdef...
```

Properties:

- object IDs are SHA-256 content hashes
- identical content is stored once per space
- object files are immutable after promotion
- the two-character fan-out directory avoids very large single directories

### `blobs/<space_id>/tmp/`

Stores temporary staged blob writes.

Writes stream into `tmp/` first. After validation and fsync, committed blobs are renamed/promoted into `objects/`.

Temporary files may be removed on rollback or by stale-temp cleanup.

## Graph References

Blob payloads are referenced from graph nodes, not from a separate persisted blob index.

A graph node may reference a blob through its `BlobRef` field. A node has either:

```text
inline text content
```

or:

```text
blob reference
```

but not both.

Blob nodes keep `Content` empty. Text about the blob, such as captions or alt text, lives in node props or child annotation nodes.

Common blob metadata props:

```text
mime_type
size_bytes
original_filename
declared_mime_type
caption
alt_text
```

The system `blob` template validates blob metadata by default.

## Transactional Write Flow

Blob writes are coordinated with graph commits.

Typical flow:

1. stream payload to `blobs/<space_id>/tmp/<tmp-id>`
2. compute SHA-256 digest while streaming
3. validate size/type limits
4. stage a graph node referencing the resulting blob ID
5. on transaction commit, promote the temporary file to `objects/<aa>/<sha256>`
6. append the graph transaction records to `graphs/<space_id>/segments/*.kseg`
7. remove temporary state

Rollback removes staged temporary files.

If graph commit fails after blob promotion, Mycel removes promoted staged blobs that no committed node references.

## Refcounting and Orphan Cleanup

There is no persisted blob refcount file.

On open, Mycel rebuilds an in-memory blob reference index from committed graph node records:

```text
blob_id -> live node IDs
```

A blob object is deleted when the last live node referencing it is removed.

Orphan cases:

- blob written but node never committed
- blob promoted but graph commit failed
- process crashed during staging/promotion

These are reclaimed by best-effort sweeps when the blob store is opened.

## Delete Behavior

Deleting a blob node removes the graph node and eventually decrements the in-memory blob refcount.

Deleting a space removes:

```text
blobs/<space_id>/
```

along with:

```text
graphs/<space_id>/
```

## Limits and Validation

Blob write limits are configured through engine/session config.

Supported limit categories include:

```text
global blob max size
image max size
PDF max size
audio max size
video max size
other/uncategorized max size
stale tmp age
```

A value of `0` means disallowed for that category. A value of `-1` means unlimited where supported.

## Recovery Guarantees

- visible object files are complete immutable objects
- temporary files are not considered committed
- graph references are authoritative
- blob-to-node indexes are rebuilt from graph records
- orphan cleanup is best-effort and safe because object IDs are content hashes
