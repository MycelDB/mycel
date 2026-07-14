# Programmatic GQL-style queries

Historical note: the former public Go query builder has been internalized as daemon implementation. Applications should use the daemon query API instead. Internally, the builder maps to GQL-style clauses:

- `Match(...)`
- `Where(...)`
- `Return(...)`
- `OrderBy(...)`

The implementation runs in memory over the current session's or transaction's nodes, edges, and templates. Use `tx.Query()` inside a transaction when the query must see staged writes or hide staged deletes.

## Last seven calendar days of journals

```go
// Daemon-internal code only; not an application import path.
rows, err := sess.Query().
    Match(
        q.Pattern().
            Node("journal", q.Template("logseq.journal")).
            Out("contains", q.Depth(1, q.Unbounded)).
            Node("entry", q.Template("logseq.journal_entry")),
    ).
    Where(
        q.Between(
            q.Prop("journal", "journal_date"),
            q.CurrentDate().Minus(q.Days(6)),
            q.CurrentDate(),
        ),
    ).
    Return(
        q.Var("journal"),
        q.Tree("entry").As("entries"),
    ).
    OrderBy(q.Prop("journal", "journal_date"), q.Desc).
    Execute(ctx)
```

This returns one row per matching journal node, newest to oldest. `q.Tree("entry")` returns the matched journal-entry descendants as a nested forest, preserving `graph.EdgeKindContains` hierarchy and excluding descendants that did not match the `entry` node pattern.

## Current traversal behavior

`Out("contains", ...)` traverses explicit `graph.EdgeKindContains` edges. Sibling ordering is taken from the parent template's child order policy when it specifies an edge property, such as `order`.
