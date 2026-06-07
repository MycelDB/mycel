# Programmatic GQL-style queries

KnotDB exposes a Go query builder in `martinbeauvais.com/mbgit/knotbase/knotdb/query`. The builder maps to GQL-style clauses:

- `Match(...)`
- `Where(...)`
- `Return(...)`
- `OrderBy(...)`

The first implementation runs in memory over the current session's nodes, edges, and templates.

## Last seven calendar days of journals

```go
import q "martinbeauvais.com/mbgit/knotbase/knotdb/query"

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
