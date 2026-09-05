# REPL GQL Tutorial

This tutorial shows how to use the `mycel` REPL like a database shell: log in once, create a test space, connect to that space/domain, insert a few graph records with GQL, and query them back.

The workflow is similar to using `psql` after selecting a database. In mycel, the interactive context is a **space** plus a **domain**:

```text
psql database ~= mycel space/domain
```

## Prerequisites

Start a standalone daemon and build the CLI. If you have not already done so, follow [Start mycel in Standalone Mode](standalone-start.md).

The examples assume these environment variables are set in your shell:

```sh
export MYCELD_GRPC_ADDR=127.0.0.1:9091
export BOOTSTRAP_PASSWORD='<bootstrap-admin-password>'
```

Confirm the daemon is reachable:

```sh
./bin/mycel \
  --daemon-addr "$MYCELD_GRPC_ADDR" \
  --username admin \
  --password "$BOOTSTRAP_PASSWORD" \
  auth whoami
```

## Start the REPL

Start the REPL with the daemon address. You can pass credentials up front, but this tutorial logs in from inside the REPL so the flow is visible.

```sh
./bin/mycel --daemon-addr "$MYCELD_GRPC_ADDR" repl
```

Expected prompt:

```text
mycel>
```

When standard input and output are attached to a terminal, the REPL supports readline-style line editing and in-session command history with the up/down arrow keys. Piped input still uses plain line-by-line execution for scripts and documentation examples.

## Log in

Inside the REPL:

```text
login admin <bootstrap-admin-password>
```

Expected output:

```text
logged in as admin
```

The REPL stores the username/password in local CLI state and uses them for later commands.

## Create a test space with a default domain

Create a small tutorial space owned by the bootstrap admin principal:

```text
space add "GQL Tutorial" --owner-username admin --default-domain-key default --default-domain-name Default
```

Expected output:

```text
space added: GQL Tutorial (<space-id>)
```

The `--default-domain-key default` flag creates an initial domain named `default`. GQL runs inside a graph domain, so having a default domain makes the next connection step convenient.

If you prefer JSON output for copying IDs, run the command outside the REPL or start the REPL with `--output json`; otherwise the tutorial uses names.

## Connect to the space/domain

Connect by space name:

```text
connect space "GQL Tutorial"
```

Expected output:

```text
connected to space GQL Tutorial (<space-id>) domain default (<domain-id>)
```

The prompt now shows the connected context:

```text
mycel[GQL Tutorial/default]>
```

You can also connect with the psql-style alias:

```text
\c "GQL Tutorial"
```

Or specify both space and domain explicitly:

```text
connect "GQL Tutorial/default"
```

If the space name is ambiguous, connect by space ID instead.

## Insert example nodes

Insert a few people. The REPL `gql` shortcut takes the rest of the line as query text, so you do not need to quote the whole query.

```text
gql INSERT (:Person {name: 'Alice', role: 'Engineer', city: 'Montreal'})
gql INSERT (:Person {name: 'Bob', role: 'Designer', city: 'Montreal'})
gql INSERT (:Person {name: 'Carol', role: 'Engineer', city: 'Toronto'})
```

Each insert should report a committed write, for example:

```text
query executed: nodes_inserted=1 edges_inserted=0 revision=<revision>
```

## Query all people

Return all `Person` nodes:

```text
gql MATCH (p:Person) RETURN p FETCH FIRST 10 ROWS ONLY
```

The text output prints one row per matched record. Returning a whole node renders the node value as JSON-like text.

## Query scalar properties

Scalar projections are easier to read in the terminal:

```text
gql MATCH (p:Person) RETURN p.name, p.role, p.city FETCH FIRST 10 ROWS ONLY
```

Example output:

```text
p.name="Alice"    p.role="Engineer"    p.city="Montreal"
p.name="Bob"      p.role="Designer"    p.city="Montreal"
p.name="Carol"    p.role="Engineer"    p.city="Toronto"
query executed: rows=3
```

## Filter with `WHERE`

Find engineers:

```text
gql MATCH (p:Person) WHERE p.role = 'Engineer' RETURN p.name, p.city FETCH FIRST 10 ROWS ONLY
```

Expected rows:

```text
p.name="Alice"    p.city="Montreal"
p.name="Carol"    p.city="Toronto"
query executed: rows=2
```

Find people in Montreal:

```text
gql MATCH (p:Person) WHERE p.city = 'Montreal' RETURN p.name, p.role FETCH FIRST 10 ROWS ONLY
```

Expected rows:

```text
p.name="Alice"    p.role="Engineer"
p.name="Bob"      p.role="Designer"
query executed: rows=2
```

## Insert and query relationships

Create a relationship between two matched people:

```text
gql MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2026}]->(b)
```

Expected output:

```text
query executed: nodes_inserted=0 edges_inserted=1 revision=<revision>
```

Then query it:

```text
gql MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, r.since, b.name FETCH FIRST 10 ROWS ONLY
```

Expected row:

```text
a.name="Alice"    r.since=2026    b.name="Bob"
query executed: rows=1
```

## Switch domains

List domains in the current space:

```text
domain list
```

Create another domain:

```text
domain add scratch --name Scratch
```

Connect to it:

```text
connect domain scratch
```

Prompt:

```text
mycel[GQL Tutorial/scratch]>
```

Queries now run against the `scratch` domain. The people inserted in `default` will not appear there unless you insert or import data into `scratch`.

Switch back:

```text
connect domain default
```

## Disconnect and exit

Disconnect clears the local space/domain context but keeps you logged in:

```text
disconnect
```

Prompt returns to:

```text
mycel>
```

Exit the REPL:

```text
exit
```

## Troubleshooting

### `no space connected`

You ran `gql ...` before connecting to a space. Use:

```text
connect space "GQL Tutorial"
```

### `no domain connected`

The connected space has no default domain or has ambiguous domains. Use:

```text
connect domain <domain-key-or-id>
```

### Space name is ambiguous

If multiple visible spaces have the same name, connect by UUID:

```text
connect space <space-id>
```

### Authentication fails

The REPL uses the current local credentials. Run `login` again:

```text
login admin <bootstrap-admin-password>
```

### You see `unknown service mycel.client.v1.AuthService`

Your CLI binary is stale. Rebuild it against the unified auth API:

```sh
MYCEL_API_ROOT=../mycel-api make build-cli
```
