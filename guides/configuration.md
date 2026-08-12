# Configuration

Every `config :reactive_dag, …` key the library reads, what it does, and when
you would change it.

Only **`:repo`** is required. Everything else has a working default, and most
hosts never touch more than two or three.

```elixir
# config/config.exs — a typical host
config :reactive_dag, repo: MyApp.Repo
```

## The keys

| key | default | required? | read by |
|---|---|---|---|
| [`:repo`](#repo) | — | **yes** | `Frontier` |
| [`:dirty_table`](#dirty_table) | `"reactive_dag_dirty"` | no | `Frontier`, `Migration` |
| [`:set_op_templates`](#set_op_templates) | `%{}` | only with `SetOp` | `SetOp` |
| [`:insights_keep`](#insights_keep) | `20` | no | `Insights` |

---

### `:repo`

Your AshPostgres repo. The library goes through it with raw SQL for the one
table it owns — the dirty frontier — because claim-as-delete
(`DELETE … RETURNING`) and the coalescing upserts don't express cleanly as Ash
actions.

```elixir
config :reactive_dag, repo: MyApp.Repo
```

**The only required key.** Omitting it raises on the *first query* — which may
be a long way into a deploy — so validate at boot instead (below).

### `:dirty_table`

The physical table name for the frontier.

```elixir
config :reactive_dag, dirty_table: "my_existing_dirty"
```

This exists so **a host adopting the library keeps its table without a
rename** — both current hosts grew their own frontier table before the library
existed. On a green-field app, leave it alone.

The name is the one identifier SQL cannot parameterise, so it is validated
against an identifier grammar at read time: a typo fails loudly rather than as
a syntax error deep inside a query.

`ReactiveDag.Migration` resolves `:dirty_table` exactly as `Frontier` does, so
a host that sets the config gets a migration matching the table the runtime
queries — there is no second place to keep in sync.

### `:set_op_templates`

A registry of `op → template` functions for hosts whose recompute is set-based
SQL keyed by `cell.op`, rather than per-key Elixir.

```elixir
config :reactive_dag, set_op_templates: %{
  reconcile: &MyApp.Recompute.reconcile/2,
  relation: &MyApp.Recompute.relation/2
}
```

Only used by `ReactiveDag.SetOp`. A host using `ReactiveDag.Node.Recompute` —
the common case — never sets this. An op with no registered template logs a
warning and recomputes nothing, rather than crashing the drain.

### `:insights_keep`

How many `%ReactiveDag.Drain.Report{}`s `ReactiveDag.Insights.record/1` retains
in its rolling in-memory window.

```elixir
config :reactive_dag, insights_keep: 50
```

Only relevant if you call `Insights.record/1` (the drain persists nothing on its
own — the library reports, the host records). The buffer is per-node, in
memory, and lost on restart; it exists so a dashboard has something to show
without the host building storage. A host wanting history stores the report
where its runs already live.

## Validating at boot

Misconfiguration otherwise surfaces at the first query, in whatever process
happened to trigger it. `ReactiveDag.Config.validate!/0` moves that to boot:

```elixir
def start(_type, _args) do
  ReactiveDag.Config.validate!()
  Supervisor.start_link(children, opts)
end
```

```
** (ReactiveDag.Config.Error) reactive_dag is misconfigured:

  * `:repo` is not set (required) — add `config :reactive_dag, repo: MyApp.Repo`
  * `:dirty_table` "my dirty" is not a valid SQL identifier
```

It reports **every** problem, not the first — a config with two mistakes
should take one deploy to fix. `Config.problems/0` returns the same list without
raising, for a host that would rather log them.

**The host calls it**; the library does not start its own application to do it
automatically. That would give `reactive_dag` a supervision tree and an opinion
about when it starts, and would make the check unskippable by tests that
deliberately run unconfigured.

It checks only what is *definitely* wrong, and touches no database — whether
the tables exist is `ReactiveDag.Migration`'s business, and a boot check that
queried would make booting depend on the database being reachable.

## What is *not* configured here

- **Scheduling** — when to call `Drain.run/2`, and with which strategy and key
  rule, is passed per call. See [Getting started](getting-started.html).
- **The recompute strategy and key rule** — also per-call `Drain.run/2` options,
  not global config.
- **Per-node behaviour** — `payload_key`, `payload_action`, `key_rule`,
  `recompute_by` and the rest are declared in a resource's `reactive` block, not
  in application config. See [Authoring nodes](authoring-nodes.html).
