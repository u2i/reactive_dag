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
| [`:repo`](#repo) | — | **yes** | `Suspension` |
| [`:suspension_table`](#suspension_table) | `"reactive_dag_suspension"` | no | `Suspension`, `Migration` |
| [`:cascade_timeout`](#cascade_timeout) | `30_000` | no | `Suspension` |
| [`:dirty_table`](#dirty_table) | `"reactive_dag_dirty"` | no | `Migration.drop_dirty/1` only |
| [`:plan_mfa`](#plan_mfa) | — | only with `ScanWorker` | `ScanWorker` |
| [`:insights_keep`](#insights_keep) | `20` | no | `Insights` |
| [`:cascade_enqueuer`](#cascade_enqueuer) | `CascadeWorker.enqueue/3` | no | `dirties_on`, `augmented_by`, `Source` |
| [`:around_poll`](#around_poll) | — | no | `ScanWorker` |

---

### `:repo`

Your AshPostgres repo. The library goes through it with raw SQL for the one
table it owns — the suspension table — because the reads and writes there are
small, hot and shaped for a queue-like access pattern rather than for Ash
actions.

```elixir
config :reactive_dag, repo: MyApp.Repo
```

**The only required key.** Omitting it raises on the *first query* — which may
be a long way into a deploy — so validate at boot instead (below).

### `:suspension_table`

The physical table name for suspensions — where a cascade records that it
stopped, and the successor to the dirty frontier.

```elixir
config :reactive_dag, suspension_table: "my_suspensions"
```

Resolved identically by `ReactiveDag.Suspension` and `ReactiveDag.Migration`,
and validated against an identifier grammar at read time, so a typo fails
loudly rather than as a syntax error deep inside a query.

### `:cascade_timeout`

How long a cascade's transaction may run, in milliseconds.

```elixir
config :reactive_dag, cascade_timeout: 30_000
```

A cascade contains only fast work by construction: anything slow declares
itself so and becomes a suspension instead of running inline. So this is a
**diagnostic**, not a capacity dial — if a cascade transaction is hitting 30
seconds, a cell declared cheap is not, and you want to find out rather than
wait longer.

The drain this replaced used `timeout: :infinity`, justified by "a recompute
legitimately runs for minutes". That is exactly the condition that let a
nine-minute extraction hold a connection until the database closed it. Raising
this back to `:infinity` restores that failure.

### `:dirty_table`

**Nothing reads this at runtime.** It names the OLD queue table, and its only
consumer is `ReactiveDag.Migration.drop_dirty/1` — the migration that removes
it.

```elixir
config :reactive_dag, dirty_table: "my_existing_dirty"
```

Set it only if your queue table was not called `reactive_dag_dirty` and you
have not yet dropped it. Once the drop has run everywhere, delete the line: it
configures nothing.

Deliberately still validated as an identifier, because it is still
interpolated into DDL.

### `:plan_mfa`

How `ReactiveDag.ScanWorker` builds the plan it scans.

```elixir
config :reactive_dag, plan_mfa: {MyApp.Dag, :plan, []}
```

A plan is built from resource modules at runtime, so it cannot ride in an Oban
job argument. The worker needs one name for it; this is that name. A job may
override it (`%{"plan_mfa" => ["MyApp.Dag", "plan", []]}`), which is what lets
one app schedule scans over more than one graph.

Only read by `ScanWorker`. A host scheduling its own polls never needs it.

### `:cascade_enqueuer`

How a write queues the cascade that propagates it.

```elixir
config :reactive_dag, cascade_enqueuer: fn cell, keys, opts -> MyApp.Job.enqueue(cell, keys) end
```

The default enqueues `ReactiveDag.CascadeWorker`, which is almost always what
you want. Override it when the cascade needs to be YOUR job — a different
queue, a longer debounce, or a wrapper that records the run id your activity
page groups by.

Called from inside the write's transaction, so it must be cheap: an INSERT, not
the cascade itself. Must return `{:ok, term}` or `{:error, term}`.

An error is logged and swallowed rather than raised, because a queue being down
should not fail a user's write. **This is a real change from the mark it
replaced**: a mark was durable on its own, so a failed enqueue only delayed
propagation. A failed enqueue now means nothing downstream recomputes until
that row is written again or a scan re-observes it. The log line says so
explicitly.

Read by `dirties_on`, `augmented_by`, and `ReactiveDag.Source` polls.

### `:insights_keep`

How many runs `ReactiveDag.Insights.record/1` retains in its rolling in-memory
window.

```elixir
config :reactive_dag, insights_keep: 50
```

Only relevant if you call `Insights.record/1` (neither a scan nor a cascade
persists anything on its own — the library reports, the host records). It takes
a `%ReactiveDag.ScanRun{}` or a bare `%ReactiveDag.Report{}`, and retains a run
either way — so a log line can show the poll's duration, changed keys, cost and
`unreachable` list rather than only the propagation's much smaller share.

The buffer is per-BEAM-node, in memory, and lost on restart; it exists so a
dashboard has something to show without the host building storage. A host
wanting history stores the run where its runs already live.

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
  * `:suspension_table` "my suspensions" is not a valid SQL identifier
```

It reports **every** problem, not the first — a config with two mistakes
should take one deploy to fix. `ReactiveDag.Config.problems/0` returns the same list without
raising, for a host that would rather log them.

**The host calls it**; the library does not start its own application to do it
automatically. That would give `reactive_dag` a supervision tree and an opinion
about when it starts, and would make the check unskippable by tests that
deliberately run unconfigured.

It checks only what is *definitely* wrong, and touches no database — whether
the tables exist is `ReactiveDag.Migration`'s business, and a boot check that
queried would make booting depend on the database being reachable.

## What is *not* configured here

- **Scheduling** — when to cascade is the host's, though `dirties_on`,
  `augmented_by` and a `Source` poll all enqueue one for you. See
  [Getting started](getting-started.html).
- **How a node recomputes, and how its changes propagate** — declared in the
  node's `reactive` block and read off the plan, not configured and not passed
  per call.
- **Per-node behaviour** — `payload_key`, `payload_action`, `key_rule`,
  `recompute_by` and the rest are declared in a resource's `reactive` block, not
  in application config. See [Authoring nodes](authoring-nodes.html).

### `:around_poll`

A wrapper `ScanWorker` runs the POLL inside, for the one thing a telemetry
handler cannot do: be present while the fetch happens.

```elixir
config :reactive_dag, around_poll: {MyApp.Audit, :around, []}
```

The function takes the job's `args` and a one-arity function, and returns
whatever that function returns. Its argument is a keyword list merged into the
poll's options — which is the point: a wrapper that starts a collector hands
the scanner a way to reach it.

```elixir
def around(_args, run) do
  {:ok, pid} = Agent.start_link(fn -> [] end)

  try do
    run.(collector: pid)
  after
    persist(Agent.get(pid, & &1))
    Agent.stop(pid)
  end
end
```

**Reach for a telemetry handler first.** Broadcasts, durable scan rows,
follow-up enqueues — everything that only needs the RESULT — belongs on
`[:reactive_dag, :scan, :stop]`, which carries a `ReactiveDag.ScanRun` with
both phases. A wrapper puts work inside the job's failure boundary, so use it
only when the work must happen DURING the poll.

The case it exists for: a host that records every HTTP request its crawler
makes needs something live for the duration, and a process cannot ride in an
Oban argument. Without this, that host forks the worker — and then owns the
poll/mark/drain loop forever to keep one wrapper.
