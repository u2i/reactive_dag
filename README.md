# reactive_dag

A domain-agnostic **reactive DAG engine** for Elixir/Ash apps: a dirty frontier
+ depth-ordered incremental drain + change propagation. Extracted from two apps
that independently grew the same engine (the Red Hook `cascade` pipeline and the
u2i compliance portal's model_eval).

The substrate decides *when* and *in what order* cells recompute; it never
decides *how*. You bring:

- **`ReactiveDag.RecomputeStrategy`** — how a cell recomputes (per-key Elixir, or
  one set-based SQL join). Returns the keys that actually changed.
- **`ReactiveDag.KeyRule`** — how a change propagates to a parent (identity, a
  remap, or `:all` for a whole-cell recompute).

Everything else is provided: the `Plan`/`Cell` IR, `Graph.build` (validate +
depths + cycle check), the `Frontier` (claim-as-delete over
`reactive_dag_dirty`), and the `Drain` loop.

```elixir
plan = ReactiveDag.Graph.build(cells)
{:ok, passes} = ReactiveDag.Drain.run(plan, recompute: MyApp.Ops, key_rule: MyApp.Rules)
```

Status: **phase 0** — interface skeleton (compiles); neither host migrated yet.
See [docs/adr-001-reactive-dag-library.md](docs/adr-001-reactive-dag-library.md)
for the full boundary, the two seams, and per-app migration cost.
