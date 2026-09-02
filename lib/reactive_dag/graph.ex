defmodule ReactiveDag.Graph do
  @moduledoc """
  Pure DAG construction: a list of `ReactiveDag.Cell` → a `ReactiveDag.Plan`.

  No live data — only the plan (cells, parent edges, depths). Validates that
  every referenced input exists and the graph is acyclic (raises `ArgumentError`
  on a dangling input or cycle; a host's DSL transformer should catch these at
  compile time, but the runtime builder re-checks so a hand-built plan can't
  wedge a cascade).
  """

  require Logger

  alias ReactiveDag.{Cell, Plan}

  @spec build([Cell.t()]) :: Plan.t()
  def build(cells) when is_list(cells) do
    by_id = Map.new(cells, &{&1.id, &1})
    validate_inputs!(cells, by_id)

    # `tenant` explicitly, though it equals the struct default. Omitting it makes
    # the INFERRED type of this expression narrower than `%Plan{}` — Elixir's
    # type checker reports the omitted field as absent rather than defaulted — so
    # a host with `def f(plan \\ build(...))` and a `%Plan{}` spec got an
    # "incompatible types" warning it could do nothing about.
    %Plan{
      cells: by_id,
      parents: build_parents(cells),
      depths: build_depths(by_id),
      tenant: "*"
    }
  end

  @doc """
  Deprecated. Renamed to `claims_for/5`.

  "Dirty" was the vocabulary of the queue this engine no longer has: a write
  MARKED cells dirty and a drain later walked the frontier. A cascade is told
  what changed and walks immediately, so what this returns is a CLAIM — what
  each parent says it needs recomputed — not a mark left for someone else.

  Kept as a delegate because the seam is public and hosts call it directly.
  """
  @deprecated "Use claims_for/5 instead"
  @spec dirty_parents(Plan.t(), Cell.id(), [String.t()], module(), map()) ::
          [{Cell.id(), [String.t()]}]
  def dirty_parents(plan, child_id, keys, key_rule \\ ReactiveDag.Node.KeyRule, diffs \\ %{}),
    do: claims_for(plan, child_id, keys, key_rule, diffs)

  @doc """
  What each parent CLAIMS when `child`'s keys change, applying the key rule
  each parent DECLARED. The rule sees the parent, the specific child input, and
  the changed keys, and returns `:all` (whole-cell recompute, the `"*"`
  wildcard) or `{:keys, mapped}`.

  Returns `[{parent_id, [key]}]`. `key_rule` defaults to
  `ReactiveDag.Node.KeyRule`, which reads `:identity | :all | :group` off the
  authored block — a cascade always uses that one, and the parameter exists so
  a host calling this directly gets the same answer.

  A rule that returns anything outside its contract is answered `:all` and
  logged, rather than passed on: an out-of-contract value used to travel into
  the walk and fail several frames away, naming neither the rule nor the edge.

  `diffs` maps a changed key to the DIFF of that change — both sides, as
  captured by the payload write that produced it. A rule implementing `rule/4`
  receives it and can derive a claim from a row that no longer exists; one
  implementing only `rule/3` is called exactly as before.
  """
  @spec claims_for(Plan.t(), Cell.id(), [String.t()], module(), map()) ::
          [{Cell.id(), [String.t()]}]
  def claims_for(
        %Plan{} = plan,
        child_id,
        keys,
        key_rule \\ ReactiveDag.Node.KeyRule,
        diffs \\ %{}
      ) do
    plan.parents
    |> Map.get(child_id, [])
    |> Enum.map(fn parent_id ->
      parent = Map.fetch!(plan.cells, parent_id)

      case apply_rule(key_rule, parent, child_id, keys, diffs, Plan.frontier_opts(plan)) do
        :all ->
          {parent_id, ["*"]}

        {:keys, mapped} when is_list(mapped) ->
          {parent_id, mapped}

        # OUT OF CONTRACT — degrade to a whole-cell claim rather than pass it on.
        #
        # A rule returns `:all | {:keys, [key]}`. Anything else used to fall
        # through this `case` unmatched and travel on as if it were a key list,
        # detonating several frames later inside `Cascade.entries_for/6` on
        # `"*" in mapped_keys` — a `Protocol.UndefinedError` naming neither the
        # rule nor the cell that produced it. Seen in production as a resumption
        # failing with `Enumerable not implemented for Atom, got :error`, where
        # `:error` is this module's own internal sentinel for a key that does
        # not fit its grain.
        #
        # `:all` rather than a raise, matching what every degradation in
        # `KeyRule` already does: a claim that cannot be narrowed is answered
        # wide, which is correct and expensive, never wrong and cheap. Logged at
        # warning because a rule breaking its contract is a defect even when the
        # fallback is safe — silence here is what made the original crash hard
        # to attribute.
        other ->
          Logger.warning(fn ->
            "reactive_dag: #{inspect(key_rule)} returned #{inspect(other, limit: 5)} for " <>
              "#{child_id} -> #{parent_id}; expected `:all` or `{:keys, keys}`. " <>
              "Claiming the whole cell."
          end)

          {parent_id, ["*"]}
      end
    end)
  end

  # The WIDEST arity the module exports — rule/5 (with the plan's opts), then
  # rule/4 (with diffs), then rule/3. The seam is public and hosts implement it,
  # so widening a callback in place would break them for a feature they have not
  # asked for.
  #
  # `Code.ensure_loaded!/1` first: `function_exported?/3` answers FALSE for a
  # module that is not loaded yet, and under a release or a first call in a fresh
  # process that is exactly the state — which would silently pick a narrower
  # arity and drop the tenant. (Learned the hard way: the same trap made an
  # idempotence test claim 114 phantom changed keys.)
  defp apply_rule(mod, parent, child, keys, diffs, opts) do
    Code.ensure_loaded!(mod)

    cond do
      function_exported?(mod, :rule, 5) -> mod.rule(parent, child, keys, diffs, opts)
      function_exported?(mod, :rule, 4) -> mod.rule(parent, child, keys, diffs)
      true -> mod.rule(parent, child, keys)
    end
  end

  # ---- parents: inverse of each cell's inputs, EXCLUDING context edges ----
  # A cell's `inputs` are every edge — used for validation + depth ordering +
  # reading. But a CONTEXT input (listed in `meta.context_inputs`) is read as
  # settled context, not recomputed on: a change to it must NOT claim this cell
  # (e.g. an expensive/non-deterministic LLM node that consults mutable context
  # data). So the propagation graph (`parents`) omits context edges — the node
  # still reads the current value when it recomputes for other reasons, it just
  # isn't triggered BY that value changing.
  defp build_parents(cells) do
    Enum.reduce(cells, %{}, fn cell, acc ->
      refs = MapSet.new(cell.meta[:context_inputs] || [])

      cell.inputs
      |> Enum.reject(&MapSet.member?(refs, &1))
      |> Enum.reduce(acc, fn input_id, acc2 ->
        Map.update(acc2, input_id, [cell.id], &[cell.id | &1])
      end)
    end)
    |> Map.new(fn {k, v} -> {k, Enum.reverse(v)} end)
  end

  defp validate_inputs!(cells, by_id) do
    for cell <- cells, input <- cell.inputs, not Map.has_key?(by_id, input) do
      raise ArgumentError, "cell #{inspect(cell.id)} references unknown input #{inspect(input)}"
    end

    :ok
  end

  # ---- depths: longest path from a leaf, memoized, with cycle detection ----
  defp build_depths(by_id) do
    Enum.reduce(Map.keys(by_id), %{}, fn id, memo ->
      {depth, memo} = depth_of(id, by_id, memo, MapSet.new())
      Map.put(memo, id, depth)
    end)
  end

  defp depth_of(id, by_id, memo, on_stack) do
    cond do
      Map.has_key?(memo, id) -> {Map.fetch!(memo, id), memo}
      MapSet.member?(on_stack, id) -> raise ArgumentError, "cycle at cell #{inspect(id)}"
      true -> compute_depth(Map.fetch!(by_id, id), id, by_id, memo, on_stack)
    end
  end

  # Field access (cell.inputs), not a %Cell{} match — a host may pass its OWN
  # cell struct (cascade keeps Cascade.Engine.Cell); the graph math only needs
  # `.id` and `.inputs`.
  defp compute_depth(%{inputs: []}, id, _by_id, memo, _on_stack), do: {0, Map.put(memo, id, 0)}

  defp compute_depth(%{inputs: inputs}, id, by_id, memo, on_stack) do
    on_stack = MapSet.put(on_stack, id)

    {max_input_depth, memo} =
      Enum.reduce(inputs, {-1, memo}, fn input_id, {acc_max, acc_memo} ->
        {d, acc_memo} = depth_of(input_id, by_id, acc_memo, on_stack)
        {max(acc_max, d), acc_memo}
      end)

    depth = max_input_depth + 1
    {depth, Map.put(memo, id, depth)}
  end
end
