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

    # Depths FIRST: `validate_feedback!` walks the graph with feedback edges
    # excluded, and `build_depths` raising on an undeclared cycle is what
    # guarantees that walk terminates.
    depths = build_depths(by_id)
    validate_feedback!(cells, by_id)

    # `tenant` explicitly, though it equals the struct default. Omitting it makes
    # the INFERRED type of this expression narrower than `%Plan{}` — Elixir's
    # type checker reports the omitted field as absent rather than defaulted — so
    # a host with `def f(plan \\ build(...))` and a `%Plan{}` spec got an
    # "incompatible types" warning it could do nothing about.
    %Plan{
      cells: by_id,
      parents: build_parents(cells),
      depths: depths,
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
  # A cell's `inputs` are every edge — used for validation + scheduling +
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
  #
  # A LAYERING, for humans and for staging — the dashboard's levels, the rerun
  # page's stage list, the poll order over leaves. NOT the scheduling invariant:
  # the cascade pops a cell with no pending upstream, and depth is only one
  # witness to that (it strictly increases along every non-feedback edge, so an
  # ancestor is always shallower). Saying it in depth's terms over-claimed: on a
  # real 35-cell plan, 472 of 595 cell pairs are genuinely incomparable and depth
  # imposed an order on 388 of them. See `Cascade.shallowest/2`.
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

  defp compute_depth(%{inputs: inputs} = cell, id, by_id, memo, on_stack) do
    on_stack = MapSet.put(on_stack, id)
    feedback = feedback_inputs(cell)

    {max_input_depth, memo} =
      Enum.reduce(inputs, {-1, memo}, fn input_id, {acc_max, acc_memo} ->
        # A FEEDBACK edge does not order this cell. The edge is real — it
        # propagates, it is read — but it points at something derived from this
        # cell, so counting it would make the cell's depth depend on its own
        # depth: the cycle `depth_of` exists to refuse. Declaring the edge
        # `feedback` is the author saying "the loop is real in the graph and
        # never real in time", and this skip is the entire structural meaning
        # of that declaration. An UNDECLARED back-edge still recurses and hits
        # the `on_stack` guard, so only named loops assemble.
        if MapSet.member?(feedback, input_id) do
          {acc_max, acc_memo}
        else
          {d, acc_memo} = depth_of(input_id, by_id, acc_memo, on_stack)
          {max(acc_max, d), acc_memo}
        end
      end)

    depth = max_input_depth + 1
    {depth, Map.put(memo, id, depth)}
  end

  # The declared back-edges, as a set of input ids. A pattern match rather than
  # `cell.meta[:feedback_inputs]`, because the contract above `compute_depth/5`
  # holds: a host may pass its OWN cell struct, and the graph math must need
  # only `.id` and `.inputs` of it — a struct without `meta` (or with a non-map
  # one) reads as "no feedback edges" instead of crashing.
  defp feedback_inputs(%{meta: %{feedback_inputs: feedback}}) when is_list(feedback),
    do: MapSet.new(feedback)

  defp feedback_inputs(_cell), do: MapSet.new()

  # ---- feedback edges: each must close a REAL cycle ----
  #
  # A feedback declaration weakens the acyclicity guarantee for exactly one
  # edge, so it must be spent on an actual loop. Accepting a spurious one would
  # leave an input silently unordered: the cell could run BEFORE the input it
  # reads, and nothing anywhere would report it — the read just returns stale
  # or empty rows, which is the failure mode this codebase least tolerates.
  #
  # "Real cycle" means the feedback target is transitively DERIVED from the
  # declaring cell through ordinary (non-feedback) edges — the loop exists in
  # the graph and only the declared edge closes it. The walk excludes every
  # feedback edge, not just the one under test, so it runs on the same graph
  # `build_depths` just proved acyclic and cannot orbit; the visited set is
  # kept anyway, because this function must terminate even if that ordering
  # is ever broken by a refactor.
  defp validate_feedback!(cells, by_id) do
    for cell <- cells,
        feedback = feedback_inputs(cell),
        MapSet.size(feedback) > 0,
        input <- feedback do
      cond do
        input not in cell.inputs ->
          # Only reachable on a HAND-BUILT cell whose meta drifted from its
          # inputs — the DSL emits both from one entity. Loud, because a
          # feedback declaration naming a non-input would otherwise be inert
          # and read as protection it isn't providing.
          raise ArgumentError,
                "cell #{inspect(cell.id)} declares feedback on #{inspect(input)}, " <>
                  "which is not one of its inputs #{inspect(cell.inputs)} — a feedback " <>
                  "edge IS an input edge; declare it as one"

        derived_from?(input, cell.id, by_id) ->
          :ok

        true ->
          raise ArgumentError,
                "cell #{inspect(cell.id)} declares feedback on #{inspect(input)}, but " <>
                  "#{inspect(input)} is not derived from #{inspect(cell.id)} — the edge " <>
                  "closes no cycle. A feedback edge exists to name a back-edge the depth " <>
                  "ordering must skip; on an acyclic edge it would leave the input " <>
                  "UNORDERED, so this cell could run before #{inspect(input)} settles, " <>
                  "silently reading stale rows. Declare it `ref`/`depends_on` (recompute " <>
                  "on change) or `context` (read without recomputing) instead."
      end
    end

    :ok
  end

  # Is `target` transitively derived from `source` through non-feedback edges?
  # An iterative walk with a shared seen-set rather than a recursion — the
  # closure can be wide, and one path's visits must count for the others.
  defp derived_from?(target, source, by_id) do
    walk_derivation([target], MapSet.new([target]), source, by_id)
  end

  defp walk_derivation([], _seen, _source, _by_id), do: false
  defp walk_derivation([source | _], _seen, source, _by_id), do: true

  defp walk_derivation([id | rest], seen, source, by_id) do
    next =
      case Map.get(by_id, id) do
        nil ->
          []

        cell ->
          feedback = feedback_inputs(cell)

          Enum.reject(
            cell.inputs,
            &(MapSet.member?(feedback, &1) or MapSet.member?(seen, &1))
          )
      end

    walk_derivation(next ++ rest, MapSet.union(seen, MapSet.new(next)), source, by_id)
  end
end
