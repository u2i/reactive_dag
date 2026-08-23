defmodule ReactiveDag.Node.Changes.MarkDirty do
  @moduledoc """
  Marks a written record's key dirty on its own cell, so the next drain picks
  the change up. Wired automatically by `dirties_on` — hosts do not add it.

  ## Why a change, not a notifier

  The obvious tool is an `Ash.Notifier`, and it is the wrong one: Ash dispatches
  notifications **after** the transaction commits. A mark written there is not
  covered by the write's transaction, so a crash between commit and dispatch
  loses it — and a lost mark is silent staleness, the failure mode this feature
  exists to remove.

  An `after_action` hook runs **inside** the transaction (its counterpart is
  named `before_transaction` precisely because that one does not). The dirty
  write therefore joins the host's transaction through the same repo:

    * a rolled-back write leaves **no** dirty key, and
    * a committed write **always** leaves one.

  Both halves matter. Neither is true of a notifier.

  ## Cost

  The mark is one small `INSERT … ON CONFLICT DO NOTHING` per write, inside a
  transaction the host already holds open. A host writing at a rate where that
  contends should feed the leaf from a `ReactiveDag.Source` poll instead — see
  the sources guide.
  """
  use Ash.Resource.Change
  require Logger

  @impl true
  def change(changeset, opts, context) do
    Ash.Changeset.after_action(changeset, fn cs, result ->
      mark(result, prior_of(cs, result), opts, cs.tenant, Map.get(context, :actor), cs)
      maybe_schedule_drain(opts)
      {:ok, result}
    end)
  end

  # a destroy still marks: the row is gone but its key is exactly what
  # downstream needs to reprice (a `:group` claim then degrades to whole-cell,
  # since the lookup can no longer resolve it — correct, just coarse).
  @impl true
  def atomic(changeset, opts, context), do: {:ok, change(changeset, opts, context)}

  # The row AS IT WAS. `after_action` runs after the change is applied, so
  # `result` is the NEW row — for an update that moved between units, that names
  # where it went, which the live lookup already knows. The prior state is on
  # the changeset (`data`), and it is the only thing that names where it came
  # FROM.
  #
  # A create has no prior; an action that did not load the original gets
  # %OriginalDataNotAvailable{}. Both fall back to the result, which is correct
  # for a create and no worse than today for the rest.
  defp prior_of(%Ash.Changeset{data: %Ash.Changeset.OriginalDataNotAvailable{}}, result), do: result

  # A create has NO prior, and `nil` is how the diff says so — every attribute
  # reads `%{"to" => v}`, which is what "nothing existed before" looks like. This
  # returned `result` while the mark carried a one-sided snapshot, where the new
  # row was the only sensible fallback; against a diff it would claim every
  # attribute `unchanged`, which says the opposite.
  defp prior_of(%Ash.Changeset{action_type: :create}, _result), do: nil
  defp prior_of(%Ash.Changeset{data: %{__struct__: _} = data}, _result), do: data
  defp prior_of(_changeset, result), do: result

  # `schedule_drain: true` — enqueue the drain HERE, inside the same transaction
  # as the mark. Both commit or neither does: a rolled-back write leaves no dirty
  # key AND no job to consume one.
  #
  # The insert is cheap; the DRAIN is not, and it runs later out of the request.
  # Oban's uniqueness turns a burst of writes into one pending job, so a bulk
  # import does not enqueue thousands.
  #
  # Deliberately not raising on failure: the mark is already written and durable,
  # so the worst case is the staleness this option removes rather than a lost
  # write. A host whose Oban is down should not have its writes fail.
  defp maybe_schedule_drain(opts) do
    if Keyword.get(opts, :schedule_drain, false) do
      case enqueuer().() do
        {:ok, _job} ->
          :ok

        {:error, reason} ->
          require Logger

          Logger.warning(
            "reactive_dag: marked dirty but could not enqueue a drain — " <>
              "#{inspect(reason)}. The mark is durable; whatever drains next will " <>
              "pick it up."
          )

          :ok
      end
    end
  end

  # Overridable so a host can queue the drain its own way — a different queue, a
  # debounce, its own worker wrapping `Drain.run/2` for run-id bookkeeping — and
  # so a test can observe the enqueue without standing up Oban and Postgres.
  #
  #     config :reactive_dag, drain_enqueuer: fn -> MyApp.DrainJob.enqueue() end
  #
  # Must return `{:ok, term}` or `{:error, term}`.
  defp enqueuer do
    Application.get_env(:reactive_dag, :drain_enqueuer) ||
      fn -> ReactiveDag.DrainWorker.enqueue() end
  end

  defp mark(record, _prior, opts, tenant, actor, changeset) do
    cell = Keyword.fetch!(opts, :cell)

    # The tenant off the CHANGESET, which is where a tenanted write already put
    # it — there is no plan in scope here, and a `dirties_on` mark that omitted
    # it would name work that tenant's drain never reads. A drain finding nothing
    # reports success, so the write would look handled and nothing would
    # recompute.
    frontier =
      (if tenant, do: [tenant: tenant], else: [])
      |> Keyword.put(:awaiting_approval, hold?(opts[:gated], actor))

    case key_of(record, opts) do
      nil ->
        # A record with no derivable key cannot be named downstream. Escalating
        # to a whole-cell claim is the honest fallback: correct, and loud in the
        # Report rather than silently missing.
        ReactiveDag.Frontier.mark_dirty(cell, ["*"], "written (no key)", frontier)

      key ->
        # WHICH entity changed, and a reference to WHAT the change did. The
        # version holds the diff; the queue row does not copy it.
        entry = {key, version_id(opts[:version_id], record, changeset)}
        ReactiveDag.Frontier.mark_dirty(cell, [entry], "written", frontier)
    end
  end

  # The id of the version recording this change, from the host's own resolver.
  # Nil when the node declares none — the mark then carries the diff only, which
  # is all propagation needs; what is missing is the durable record.
  #
  # A raise here would fail the host's WRITE over a bookkeeping lookup, so a
  # resolver that blows up costs the record and nothing else. Logged, because a
  # silently absent version is exactly the thing an audit trail cannot afford.
  defp version_id(nil, _record, _changeset), do: nil

  defp version_id(resolver, record, changeset) do
    case resolver do
      {m, f, a} -> apply(m, f, [record, changeset | a])
      fun when is_function(fun, 2) -> fun.(record, changeset)
    end
  rescue
    e ->
      Logger.warning(
        "reactive_dag: version_id resolver failed (#{Exception.message(e)}); " <>
          "the mark carries its diff but no durable record"
      )

      nil
  end

  # Does this change wait for a human?
  #
  # `gated true` holds every change through the cell. `gated human?: {M,F,A}`
  # holds only the MACHINE ones: the host's predicate is called with the write's
  # ACTOR, and a change a person made propagates immediately — nobody should
  # queue for approval of their own edit.
  #
  # The library cannot tell a person from a service account (a host's LLM calls
  # may well run as one), so the host says. A nil actor with a predicate declared
  # is a machine: nothing claimed to be a person.
  defp hold?(nil, _actor), do: false
  defp hold?(false, _actor), do: false
  defp hold?(true, _actor), do: true

  defp hold?(gated, actor) when is_list(gated) do
    case Keyword.get(gated, :human?) do
      {m, f, a} -> not apply(m, f, [actor | a])
      nil -> true
    end
  end



  # the same derivation the payload loop uses: identity fields serialized in
  # primary-key order, else the single payload key attribute.
  defp key_of(record, opts) do
    case Keyword.get(opts, :identity_fields) do
      fields when is_list(fields) and fields != [] ->
        if Enum.all?(fields, &(not is_nil(Map.get(record, &1)))) do
          Enum.map_join(fields, "|", &to_string(Map.get(record, &1)))
        end

      _ ->
        case Map.get(record, Keyword.fetch!(opts, :payload_key)) do
          nil -> nil
          value -> to_string(value)
        end
    end
  end
end
