defmodule ReactiveDag.Node.Changes.MarkDirty do
  @moduledoc """
  ORIGINATES A CASCADE from a written record, so what depends on it recomputes.
  Wired automatically by `dirties_on` and `augmented_by` — hosts do not add it.

  ## Why a change, not a notifier

  The obvious tool is an `Ash.Notifier`, and it is the wrong one: Ash dispatches
  notifications **after** the transaction commits. A cascade enqueued there is
  not covered by the write's transaction, so a crash between commit and dispatch
  loses it — and a lost cascade is silent staleness, the failure mode this
  feature exists to remove.

  An `after_action` hook runs **inside** the transaction (its counterpart is
  named `before_transaction` precisely because that one does not). The enqueue
  therefore joins the host's transaction:

    * a rolled-back write enqueues **nothing**, and
    * a committed write **always** enqueues exactly one cascade.

  Both halves matter. Neither is true of a notifier.

  ## The cascade is the JOB'S transaction, not the writer's

  What commits with the write is one INSERT — an Oban job naming what changed.
  The walk itself happens later, in its own transaction.

  That is deliberate. A write that transitively touches fourteen cells should
  not hold a user's request open for all of them, and running the walk inline
  would put the host's transaction at risk of exactly the problem this design
  removes: a long-held connection. The visible consequence is that a host
  reading a derived table immediately after its own write sees the old value —
  which was equally true of the drain that preceded this.

  ## Cost

  One `INSERT` per write, inside a transaction the host already holds open.
  Oban's uniqueness collapses a burst of writes to the same row into one
  pending cascade. A host writing at a rate where even that contends should
  feed the leaf from a `ReactiveDag.Source` poll instead — see the sources
  guide.
  """
  use Ash.Resource.Change
  require Logger

  @impl true
  def change(changeset, opts, context) do
    Ash.Changeset.after_action(changeset, fn cs, result ->
      originate(result, prior_of(cs, result), opts, cs.tenant, Map.get(context, :actor), cs)
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

  # The enqueue, inside the write's transaction. This is now the WHOLE act:
  # under the queue there were two steps — write a mark, then optionally
  # schedule something to consume it — and a host that forgot the second got
  # durable staleness. There is nothing to forget here, so `schedule_drain` is
  # gone as an option: originating IS enqueuing.
  #
  # A FAILURE HERE LOSES THE CASCADE, and that is a real change from the mark it
  # replaces. A mark was durable on its own, so a failed enqueue only delayed
  # things; a failed enqueue now means nothing recomputes until the row is
  # written again.
  #
  # It still does not raise. A host whose Oban is unavailable should not have
  # its writes fail — that trades a recoverable staleness for an unrecoverable
  # rejection, and the staleness is visible in the log while a rejected write is
  # the user's problem.
  defp enqueue(cell, keys, opts) do
    # RESCUED, not just matched on `{:error, _}`. `Oban.insert/3` RAISES when no
    # instance is running rather than returning an error tuple — so a promise
    # not to fail the host's write has to cover the raising path too, or the
    # promise is only true when the queue is healthy, which is exactly when it
    # does not matter.
    case safe_enqueue(cell, keys, opts) do
      {:ok, _job} ->
        :ok

      {:error, reason} ->
        require Logger

        Logger.warning(
          "reactive_dag: wrote #{inspect(cell)} but could not enqueue its cascade — " <>
            "#{inspect(reason)}. NOTHING downstream will recompute until this row " <>
            "is written again or a scan re-observes it."
        )

        :ok
    end
  end

  defp safe_enqueue(cell, keys, opts) do
    enqueuer().(cell, keys, opts)
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # Overridable so a host can queue the cascade its own way — a different queue,
  # a debounce, its own worker wrapping `Cascade.run/3` for run-id bookkeeping —
  # and so a test can observe the enqueue without standing up Oban and Postgres.
  #
  #     config :reactive_dag, cascade_enqueuer: fn cell, keys, opts -> ... end
  #
  # Must return `{:ok, term}` or `{:error, term}`.
  defp enqueuer do
    Application.get_env(:reactive_dag, :cascade_enqueuer) ||
      &ReactiveDag.CascadeWorker.enqueue/3
  end

  defp originate(record, _prior, opts, tenant, actor, changeset) do
    cell = Keyword.fetch!(opts, :cell)

    # The tenant off the CHANGESET, which is where a tenanted write already put
    # it — there is no plan in scope here, and a cascade that omitted it would
    # walk a graph scoped to the wrong tenant. A read scoped to the wrong tenant
    # returns nothing and REPORTS SUCCESS, so the write would look handled while
    # nothing recomputed.
    {keys, versions} =
      case key_of(record, opts) do
        nil ->
          # A record with no derivable key cannot be named downstream.
          # Escalating to the whole cell is the honest fallback: correct, and
          # visible in the report rather than silently missing.
          {["*"], %{}}

        key ->
          # WHICH entity changed, and a reference to WHAT the change did. The
          # version holds the diff; the cascade carries only the reference, and
          # resolves it when a consumer needs to narrow.
          case version_id(opts[:version_id], record, changeset) do
            nil -> {[key], %{}}
            vid -> {[key], %{key => vid}}
          end
      end

    enqueue(cell, keys,
      tenant: tenant,
      versions: versions,
      skip_gate: gate_cleared?(opts, actor)
    )
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

  # Does this change need a human, given who made it?
  #
  # `gated true` gates every change through the cell. `gated human?: {M,F,A}`
  # gates only the MACHINE ones: the host's predicate is called with the write's
  # ACTOR, and a change a person made propagates immediately — nobody should
  # queue for approval of their own edit.
  #
  # The library cannot tell a person from a service account (a host's LLM calls
  # may well run as one), so the host says. A nil actor with a predicate declared
  # is a machine: nothing claimed to be a person.
  #
  # ANSWERED HERE, not in the cascade, because the actor exists only at the
  # write. By the time a cascade runs — in a job, minutes later — there is
  # nobody to ask. So the answer rides along as `skip_gate:` and the cascade
  # honours it.
  defp gate_cleared?(opts, actor) do
    case Keyword.get(opts, :gated, false) do
      false -> false
      nil -> false
      true -> false
      gated when is_list(gated) -> human_write?(Keyword.get(gated, :human?), actor)
    end
  end

  defp human_write?({m, f, a}, actor), do: apply(m, f, [actor | a])
  defp human_write?(nil, _actor), do: false



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
