if Code.ensure_loaded?(Oban.Worker) do
  defmodule ReactiveDag.CascadeWorker do
    @moduledoc """
    The Oban job that propagates ONE change — the entry point for everything a
    host writes.

    A write records what it changed and enqueues this. The job then walks the
    graph from that change, in its own transaction, until the cascade completes
    or reaches something that has to stop.

    ## Why the write does not cascade inline

    The design says a cascade runs in one transaction. It does — its own, not
    the writer's. A host write that transitively touches fourteen cells should
    not hold a user's request open for all of them, and running the walk inside
    the writer's transaction would put that transaction at risk of exactly the
    problem this redesign exists to remove: a long-held connection.

    So the write's transaction contains one INSERT — this job — and commits.
    The consequence, worth stating because it surprises: a host reading a
    derived table immediately after its own write sees the old value. That was
    true of the drain too, and remains true here.

    ## Uniqueness is a 60-second window, NOT `:infinity`

    This is the trap in this file, and it is worth reading before copying the
    config from anywhere else.

    `DrainWorker` used `period: :infinity` with empty args, and that was right
    for it: every drain did identical global work, so a burst of writes wanted
    exactly one pending drain.

    These args name a SPECIFIC change. `:infinity` would mean "the same row
    changing twice, ever, cascades once" — a durable correctness bug, silent,
    and impossible to notice from the outside. Sixty seconds coalesces a burst
    of writes to one row and nothing more.

    Contrast `ReactiveDag.ResumptionWorker`, whose `:infinity` IS right, for a
    reason that only applies there: its args are a stopping point rather than a
    change.
    """
    use Oban.Worker,
      queue: :reactive_dag,
      max_attempts: 3,
      unique: [
        period: 60,
        fields: [:args, :queue, :worker],
        states: :incomplete
      ]


    alias ReactiveDag.{Cascade, Job}

    @doc """
    Enqueue a cascade from one cell's changed keys.

    Safe to call inside a transaction: it is one INSERT, and it commits with the
    write that caused it — so a rolled-back write leaves no cascade, and a
    committed one always leaves exactly one.

    `versions` maps a changed key to the id of the version recording what the
    change did. Omitting it is allowed and costly: a suspension downstream will
    carry `"*"` and resume by recomputing the whole cell.
    """
    @spec enqueue(String.t(), [String.t()], keyword()) ::
            {:ok, Oban.Job.t()} | {:error, term()}
    def enqueue(cell, keys, opts \\ []) do
      %{
        "cell" => to_string(cell),
        "keys" => keys,
        "versions" => Keyword.get(opts, :versions, %{}),
        # NORMALISED HERE, because the two origination paths disagree: a
        # `Source` poll passes the plan's tenant verbatim (already `"*"` when
        # untenanted), while a `dirties_on` write takes it off the changeset and
        # has `nil`. Both mean "not scoped to one tenant", and letting both
        # shapes reach the job would make the args — which are the uniqueness
        # key — differ for identical work.
        "tenant" => ReactiveDag.Suspension.tenant(opts),
        # The gate decision, made at the write because that is the only moment
        # the ACTOR exists. `gated human?:` says a person's own edit propagates
        # immediately; by the time this job runs there is nobody left to ask.
        "skip_gate" => Keyword.get(opts, :skip_gate, false)
      }
      |> then(&if opts[:plan_mfa], do: Map.put(&1, "plan_mfa", opts[:plan_mfa]), else: &1)
      |> __MODULE__.new(Keyword.take(opts, [:schedule_in, :priority, :queue]))
      |> Oban.insert()
    end

    @impl Oban.Worker
    def perform(%Oban.Job{args: args}) do
      plan = Job.plan(args, __MODULE__)

      origin = %{
        cell: Map.fetch!(args, "cell"),
        keys: Map.get(args, "keys", []),
        versions: Map.get(args, "versions", %{})
      }

      opts =
        case Map.get(args, "tenant") do
          nil -> []
          tenant -> [tenant: tenant]
        end

      opts =
        if Map.get(args, "skip_gate", false),
          do: Keyword.put(opts, :skip_gate, Map.fetch!(args, "cell")),
          else: opts

      # Resumptions are scheduled by `Cascade.run/3` itself, not here: a source
      # poll and a reprocess also run cascades directly, and leaving it to each
      # caller meant one of three paths scheduled anything.
      opts =
        case Map.get(args, "plan_mfa") do
          nil -> opts
          mfa -> Keyword.put(opts, :plan_mfa, mfa)
        end

      {:ok, _report} = Cascade.run(plan, [origin], opts)

      :ok
    end
  end
end
