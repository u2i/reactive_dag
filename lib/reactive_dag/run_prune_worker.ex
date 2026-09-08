if Code.ensure_loaded?(Oban.Worker) do
  defmodule ReactiveDag.RunPruneWorker do
    @moduledoc """
    Delete run-log rows past the retention window.

    `ReactiveDag.Insights` bounds its own in-memory window (`insights_keep`,
    default 20) without a host being told to. This is the same obligation for
    the persistent log: the library owns the table, so it owns keeping it
    bounded. A host that has to schedule its own prune is a host whose table
    grows until someone notices.

    Suspensions need no equivalent — a suspension discharges when its work
    resumes, so that table empties as the graph runs. Runs never discharge:
    every job ever executed leaves a row.

    ## Scheduling it

    Add it to Oban's crontab. Nightly is ample — this deletes by a date, so
    running it more often just does less work each time:

        {Oban.Plugins.Cron, crontab: [{"20 3 * * *", ReactiveDag.RunPruneWorker}]}

    Not scheduled automatically, because the library ships no supervision tree
    and cannot add itself to a host's crontab. `ReactiveDag.Run.prune/2` is
    there for a host that would rather call it from its own housekeeping.

    ## The window

    `config :reactive_dag, run_log_keep_days: 90` (the default). Long enough to
    answer what the log is for — when did this last succeed, how long has that
    been failing, what did last week's release change — and short enough that
    the table stays small. Override per job with `%{"days" => n}`.

    ## What it will not delete

    A row that has not FINISHED, whatever its age. A resumption parked for six
    months is exactly what a status page should still be showing, and sweeping
    it would turn stuck work into no work — silently, and precisely on the rows
    worth surfacing. That rule lives in `Run.prune/2`; this worker only chooses
    the date.
    """
    use Oban.Worker, queue: :default, max_attempts: 3

    require Logger

    @default_days 90

    @impl Oban.Worker
    def perform(%Oban.Job{args: args}) do
      days = args["days"] || days()
      cutoff = DateTime.add(DateTime.utc_now(), -days, :day)

      # EVERY tenant — `prune/2` with no `:tenant` covers them all, which is
      # what a housekeeping job wants. Naming one here would prune a single
      # municipality and leave the rest growing.
      case ReactiveDag.Run.prune(cutoff) do
        0 ->
          :ok

        n ->
          Logger.info(
            "reactive_dag: pruned #{n} run(s) finished before #{DateTime.to_date(cutoff)}"
          )

          :ok
      end
    end

    @doc "The retention window, in days."
    @spec days() :: pos_integer()
    def days, do: Application.get_env(:reactive_dag, :run_log_keep_days, @default_days)
  end
end
