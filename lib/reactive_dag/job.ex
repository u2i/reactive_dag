defmodule ReactiveDag.Job do
  @moduledoc """
  What every library-provided Oban job needs from its arguments.

  A plan is built from resource modules at runtime, so it cannot ride in a job
  argument — Oban serialises args to JSON, and a `%Plan{}` is neither JSON nor
  cheap to rebuild blindly. So a job carries the *name* of the function that
  builds one, and this resolves it.

  Shared rather than duplicated because two workers now need it (`ScanWorker`
  and `ReprocessWorker`), and a divergence between them would be the kind of bug
  that only appears on the job that runs least often.
  """

  @doc """
  The plan for a job: its own `"plan_mfa"` argument, else the configured default.

  Raises with the fix rather than a `MatchError`, because "no plan" is a
  configuration mistake and the message is the only place it can be explained.
  """
  @spec plan(map(), module()) :: ReactiveDag.Plan.t()
  def plan(%{"plan_mfa" => [m, f, a]}, _worker),
    do: apply(module(m), String.to_existing_atom(f), a)

  def plan(_args, worker) do
    case Application.get_env(:reactive_dag, :plan_mfa) do
      {m, f, a} ->
        apply(m, f, a)

      nil ->
        raise """
        reactive_dag: #{inspect(worker)} needs a plan, and a job argument cannot carry \
        one (a plan is built from resource modules at runtime).

        Name it once:

            config :reactive_dag, plan_mfa: {MyApp.Dag, :plan, []}

        or per job:

            %{"plan_mfa" => ["MyApp.Dag", "plan", []]}
        """
    end
  end

  @doc """
  The `recompute:`/`key_rule:` a job drains with, defaulting to the `Node`
  strategies a DSL-authored graph uses.
  """
  @spec drain_opts(map()) :: keyword()
  def drain_opts(args) do
    [
      recompute: module_or(args["recompute"], ReactiveDag.Node.Recompute),
      key_rule: module_or(args["key_rule"], ReactiveDag.Node.KeyRule)
    ]
  end

  @doc """
  Run `fun` inside the host's configured wrapper, if it has one.

      config :reactive_dag, around_poll: {MyApp.Audit, :around, []}

  The named function is called with the job's `args` and a one-arity function,
  and must return whatever that function returns. The argument it passes is a
  keyword list ADDED to the poll's options — which is the point of it: a
  wrapper that starts a collector hands the scanner a way to reach it.

      def around(_args, run) do
        {:ok, pid} = Agent.start_link(fn -> [] end)

        try do
          run.(collector: pid)
        after
          persist(Agent.get(pid, & &1))
          Agent.stop(pid)
        end
      end

  ## Why this exists rather than a telemetry handler

  Almost everything a host wants around a scan is better as a handler on
  `[:reactive_dag, :scan, :stop]`: broadcasts, durable rows, follow-up
  enqueues. Those all happen when the scan is over, and the event carries
  everything they need.

  What a handler cannot do is be present WHILE the poll runs. A host that
  records every HTTP request its crawler makes — an observation log, a
  per-request mirror — needs something live for the duration and needs to hand
  the scanner a way to reach it. That is a process, and a process cannot ride
  in an Oban argument, so it has to be started inside the job.

  Without this, a host with that requirement forks the worker, and then owns
  the poll/mark/drain loop forever to keep one wrapper.

  ## What it must not be used for

  Anything that only needs the RESULT. If the work can read
  `[:reactive_dag, :scan, :stop]` and be done, it belongs there — a wrapper
  makes it run inside the job's failure boundary for no reason.
  """
  @spec around_poll(map(), (keyword() -> result)) :: result when result: term()
  def around_poll(args, fun) do
    case Application.get_env(:reactive_dag, :around_poll) do
      {m, f, a} -> apply(m, f, [args, fun | a])
      # No wrapper: the poll runs with exactly the options it was given.
      nil -> fun.([])
    end
  end

  @doc "A module name from a job argument, with or without the `Elixir.` prefix."
  @spec module(String.t()) :: module()
  def module("Elixir." <> _ = m), do: String.to_existing_atom(m)
  def module(m), do: String.to_existing_atom("Elixir." <> m)

  defp module_or(nil, default), do: default
  defp module_or(name, _default), do: module(name)
end
