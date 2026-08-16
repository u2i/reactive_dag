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

  @doc "A module name from a job argument, with or without the `Elixir.` prefix."
  @spec module(String.t()) :: module()
  def module("Elixir." <> _ = m), do: String.to_existing_atom(m)
  def module(m), do: String.to_existing_atom("Elixir." <> m)

  defp module_or(nil, default), do: default
  defp module_or(name, _default), do: module(name)
end
