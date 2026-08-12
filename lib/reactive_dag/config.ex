defmodule ReactiveDag.Config do
  @moduledoc """
  Boot-time validation of `config :reactive_dag, …`.

  Misconfiguration otherwise surfaces at the **first query** — a missing
  `:repo` raises on the first drain, which may be a long way into a deploy and
  in whatever process happened to trigger it. Worse, only `:repo` raises at all:
  a `:dirty_table` name that is not a valid identifier fails later and less
  legibly, as a syntax error deep inside a query.

  Call it once at boot:

      def start(_type, _args) do
        ReactiveDag.Config.validate!()
        Supervisor.start_link(children, opts)
      end

  It reports **every** problem it finds, not the first — a config with two
  mistakes should take one deploy to fix, not two.

  ## Why the host calls it

  Deliberately not an `Application` callback of our own. The library starting
  its own app would mean it has a supervision tree and an opinion about when it
  starts, and the check could not be skipped by tests that deliberately run
  unconfigured (this library's own suite has several). An explicit call is
  testable, skippable, and works whether or not `:reactive_dag` is started as an
  application.

  ## What it does not check

  Only what is *definitely* wrong. A validator that warns about the
  suspicious-but-legal gets wrapped in a `try` and stops being read. Nothing
  here touches the database: whether the tables exist
  is `ReactiveDag.Migration`'s business, and a boot check that queries would
  make boot depend on the database being reachable.
  """

  @identifier ~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/

  defmodule Error do
    @moduledoc "Raised by `ReactiveDag.Config.validate!/0` when configuration is wrong."
    defexception [:problems]

    @impl true
    def message(%{problems: problems}) do
      "reactive_dag is misconfigured:\n\n" <> Enum.map_join(problems, "\n", &("  * " <> &1))
    end
  end

  @doc """
  Validate the configuration, raising `ReactiveDag.Config.Error` listing every
  problem. Returns `:ok` when there is nothing wrong.
  """
  @spec validate!() :: :ok
  def validate! do
    case problems() do
      [] -> :ok
      problems -> raise Error, problems: problems
    end
  end

  @doc """
  The problems as a list of strings, for a host that would rather log or
  surface them than raise. `[]` means the configuration is sound.
  """
  @spec problems() :: [String.t()]
  def problems do
    Enum.flat_map(
      [
        &repo/0,
        &table_names/0,
        &insights_keep/0
      ],
      & &1.()
    )
  end

  # ── the checks ──────────────────────────────────────────────────────────────

  defp repo do
    case Application.get_env(:reactive_dag, :repo) do
      nil ->
        ["`:repo` is not set (required) — add `config :reactive_dag, repo: MyApp.Repo`"]

      repo when is_atom(repo) ->
        cond do
          not Code.ensure_loaded?(repo) ->
            ["`:repo` is #{inspect(repo)}, which is not a loadable module"]

          not function_exported?(repo, :query!, 2) ->
            [
              "`:repo` #{inspect(repo)} does not export `query!/2` — the library goes " <>
                "through your repo with raw SQL for the two tables it owns, so it needs " <>
                "an Ecto repo"
            ]

          true ->
            []
        end

      other ->
        ["`:repo` must be a module, got #{inspect(other)}"]
    end
  end

  # the one identifier SQL cannot parameterise, so a typo would otherwise be a
  # syntax error deep inside a query
  defp table_names do
    for {key, default} <- [dirty_table: "reactive_dag_dirty"],
        name = Application.get_env(:reactive_dag, key, default),
        problem = table_problem(key, name),
        do: problem
  end

  defp table_problem(key, name) when is_binary(name) do
    unless name =~ @identifier do
      "`#{inspect(key)}` #{inspect(name)} is not a valid SQL identifier " <>
        "(letters, digits and underscore, not starting with a digit)"
    end
  end

  defp table_problem(key, name), do: "`#{inspect(key)}` must be a string, got #{inspect(name)}"

  defp insights_keep do
    case Application.get_env(:reactive_dag, :insights_keep) do
      nil -> []
      n when is_integer(n) and n > 0 -> []
      other -> ["`:insights_keep` must be a positive integer, got #{inspect(other)}"]
    end
  end

  # ── helpers ─────────────────────────────────────────────────────────────────

end
