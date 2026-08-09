defmodule ReactiveDag.Attestation do
  @moduledoc """
  The attestation RECORD STORE — human assertions about a cell's data, as
  immutable append-only history (the host project's ADR-002).

  A record is `(cell_id, scope, who, polarity, reason, basis, basis_version,
  signed_at)`: someone (`who`) affirmed or rejected what a scope of `cell_id`'s
  data looked like (`basis` — a content digest, `ReactiveDag.Attestation.Basis`)
  at a moment. Records are never updated or deleted; a signer's current STANCE
  on a scope is simply their most recent record, and whether that stance
  currently COUNTS is a read-time predicate
  (`ReactiveDag.Attestation.Evaluation`) — never a stored status. There is no
  `active` flag, no unique index, no `updated_at`, by design: the history is
  the audit trail.

  ## Ownership (the spine pattern)

  As with `tuple_table`: the lib defines the columns and is the only
  reader/writer; the host runs the migration and configures the name —

      config :reactive_dag, attestation_table: "my_attestation"

  The expected shape:

      create table(:reactive_dag_attestation, primary_key: false) do
        add :id,            :uuid, primary_key: true
        add :cell_id,       :string, null: false
        add :scope_kind,    :string, null: false
        add :scope,         :string, null: false
        add :who,           :string, null: false
        add :polarity,      :string, null: false
        add :reason,        :text
        add :basis,         :string, null: false
        add :basis_version, :smallint, null: false
        add :signed_at,     :utc_datetime_usec, null: false
        add :meta,          :jsonb
      end

      create index(:reactive_dag_attestation,
        [:cell_id, :scope_kind, :scope, :who, :signed_at])

  ## The leaf cell

  The store surfaces in a graph as ONE leaf cell (`leaf_cell/0`, default
  `"attestations"`, configurable via `:attestation_cell`) — an implicit input
  of every attested cell, so signing propagates exactly as a scan finishing
  does: leaf write → dirty → drain. `affirm/4` / `reject/5` return the
  serialized scope as the changed leaf key for the host's refresh call.
  """

  alias ReactiveDag.Attestation.{Basis, Scope}

  @default_table "reactive_dag_attestation"
  @default_cell "attestations"

  @type polarity :: :affirm | :reject

  @type record :: %{
          id: String.t(),
          cell_id: String.t(),
          scope: Scope.t(),
          who: String.t(),
          polarity: polarity(),
          reason: String.t() | nil,
          basis: String.t(),
          basis_version: pos_integer(),
          signed_at: DateTime.t()
        }

  @doc "The id of the store's leaf cell (config `:attestation_cell`)."
  @spec leaf_cell() :: String.t()
  def leaf_cell, do: Application.get_env(:reactive_dag, :attestation_cell, @default_cell)

  @doc """
  Record that `who` AFFIRMS `scope` of `cell_id` as it currently stands. The
  basis is digested from the rows the scope selects right now (pass `rows:` to
  supply them, e.g. in a transaction that just read them). `reason:` is
  optional on an affirmation.

  Returns `{:ok, record, changed_leaf_keys}` — the changed keys are for marking
  the store's leaf cell dirty (`leaf_cell/0`).
  """
  @spec affirm(String.t(), Scope.t(), String.t(), keyword()) ::
          {:ok, record(), [String.t()]}
  def affirm(cell_id, scope, who, opts \\ []) do
    insert(cell_id, scope, who, :affirm, Keyword.get(opts, :reason), opts)
  end

  @doc """
  Record that `who` REJECTS `scope` of `cell_id` as it currently stands.
  `reason` is REQUIRED: an affirmation asserts the data as presented, but a
  rejection asserts it is *wrong* — a bare "no" leaves whoever must act with
  nothing to fix and an auditor with an unexplained refusal.
  """
  @spec reject(String.t(), Scope.t(), String.t(), String.t(), keyword()) ::
          {:ok, record(), [String.t()]}
  def reject(cell_id, scope, who, reason, opts \\ []) do
    if !is_binary(reason) or String.trim(reason) == "" do
      raise ArgumentError,
            "attestation: a rejection requires a reason — what is wrong with the data?"
    end

    insert(cell_id, scope, who, :reject, reason, opts)
  end

  @doc """
  Every signer's current STANCE on `cell_id`'s scopes: the most recent record
  per `(scope, who)`. This is the store's read for evaluation — history stays
  in the table; only the latest word per signer per scope has force.
  """
  @spec stances(String.t()) :: [record()]
  def stances(cell_id) do
    %{rows: rows} =
      query!(
        """
        SELECT DISTINCT ON (scope_kind, scope, who)
               id, cell_id, scope_kind, scope, who, polarity, reason,
               basis, basis_version, signed_at
        FROM #{table()}
        WHERE cell_id = $1
        ORDER BY scope_kind, scope, who, signed_at DESC
        """,
        [cell_id]
      )

    Enum.map(rows, &to_record/1)
  end

  @doc "Full append-only history for `cell_id` (optionally one scope), newest first."
  @spec history(String.t(), keyword()) :: [record()]
  def history(cell_id, opts \\ []) do
    {scope_sql, params} =
      case Keyword.get(opts, :scope) do
        nil -> {"", [cell_id]}
        scope -> {" AND scope = $2", [cell_id, Scope.serialize(scope)]}
      end

    %{rows: rows} =
      query!(
        """
        SELECT id, cell_id, scope_kind, scope, who, polarity, reason,
               basis, basis_version, signed_at
        FROM #{table()}
        WHERE cell_id = $1#{scope_sql}
        ORDER BY signed_at DESC
        """,
        params
      )

    Enum.map(rows, &to_record/1)
  end

  # ── internals ───────────────────────────────────────────────────────────────

  defp insert(cell_id, scope, who, polarity, reason, opts) do
    rows = Keyword.get_lazy(opts, :rows, fn -> Scope.select_db(scope, cell_id) end)
    version = Basis.current_version()
    basis = Basis.digest(rows, version)
    signed_at = Keyword.get(opts, :signed_at, DateTime.utc_now())
    id = Ecto.UUID.generate()
    {scope_kind, scope_text} = {kind(scope), Scope.serialize(scope)}

    query!(
      """
      INSERT INTO #{table()}
        (id, cell_id, scope_kind, scope, who, polarity, reason,
         basis, basis_version, signed_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      """,
      [
        Ecto.UUID.dump!(id),
        cell_id,
        scope_kind,
        scope_text,
        who,
        Atom.to_string(polarity),
        reason,
        basis,
        version,
        signed_at
      ]
    )

    record = %{
      id: id,
      cell_id: cell_id,
      scope: scope,
      who: who,
      polarity: polarity,
      reason: reason,
      basis: basis,
      basis_version: version,
      signed_at: signed_at
    }

    {:ok, record, [scope_text]}
  end

  defp kind({:key, _}), do: "key"
  defp kind({:filter, _}), do: "filter"

  defp to_record([id, cell_id, _kind, scope, who, polarity, reason, basis, version, signed_at]) do
    %{
      id: load_uuid(id),
      cell_id: cell_id,
      scope: Scope.parse(scope),
      who: who,
      polarity: String.to_existing_atom(polarity),
      reason: reason,
      basis: basis,
      basis_version: version,
      signed_at: signed_at
    }
  end

  defp load_uuid(<<_::128>> = raw), do: Ecto.UUID.load!(raw)
  defp load_uuid(id), do: id

  defp query!(sql, params), do: repo().query!(sql, params)

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end

  defp table, do: Application.get_env(:reactive_dag, :attestation_table, @default_table)
end
