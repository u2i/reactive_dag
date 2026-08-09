defmodule ReactiveDag.Attestation do
  @moduledoc """
  The attestation RECORD STORE — human assertions about a cell's data, as
  immutable append-only history (the host project's ADR-002).

  A record is `(cell_id, scope, who, polarity, reason, basis, basis_version,
  signed_at)`: someone (`who`) affirmed, rejected, or withdrew their word on
  what a scope of `cell_id`'s data looked like (`basis` — a content digest, `ReactiveDag.Attestation.Basis`)
  at a moment. Records are never updated or deleted; a signer's current STANCE
  on a scope is simply their most recent record, and whether that stance
  currently COUNTS is a read-time predicate
  (`ReactiveDag.Attestation.Evaluation`) — never a stored status.

  ## Storage: a host-defined Ash resource

  Records live in an **Ash resource the host defines** and this module reaches
  via config — the Ash-idiomatic library-storage pattern. The
  `ReactiveDag.Attestation.Record` extension stamps the required shape
  (attributes, the `:sign` create, a primary read) and ENFORCES append-only
  (an update/destroy action fails compilation); the host chooses repo, table,
  domain — and composes policies, notifications, and generated migrations onto
  it like any other resource.

      config :reactive_dag, attestation_resource: MyApp.Attestation.Record

  Writes pass `actor:` through to Ash, so a resource configured with
  `who_from_actor` derives the signer from the actor — impersonation prevented
  at the write, not merely discounted at read time. Internal reads
  (`stances/1`, `history/2`) run with `authorize?: false`: they are the
  system's own evaluation input, not a user-facing query.

  ## The leaf cell

  The store surfaces in a graph as ONE leaf cell (`leaf_cell/0`, default
  `"attestations"`, configurable via `:attestation_cell`) — an implicit input
  of every attested cell, so signing propagates exactly as a scan finishing
  does: leaf write → dirty → drain. `affirm/4` / `reject/5` return the
  serialized scope as the changed leaf key for the host's refresh call.
  """

  require Ash.Query

  alias ReactiveDag.Attestation.{Basis, Scope}

  @default_cell "attestations"

  @type polarity :: :affirm | :reject | :withdraw

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
  optional on an affirmation. `actor:` is passed through to Ash — with a
  `who_from_actor` on the record resource, the signer is derived from it.

  Returns `{:ok, record, changed_leaf_keys}` — the changed keys are for marking
  the store's leaf cell dirty (`leaf_cell/0`).
  """
  @spec affirm(String.t(), Scope.t(), String.t(), keyword()) ::
          {:ok, record(), [String.t()]}
  def affirm(cell_id, scope, who, opts \\ []) do
    sign(cell_id, scope, who, :affirm, Keyword.get(opts, :reason), opts)
  end

  @doc """
  Record that `who` REJECTS `scope` of `cell_id` as it currently stands.
  `reason` is REQUIRED: an affirmation asserts the data as presented, but a
  rejection asserts it is *wrong* — a bare "no" leaves whoever must act with
  nothing to fix and an auditor with an unexplained refusal. (The resource's
  `:sign` action enforces the same rule for writes that bypass this module.)
  """
  @spec reject(String.t(), Scope.t(), String.t(), String.t(), keyword()) ::
          {:ok, record(), [String.t()]}
  def reject(cell_id, scope, who, reason, opts \\ []) do
    if !is_binary(reason) or String.trim(reason) == "" do
      raise ArgumentError,
            "attestation: a rejection requires a reason — what is wrong with the data?"
    end

    sign(cell_id, scope, who, :reject, reason, opts)
  end

  @doc """
  Record that `who` WITHDRAWS their word on `scope` — "I no longer vouch",
  which is a different act from rejecting ("the data is wrong"). A withdrawal
  supersedes the signer's previous stance and itself carries NO force in
  either direction: the scope returns to unaffirmed (pending / re-askable),
  never to refused. `reason:` is optional — withdrawing asserts nothing about
  the data, so there is nothing that must be explained.
  """
  @spec withdraw(String.t(), Scope.t(), String.t(), keyword()) ::
          {:ok, record(), [String.t()]}
  def withdraw(cell_id, scope, who, opts \\ []) do
    sign(cell_id, scope, who, :withdraw, Keyword.get(opts, :reason), opts)
  end

  @doc """
  Every signer's current STANCE on `cell_id`'s scopes: the most recent record
  per `(scope, who)`. This is the store's read for evaluation — history stays
  in the table; only the latest word per signer per scope has force.
  """
  @spec stances(String.t()) :: [record()]
  def stances(cell_id) do
    resource()
    |> Ash.Query.filter(cell_id == ^cell_id)
    |> Ash.read!(authorize?: false)
    |> Enum.group_by(&{&1.scope_kind, &1.scope, &1.who})
    |> Enum.map(fn {_key, records} ->
      records |> Enum.max_by(& &1.signed_at, DateTime) |> to_record()
    end)
    |> Enum.sort_by(&{Scope.serialize(&1.scope), &1.who})
  end

  @doc "Full append-only history for `cell_id` (optionally one scope), newest first."
  @spec history(String.t(), keyword()) :: [record()]
  def history(cell_id, opts \\ []) do
    query =
      case Keyword.get(opts, :scope) do
        nil ->
          Ash.Query.filter(resource(), cell_id == ^cell_id)

        scope ->
          scope_text = Scope.serialize(scope)
          Ash.Query.filter(resource(), cell_id == ^cell_id and scope == ^scope_text)
      end

    query
    |> Ash.Query.sort(signed_at: :desc)
    |> Ash.read!(authorize?: false)
    |> Enum.map(&to_record/1)
  end

  # ── internals ───────────────────────────────────────────────────────────────

  defp sign(cell_id, scope, who, polarity, reason, opts) do
    rows = Keyword.get_lazy(opts, :rows, fn -> Scope.select_db(scope, cell_id) end)
    version = Basis.current_version()
    scope_text = Scope.serialize(scope)

    attrs = %{
      cell_id: cell_id,
      scope_kind: kind(scope),
      scope: scope_text,
      who: who,
      polarity: Atom.to_string(polarity),
      reason: reason,
      basis: Basis.digest(rows, version),
      basis_version: version,
      signed_at: Keyword.get(opts, :signed_at, DateTime.utc_now())
    }

    created =
      resource()
      |> Ash.Changeset.for_create(
        :sign,
        attrs,
        Keyword.take(opts, [:actor, :tenant, :authorize?])
      )
      |> Ash.create!()

    {:ok, to_record(created), [scope_text]}
  end

  defp kind({:key, _}), do: "key"
  defp kind({:filter, _}), do: "filter"

  defp to_record(struct) do
    %{
      id: to_string(struct.id),
      cell_id: struct.cell_id,
      scope: Scope.parse(struct.scope),
      who: struct.who,
      polarity: String.to_existing_atom(struct.polarity),
      reason: struct.reason,
      basis: struct.basis,
      basis_version: struct.basis_version,
      signed_at: struct.signed_at
    }
  end

  defp resource do
    Application.get_env(:reactive_dag, :attestation_resource) ||
      raise """
      reactive_dag: no attestation resource configured. Define one —

          defmodule MyApp.Attestation.Record do
            use Ash.Resource,
              domain: MyApp.Attestations,
              data_layer: AshPostgres.DataLayer,
              extensions: [ReactiveDag.Attestation.Record]

            postgres do
              table "attestation_records"
              repo MyApp.Repo
            end
          end

      — and point the library at it:

          config :reactive_dag, attestation_resource: MyApp.Attestation.Record
      """
  end
end
