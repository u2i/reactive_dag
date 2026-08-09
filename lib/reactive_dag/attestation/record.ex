defmodule ReactiveDag.Attestation.Record do
  @moduledoc """
  An **Ash resource extension** for the attestation record store — the
  Ash-idiomatic storage pattern (as `ash_authentication`'s token resource): the
  HOST defines the resource (choosing repo, table, domain, policies), this
  extension stamps the required shape onto it, and the library reaches it
  through `config :reactive_dag, attestation_resource: MyApp.Attestation.Record`.

      defmodule MyApp.Attestation.Record do
        use Ash.Resource,
          domain: MyApp.Attestations,
          data_layer: AshPostgres.DataLayer,
          extensions: [ReactiveDag.Attestation.Record]

        postgres do
          table "attestation_records"
          repo MyApp.Repo
        end

        attestation_record do
          # OPTIONAL: derive the signer from the Ash actor. When set and an
          # actor is present, `who` is FORCED from it — impersonation becomes
          # structurally impossible at the write, not merely discounted at
          # read time by the eligibility check.
          who_from_actor fn actor -> to_string(actor.email) end
        end
      end

  What the extension stamps (each only if the host hasn't declared it):

    * the record attributes — `cell_id / scope_kind / scope / who / polarity /
      reason / basis / basis_version / signed_at / meta` under a UUID pk;
    * a `:sign` create action accepting them, carrying the change that applies
      `who_from_actor` and rejects a reasonless rejection;
    * a primary `:read`.

  Being the host's resource, everything Ash composes onto it: **policies**
  (signing authorization in the same framework as the rest of the app),
  **notifications** (pub_sub a signing straight into a refresh), and
  **generated migrations** (`mix ash.codegen`) instead of a hand-written
  expected-shape blob.

  ## Append-only is enforced, not conventional

  A verifier REJECTS the resource at compile time if it declares any update or
  destroy action. Records are immutable history — a signer's current stance is
  their latest record, and whether it counts is computed at read time
  (`ReactiveDag.Attestation.Evaluation`); nothing about a record is ever
  edited. The audit trail is the point.
  """

  @attestation_record %Spark.Dsl.Section{
    name: :attestation_record,
    describe: "Host configuration for the attestation record resource.",
    schema: [
      who_from_actor: [
        type: {:fun, 1},
        required: false,
        doc:
          "`(actor -> who | nil)` — when set and an actor is present on the `:sign` " <>
            "action, `who` is FORCED from the actor (a nil return leaves the passed value)."
      ]
    ]
  }

  use Spark.Dsl.Extension,
    sections: [@attestation_record],
    transformers: [__MODULE__.AddStructure],
    verifiers: [__MODULE__.VerifyAppendOnly]

  @doc "The host-configured `who_from_actor` function, or nil."
  def who_from_actor(resource_or_dsl) do
    Spark.Dsl.Extension.get_opt(resource_or_dsl, [:attestation_record], :who_from_actor, nil)
  end

  defmodule SignChange do
    @moduledoc """
    The `:sign` action's change: applies the resource's `who_from_actor` (an
    actor present → `who` forced from it), validates the polarity, and errors
    a `:reject` with a blank `reason` — a rejection asserts the data is WRONG,
    and a bare "no" leaves whoever must act with nothing to fix and an auditor
    with an unexplained refusal. (`:withdraw` — "I no longer vouch" — asserts
    nothing about the data, so its reason stays optional.)
    """
    use Ash.Resource.Change

    @valid_polarities ~w(affirm reject withdraw)

    @impl true
    def change(changeset, _opts, ctx) do
      changeset
      |> force_who(ctx.actor)
      |> validate_polarity()
      |> require_reason_on_reject()
    end

    defp validate_polarity(changeset) do
      p = changeset |> Ash.Changeset.get_attribute(:polarity) |> to_string()

      if p in @valid_polarities do
        changeset
      else
        Ash.Changeset.add_error(changeset,
          field: :polarity,
          message: "must be one of #{Enum.join(@valid_polarities, " | ")}"
        )
      end
    end

    defp force_who(changeset, nil), do: changeset

    defp force_who(changeset, actor) do
      case ReactiveDag.Attestation.Record.who_from_actor(changeset.resource) do
        nil ->
          changeset

        fun ->
          case fun.(actor) do
            who when is_binary(who) ->
              Ash.Changeset.force_change_attribute(changeset, :who, who)

            _ ->
              changeset
          end
      end
    end

    defp require_reason_on_reject(changeset) do
      polarity = Ash.Changeset.get_attribute(changeset, :polarity)
      reason = Ash.Changeset.get_attribute(changeset, :reason)

      if to_string(polarity) == "reject" and (is_nil(reason) or String.trim(reason) == "") do
        Ash.Changeset.add_error(changeset,
          field: :reason,
          message: "a rejection requires a reason — what is wrong with the data?"
        )
      else
        changeset
      end
    end
  end

  defmodule AddStructure do
    @moduledoc false
    use Spark.Dsl.Transformer

    alias Ash.Resource.Builder

    # run before Ash's own transformers so the stamped entities go through the
    # same pipeline as hand-written DSL.
    @impl true
    def before?(_), do: true

    @accepted [
      :cell_id,
      :scope_kind,
      :scope,
      :who,
      :polarity,
      :reason,
      :basis,
      :basis_version,
      :signed_at,
      :meta
    ]

    @impl true
    def transform(dsl) do
      with {:ok, dsl} <-
             Builder.add_new_attribute(dsl, :id, :uuid,
               primary_key?: true,
               allow_nil?: false,
               public?: true,
               default: &Ash.UUID.generate/0
             ),
           {:ok, dsl} <- string_attr(dsl, :cell_id),
           {:ok, dsl} <- string_attr(dsl, :scope_kind),
           {:ok, dsl} <- string_attr(dsl, :scope),
           {:ok, dsl} <- string_attr(dsl, :who),
           {:ok, dsl} <- string_attr(dsl, :polarity),
           {:ok, dsl} <-
             Builder.add_new_attribute(dsl, :reason, :string, allow_nil?: true, public?: true),
           {:ok, dsl} <- string_attr(dsl, :basis),
           {:ok, dsl} <-
             Builder.add_new_attribute(dsl, :basis_version, :integer,
               allow_nil?: false,
               public?: true
             ),
           {:ok, dsl} <-
             Builder.add_new_attribute(dsl, :signed_at, :utc_datetime_usec,
               allow_nil?: false,
               public?: true,
               default: &DateTime.utc_now/0
             ),
           {:ok, dsl} <-
             Builder.add_new_attribute(dsl, :meta, :map, allow_nil?: true, public?: true),
           {:ok, sign_change} <- Builder.build_action_change(SignChange),
           {:ok, dsl} <-
             Builder.add_new_action(dsl, :create, :sign,
               accept: @accepted,
               changes: [sign_change]
             ),
           {:ok, dsl} <- Builder.add_new_action(dsl, :read, :read, primary?: true) do
        {:ok, dsl}
      end
    end

    defp string_attr(dsl, name),
      do: Builder.add_new_attribute(dsl, name, :string, allow_nil?: false, public?: true)
  end

  defmodule VerifyAppendOnly do
    @moduledoc false
    use Spark.Dsl.Verifier

    @impl true
    def verify(dsl) do
      mutating =
        dsl
        |> Ash.Resource.Info.actions()
        |> Enum.filter(&(&1.type in [:update, :destroy]))
        |> Enum.map(& &1.name)

      if mutating == [] do
        :ok
      else
        {:error,
         Spark.Error.DslError.exception(
           module: Spark.Dsl.Verifier.get_persisted(dsl, :module),
           path: [:actions],
           message:
             "attestation records are APPEND-ONLY — a signer's current stance is their " <>
               "latest record, and whether it counts is computed at read time; nothing is " <>
               "ever edited. Remove the #{inspect(mutating)} action(s). To withdraw or " <>
               "change an assertion, sign a NEW record (the history is the audit trail)."
         )}
      end
    end
  end
end
