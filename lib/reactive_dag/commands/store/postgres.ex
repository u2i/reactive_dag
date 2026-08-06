defmodule ReactiveDag.Commands.Store.Postgres do
  @moduledoc """
  The default `ReactiveDag.Commands.Store` — a `seq`-ordered Postgres table drained
  with `FOR UPDATE SKIP LOCKED`, coalescing open intents by `dedup_key`, and
  freezing a scope while it has a blocked/failed command. See `ReactiveDag.Commands`
  for the required columns; table name from `config :reactive_dag, commands_table:`
  (default `"commands"`), repo from `config :reactive_dag, repo:`.
  """
  @behaviour ReactiveDag.Commands.Store

  @impl true
  def enqueue(attrs) do
    vals =
      [:kind, :scope, :payload, :dedup_key, :actor, :answers_id]
      |> Enum.map(&Map.get(attrs, &1))
      |> Enum.map(fn v -> if is_map(v), do: Jason.encode!(v), else: v end)

    repo().query!(
      """
      INSERT INTO #{table()} (kind, scope, payload, dedup_key, actor, answers_id, status, inserted_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, 'queued', now(), now())
      ON CONFLICT (dedup_key) WHERE status IN ('queued','running') DO NOTHING
      """,
      vals
    )

    :ok
  end

  @impl true
  def claim(run_id, freeze_exempt) do
    %{rows: rows} =
      repo().query!(
        """
        UPDATE #{table()} SET status = 'running', run_id = $1, updated_at = now()
        WHERE id = (
          SELECT id FROM #{table()}
          WHERE status = 'queued'
            AND (kind = ANY($2) OR NOT (scope = ANY(
              SELECT DISTINCT scope FROM #{table()} WHERE status IN ('blocked','failed')
            )))
          ORDER BY seq
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        )
        RETURNING id, kind, scope, payload, dedup_key, actor, answers_id
        """,
        [Ecto.UUID.dump!(run_id), freeze_exempt]
      )

    case rows do
      [[id, kind, scope, payload, dedup_key, actor, answers_id]] ->
        %{
          "id" => Ecto.UUID.cast!(id),
          "kind" => kind,
          "scope" => scope,
          "payload" => payload,
          "dedup_key" => dedup_key,
          "actor" => actor,
          "answers_id" => answers_id && Ecto.UUID.cast!(answers_id)
        }

      [] ->
        nil
    end
  end

  @impl true
  def settle(id, status, fields) do
    result = fields |> Keyword.get(:result) |> encode()
    needs = fields |> Keyword.get(:needs) |> encode()
    error = Keyword.get(fields, :error)

    repo().query!(
      """
      UPDATE #{table()}
      SET status = $2, result = coalesce($3, result), needs = coalesce($4, needs),
          error = coalesce($5, error), executed_at = now(), updated_at = now()
      WHERE id = $1
      """,
      [Ecto.UUID.dump!(id), status, result, needs, error]
    )

    :ok
  end

  defp repo do
    Application.get_env(:reactive_dag, :repo) ||
      raise "reactive_dag: set `config :reactive_dag, repo: MyApp.Repo`"
  end

  defp table, do: Application.get_env(:reactive_dag, :commands_table, "commands")
  defp encode(nil), do: nil
  defp encode(map), do: Jason.encode!(map)
end
