defmodule ReactiveDag.CommandExecutor.Ash do
  @moduledoc """
  A GENERIC mutation executor for the common case: a command kind is a CRUD action
  on an Ash resource, and its `payload` is that action's input.

  Hosts editing a source-of-truth through the frontier otherwise hand-write one
  near-identical executor per kind — pull payload, resolve the record, call the
  action, wrap `{:ok, _}` / `{:error, _}`. Ash already specifies the write (action
  name, accepts, arguments, validation); a command adds ORDER + AUDIT + the
  transaction. This executor keeps those concerns separate: declare the mapping,
  don't re-code the write.

  ## Declaring the mapping

      config :reactive_dag,
        ash_commands: %{
          "app.add"        => {MyApp.Org.App, :add},
          "app.rename"     => {MyApp.Org.App, :rename, by: [:id]},
          "app.remove"     => {MyApp.Org.App, :destroy, by: [:id]},
          "repo.add"       => {MyApp.Org.Repo, :add},
          "repo.set_gated" => {MyApp.Org.Repo, :set_gated, by: [:app_id, :full]},
          "repo.remove"    => {MyApp.Org.Repo, :destroy, by: [:app_id, :full]}
        }

  and point every one of those kinds at this module:

      config :reactive_dag,
        command_executors: %{"app.add" => ReactiveDag.CommandExecutor.Ash, …}

  (or route a whole family to it with `:command_executor_resolver`).

  ## How a kind is applied

  The entry is `{resource, action}` or `{resource, action, opts}`:

    * **no `:by`** → a CREATE: `Ash.create(resource, payload, action: action)`. Upserts
      are just a create action declared `upsert? true`.
    * **`by: [:field, …]`** → resolve the record first by matching those fields
      against the same-named payload keys, then run the action on it — an UPDATE
      (`Ash.update/3`) or, when the action is a destroy, `Ash.destroy/2`. Which one
      is read off the resource's action type, so `:destroy` needs no special config.

  Payload keys are strings (the frontier's shape); Ash accepts string-keyed params,
  so arguments and attributes both flow through without the executor knowing which
  is which.

  The `by:` fields ADDRESS the record — they are not action input, and are stripped
  from the params before the action runs. So `{Repo, :set_gated, by: [:app_id,
  :full]}` with payload `%{"app_id" => …, "full" => …, "gated" => false}` finds the
  repo by app_id+full and passes only `gated` to `:set_gated` (whose accept list is
  just `[:gated]`). This keeps the addressing/​input split explicit rather than
  forcing every update action to accept its own lookup keys.

  Other `opts`:

    * `:result` — `(record -> map())` building the `{:done, result}` map recorded on
      the command row. Default: the record's primary key(s), plus `"action"`.
    * `:domain` / `:actor` / `:tenant` — forwarded to the Ash call. `:actor` may be a
      `(ctx -> actor)` fun to read the command's actor out of `ctx`.

  ## Outcomes

  Never raises: a missing record, a failed validation, or a raised exception all come
  back as `{:error, reason}`, so the frontier parks the command `failed` and freezes
  its scope (per `ReactiveDag.CommandExecutor.Mutation`).
  """
  use ReactiveDag.CommandExecutor.Mutation

  require Ash.Query

  @impl true
  def execute(cmd, ctx) do
    kind = ReactiveDag.Commands.field(cmd, :kind)
    payload = ReactiveDag.Commands.field(cmd, :payload) || %{}

    case mapping(kind) do
      nil -> {:error, "no ash_commands mapping for kind #{inspect(kind)}"}
      spec -> apply_spec(spec, payload, ctx)
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  # ── applying one mapping ────────────────────────────────────────────────────

  defp apply_spec({resource, action}, payload, ctx), do: apply_spec({resource, action, []}, payload, ctx)

  defp apply_spec({resource, action, opts}, payload, ctx) do
    case Keyword.get(opts, :by) do
      nil -> create(resource, action, payload, opts, ctx)
      by -> on_record(resource, action, by, payload, opts, ctx)
    end
  end

  defp apply_spec(other, _payload, _ctx), do: {:error, "bad ash_commands entry: #{inspect(other)}"}

  defp create(resource, action, payload, opts, ctx) do
    resource
    |> Ash.create(payload, [action: action] ++ ash_opts(opts, ctx))
    |> wrap(action, opts)
  end

  # resolve the record by the `by:` fields, then update or destroy it.
  defp on_record(resource, action, by, payload, opts, ctx) do
    with {:ok, record} <- fetch(resource, by, payload, opts, ctx) do
      case action_type(resource, action) do
        :destroy ->
          case Ash.destroy(record, [action: action] ++ ash_opts(opts, ctx)) do
            :ok -> {:done, result(record, action, opts)}
            {:ok, r} -> {:done, result(r, action, opts)}
            {:error, e} -> {:error, describe(e)}
          end

        _ ->
          record
          |> Ash.update(input(payload, by), [action: action] ++ ash_opts(opts, ctx))
          |> wrap(action, opts)
      end
    end
  end

  # the `by:` fields ADDRESS the record; they are not action input. Ash rejects
  # inputs an action doesn't accept, so strip them before applying — leaving only
  # "what to change". (A field that is genuinely both is re-added by the action's
  # own accept list via the remaining payload.)
  defp input(payload, by) do
    Map.drop(payload, Enum.map(by, &to_string/1))
  end

  # find exactly one record whose `by` fields equal the same-named payload keys.
  defp fetch(resource, by, payload, opts, ctx) do
    missing = Enum.reject(by, &Map.has_key?(payload, to_string(&1)))

    if missing != [] do
      {:error, "payload missing #{inspect(Enum.map(missing, &to_string/1))} needed to find the record"}
    else
      filter = for f <- by, do: {f, Map.get(payload, to_string(f))}

      resource
      |> Ash.Query.do_filter(filter)
      |> Ash.read(ash_opts(opts, ctx))
      |> case do
        {:ok, [record]} -> {:ok, record}
        {:ok, []} -> {:error, "no #{inspect(resource)} matching #{inspect(Map.new(filter))}"}
        {:ok, many} -> {:error, "#{length(many)} #{inspect(resource)} match #{inspect(Map.new(filter))}"}
        {:error, e} -> {:error, describe(e)}
      end
    end
  end

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp wrap({:ok, record}, action, opts), do: {:done, result(record, action, opts)}
  defp wrap({:error, e}, _action, _opts), do: {:error, describe(e)}

  # the {:done, result} map: host-supplied, else the record's primary key + action.
  defp result(record, action, opts) do
    case Keyword.get(opts, :result) do
      fun when is_function(fun, 1) ->
        fun.(record)

      _ ->
        record
        |> primary_key_map()
        |> Map.put("action", to_string(action))
    end
  end

  defp primary_key_map(%resource{} = record) do
    for key <- Ash.Resource.Info.primary_key(resource),
        into: %{},
        do: {to_string(key), to_string_safe(Map.get(record, key))}
  rescue
    _ -> %{}
  end

  defp primary_key_map(_), do: %{}

  defp to_string_safe(v) when is_binary(v), do: v
  defp to_string_safe(v), do: inspect(v)

  defp action_type(resource, action) do
    case Ash.Resource.Info.action(resource, action) do
      %{type: type} -> type
      _ -> nil
    end
  end

  defp ash_opts(opts, ctx) do
    [:domain, :tenant]
    |> Enum.flat_map(fn k -> if v = Keyword.get(opts, k), do: [{k, v}], else: [] end)
    |> then(fn base ->
      case Keyword.get(opts, :actor) do
        nil -> base
        fun when is_function(fun, 1) -> [{:actor, fun.(ctx)} | base]
        actor -> [{:actor, actor} | base]
      end
    end)
  end

  defp describe(%{__exception__: true} = e), do: Exception.message(e)
  defp describe(e), do: inspect(e)

  defp mapping(kind), do: Application.get_env(:reactive_dag, :ash_commands, %{})[kind]
end
