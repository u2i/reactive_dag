defmodule ReactiveDag.AshCommandExecutorTest do
  @moduledoc """
  `ReactiveDag.CommandExecutor.Ash` — the generic CRUD executor: a command kind maps
  to `{resource, action}` and its payload IS the action input, so a host editing a
  source-of-truth through the frontier declares the mapping instead of hand-writing
  one near-identical executor per kind.

  Shaped after the real consumer (an org editor): an App with a string primary key,
  and a Repo resolved by TWO payload fields — covering create / update / destroy and
  the failure paths.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.CommandExecutor.Ash, as: AshExec

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false
    resources do
      allow_unregistered? true
    end
  end

  defmodule App do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end

    attributes do
      attribute :id, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :name, :string, public?: true
    end

    identities do
      identity :by_id, [:id], pre_check_with: ReactiveDag.AshCommandExecutorTest.Domain
    end

    actions do
      defaults [:read, :destroy]

      create :add do
        accept [:id, :name]
        upsert? true
        upsert_identity :by_id
      end

      update :rename do
        accept [:name]
        # the identity's pre_check adds a before_action hook, which blocks Ash's
        # atomic-update path; irrelevant to what's under test.
        require_atomic? false
      end
    end
  end

  defmodule Repo do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets
    ets do private?(true) end

    attributes do
      uuid_primary_key :id
      attribute :app_id, :string, allow_nil?: false, public?: true
      attribute :full, :string, allow_nil?: false, public?: true
      attribute :gated, :boolean, default: true, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :add do
        accept [:app_id, :full, :gated]
      end

      update :set_gated do
        accept [:gated]
      end
    end
  end

  setup do
    prev = Application.get_env(:reactive_dag, :ash_commands)

    Application.put_env(:reactive_dag, :ash_commands, %{
      "app.add" => {App, :add},
      "app.rename" => {App, :rename, by: [:id]},
      "app.remove" => {App, :destroy, by: [:id]},
      "repo.add" => {Repo, :add},
      "repo.set_gated" => {Repo, :set_gated, by: [:app_id, :full]},
      "repo.remove" => {Repo, :destroy, by: [:app_id, :full]},
      "app.add_named" => {App, :add, result: fn r -> %{"named" => r.name} end}
    })

    on_exit(fn -> Application.put_env(:reactive_dag, :ash_commands, prev) end)
    :ok
  end

  defp cmd(kind, payload), do: %{"kind" => kind, "payload" => payload}

  test "create: the payload IS the action input" do
    assert {:done, result} = AshExec.execute(cmd("app.add", %{"id" => "acme", "name" => "Acme"}), %{})
    assert result["id"] == "acme"
    assert result["action"] == "add"

    assert {:ok, [app]} = Ash.read(App)
    assert app.id == "acme"
    assert app.name == "Acme"
  end

  test "create passes ARGUMENTS through too (not just attributes)" do
    AshExec.execute(cmd("app.add", %{"id" => "acme"}), %{})

    assert {:done, _} =
             AshExec.execute(cmd("repo.add", %{"app_id" => "acme", "full" => "acme/api", "gated" => true}), %{})

    assert {:ok, [repo]} = Ash.read(Repo)
    assert repo.app_id == "acme"
    assert repo.full == "acme/api"
  end

  test "update: resolves the record by `by:` fields, then applies the action" do
    AshExec.execute(cmd("app.add", %{"id" => "acme", "name" => "Acme"}), %{})

    assert {:done, _} = AshExec.execute(cmd("app.rename", %{"id" => "acme", "name" => "Acme Corp"}), %{})

    assert {:ok, [app]} = Ash.read(App)
    assert app.name == "Acme Corp"
  end

  test "update resolves by MULTIPLE payload fields" do
    AshExec.execute(cmd("app.add", %{"id" => "acme"}), %{})
    AshExec.execute(cmd("repo.add", %{"app_id" => "acme", "full" => "acme/api", "gated" => true}), %{})
    AshExec.execute(cmd("repo.add", %{"app_id" => "acme", "full" => "acme/web", "gated" => true}), %{})

    assert {:done, _} =
             AshExec.execute(cmd("repo.set_gated", %{"app_id" => "acme", "full" => "acme/web", "gated" => false}), %{})

    {:ok, repos} = Ash.read(Repo)
    by_full = Map.new(repos, &{&1.full, &1.gated})
    assert by_full["acme/web"] == false
    # the OTHER repo is untouched — the two-field lookup discriminated correctly.
    assert by_full["acme/api"] == true
  end

  test "destroy: the action type is read off the resource (no special config)" do
    AshExec.execute(cmd("app.add", %{"id" => "acme"}), %{})
    assert {:done, _} = AshExec.execute(cmd("app.remove", %{"id" => "acme"}), %{})
    assert {:ok, []} = Ash.read(App)
  end

  test "a missing record is an {:error, _}, not a raise" do
    assert {:error, msg} = AshExec.execute(cmd("app.rename", %{"id" => "ghost", "name" => "x"}), %{})
    assert msg =~ "no"
    assert msg =~ "ghost"
  end

  test "a payload missing a lookup field is an {:error, _}" do
    assert {:error, msg} = AshExec.execute(cmd("repo.set_gated", %{"app_id" => "acme"}), %{})
    assert msg =~ "full"
  end

  test "an unmapped kind is an {:error, _}" do
    assert {:error, msg} = AshExec.execute(cmd("nope.thing", %{}), %{})
    assert msg =~ "no ash_commands mapping"
  end

  test "a failing Ash action (validation) comes back as {:error, _}" do
    # :add requires app_id + full; omit them.
    assert {:error, _msg} = AshExec.execute(cmd("repo.add", %{"gated" => true}), %{})
    assert {:ok, []} = Ash.read(Repo)
  end

  test "a host `:result` fun shapes the {:done, result} map" do
    assert {:done, %{"named" => "Acme"}} =
             AshExec.execute(cmd("app.add_named", %{"id" => "acme", "name" => "Acme"}), %{})
  end
end
