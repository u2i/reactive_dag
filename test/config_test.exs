defmodule ReactiveDag.ConfigTest do
  @moduledoc """
  `ReactiveDag.Config.validate!/0` (#53) — boot-time validation.

  Misconfiguration otherwise surfaces at the FIRST QUERY: a missing `:repo`
  raises on the first drain, possibly a long way into a deploy. And only
  `:repo` raises at all — a bad writer or table name fails later and less
  legibly.

  The property that matters most here is that it reports EVERY problem, not the
  first. A config with two mistakes should take one deploy to fix.
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Config

  @keys [
    :repo,
    :dirty_table,
    :insights_keep
  ]

  defmodule GoodRepo do
    def query!(_sql, _params), do: %{rows: []}
  end

  defmodule NotAWriter do
    def hello, do: :world
  end

  setup do
    prior = Map.new(@keys, &{&1, Application.get_env(:reactive_dag, &1)})

    on_exit(fn ->
      for {k, v} <- prior do
        if is_nil(v),
          do: Application.delete_env(:reactive_dag, k),
          else: Application.put_env(:reactive_dag, k, v)
      end
    end)

    # start from a known-good baseline; each test breaks one thing
    for k <- @keys, do: Application.delete_env(:reactive_dag, k)
    Application.put_env(:reactive_dag, :repo, GoodRepo)

    :ok
  end

  test "a sound configuration passes" do
    assert Config.problems() == []
    assert Config.validate!() == :ok
  end

  test "only :repo is required — everything else has a working default" do
    # nothing but :repo set, and it is fine
    assert Config.problems() == []
  end

  describe ":repo" do
    test "missing is reported, with the fix" do
      Application.delete_env(:reactive_dag, :repo)

      assert [problem] = Config.problems()
      assert problem =~ "`:repo` is not set"
      assert problem =~ "config :reactive_dag, repo: MyApp.Repo"
    end

    test "a module that isn't loadable" do
      Application.put_env(:reactive_dag, :repo, NoSuchRepoModule)

      assert [problem] = Config.problems()
      assert problem =~ "not a loadable module"
    end

    test "a module that isn't a repo — says WHY it needs query!/2" do
      Application.put_env(:reactive_dag, :repo, NotAWriter)

      assert [problem] = Config.problems()
      assert problem =~ "query!/2"
      assert problem =~ "raw SQL"
    end

    test "not a module at all" do
      Application.put_env(:reactive_dag, :repo, "MyApp.Repo")

      assert [problem] = Config.problems()
      assert problem =~ "must be a module"
    end
  end

  describe "table names" do
    test "a name that isn't a SQL identifier" do
      Application.put_env(:reactive_dag, :dirty_table, "my dirty")

      assert [problem] = Config.problems()
      assert problem =~ ":dirty_table"
      assert problem =~ "not a valid SQL identifier"
    end

    test "a name starting with a digit" do
      Application.put_env(:reactive_dag, :dirty_table, "2dirty")

      assert [problem] = Config.problems()
      assert problem =~ ":dirty_table"
    end

    test "a legal custom name passes — this is how a host keeps its table" do
      Application.put_env(:reactive_dag, :dirty_table, "my_existing_dirty")

      assert Config.problems() == []
    end

    test "not a string" do
      Application.put_env(:reactive_dag, :dirty_table, :my_dirty)

      assert [problem] = Config.problems()
      assert problem =~ "must be a string"
    end
  end

  describe ":insights_keep" do
    test "absent is fine" do
      assert Config.problems() == []
    end

    test "a positive integer is fine" do
      Application.put_env(:reactive_dag, :insights_keep, 50)
      assert Config.problems() == []
    end

    test "zero or negative is not" do
      Application.put_env(:reactive_dag, :insights_keep, 0)

      assert [problem] = Config.problems()
      assert problem =~ "positive integer"
    end
  end

  describe "validate!/0" do
    test "reports EVERY problem at once — one deploy to fix, not two" do
      Application.delete_env(:reactive_dag, :repo)
      Application.put_env(:reactive_dag, :dirty_table, "my dirty")

      err = assert_raise Config.Error, fn -> Config.validate!() end
      msg = Exception.message(err)

      assert msg =~ "`:repo` is not set"
      assert msg =~ "not a valid SQL identifier"

      # …and reads as a list, not one run-on sentence
      assert length(String.split(msg, "\n  * ")) == 3
    end

    test "raises nothing when sound" do
      assert Config.validate!() == :ok
    end
  end
end
