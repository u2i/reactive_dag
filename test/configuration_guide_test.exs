defmodule ReactiveDag.ConfigurationGuideTest do
  @moduledoc """
  The configuration guide must list every key the library actually reads.

  A doc page drifts as readily as an issue does — #40 was filed listing 8 keys,
  and `:insights_keep` landed later without it being updated, which is what
  prompted the guide in the first place. A prose promise to keep docs current is
  a discipline; this is a gate.

  It reads the call sites out of `lib/` rather than a hand-maintained list, so
  adding a `Application.get_env(:reactive_dag, :new_thing)` anywhere fails here
  until the guide mentions it.
  """
  use ExUnit.Case, async: true

  @guide "guides/configuration.md"

  test "every configured key is documented, and every documented key is real" do
    used = keys_used_in_lib()
    documented = keys_in_guide()

    assert used != [], "found no config call sites — has the grep broken?"

    undocumented = used -- documented

    assert undocumented == [],
           """
           #{@guide} does not mention #{inspect(undocumented)}.

           The library reads these but the guide never names them, so a host has
           no way to discover them short of grepping lib/. Add a section (and a
           row in the table) for each.
           """

    stale = documented -- used

    assert stale == [],
           """
           #{@guide} documents #{inspect(stale)}, which nothing in lib/ reads.

           Either the key was removed and the guide kept it, or it is named
           somewhere it should not be. A guide that lists options that do
           nothing is worse than one that omits them.
           """
  end

  test "the guide's table lists every key, not just the prose" do
    # a key explained in prose but missing from the summary table is invisible
    # to the reader who only scans the table — which is most of them
    missing = keys_used_in_lib() -- table_keys(File.read!(@guide))

    assert missing == [],
           "#{@guide}'s summary table is missing #{inspect(missing)}"
  end

  # every `Application.get_env(:reactive_dag, :key…)` call site under lib/
  defp keys_used_in_lib do
    Path.wildcard("lib/**/*.ex")
    |> Enum.flat_map(fn file ->
      ~r/Application\.get_env\(:reactive_dag,\s*:(\w+)/
      |> Regex.scan(File.read!(file))
      |> Enum.map(fn [_, key] -> String.to_atom(key) end)
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  # keys the guide DOCUMENTS: the table rows and the `### :key` headings. Not
  # every `:atom` in the prose — the guide mentions `:ok` as a return value,
  # and a looser match would call that a config key.
  defp keys_in_guide do
    text = File.read!(@guide)

    heading_keys =
      ~r/^### (.+)$/m
      |> Regex.scan(text)
      |> Enum.flat_map(fn [_, heading] ->
        ~r/`:(\w+)`/ |> Regex.scan(heading) |> Enum.map(fn [_, k] -> String.to_atom(k) end)
      end)

    (heading_keys ++ table_keys(text)) |> Enum.uniq() |> Enum.sort()
  end

  defp table_keys(text) do
    text
    |> String.split("\n")
    |> Enum.filter(&String.match?(&1, ~r/^\| \[`:/))
    |> Enum.map(fn row ->
      [[_, key]] = Regex.scan(~r/^\| \[`:(\w+)`\]/, row)
      String.to_atom(key)
    end)
  end
end
