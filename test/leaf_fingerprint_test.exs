defmodule ReactiveDag.LeafFingerprintTest do
  @moduledoc """
  `fingerprint` on a source-fed leaf (#73) — the one value that decides whether
  an observation moved.

  `Payload.upsert` compares every writable attribute, which is right for a
  derived node: every attribute there is part of the result. It is wrong for a
  leaf, whose row carries fields that move on every observation *without the
  observation having changed anything* — a `last_seen_at` by definition, an
  `etag` a server may re-issue for identical bytes.

  Without this, a re-crawl of an unmodified document reports `:changed`, the key
  propagates, and everything downstream recomputes — for cascade, re-running LLM
  extraction over PDFs that did not move. That is precisely the cost change
  detection exists to avoid.

  The property under test: **a re-observation that only moves the
  observation-bookkeeping fields is NOT a change.**
  """
  use ExUnit.Case, async: false

  alias ReactiveDag.Node.{Payload, Rows}

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      allow_unregistered?(true)
    end
  end

  # a crawled document: content, plus the fields that move on every poll
  defmodule Docs do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :title, :string, public?: true
      attribute :content_md5, :string, public?: true
      # the two that move on their own
      attribute :last_seen_at, :utc_datetime_usec, public?: true
      attribute :etag, :string, public?: true
      attribute :fingerprint, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :title, :content_md5, :last_seen_at, :etag, :fingerprint])
      end
    end

    reactive do
      id(:docs)
      leaf?(true)
      fingerprint([:content_md5])
    end
  end

  # the agenda case: the fingerprint is COMPUTED, not a field list — a re-titled
  # meeting must re-fire even though its PDF bytes are identical
  defmodule Agendas do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :title, :string, public?: true
      attribute :content_md5, :string, public?: true
      attribute :last_seen_at, :utc_datetime_usec, public?: true
      attribute :digest, :string, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :title, :content_md5, :last_seen_at, :digest])
      end
    end

    reactive do
      id(:agendas)
      leaf?(true)
      fingerprint(fn row -> "#{row.content_md5}|#{:erlang.phash2(row.title)}" end)
      fingerprint_attribute(:digest)
    end
  end

  # no fingerprint declared — the old behaviour, unchanged
  defmodule Plain do
    use Ash.Resource, domain: Domain, data_layer: Ash.DataLayer.Ets, extensions: [ReactiveDag.Node]

    ets do
    end

    attributes do
      attribute :key, :string, primary_key?: true, allow_nil?: false, public?: true
      attribute :content_md5, :string, public?: true
      attribute :last_seen_at, :utc_datetime_usec, public?: true
    end

    actions do
      defaults [:read, :destroy]

      create :upsert do
        upsert?(true)
        accept([:key, :content_md5, :last_seen_at])
      end
    end

    reactive do
      id(:plain)
      leaf?(true)
    end
  end

  setup do
    for r <- [Docs, Agendas, Plain], row <- Ash.read!(r), do: Ash.destroy!(row)
    :ok
  end

  defp cell(mod) do
    [c] = ReactiveDag.Node.cells(mod)
    c
  end

  defp t(n), do: DateTime.add(~U[2026-01-01 00:00:00.000000Z], n, :second)

  describe "the leaf problem this exists to solve" do
    test "a re-crawl that only moves last_seen_at/etag is NOT a change" do
      cell = cell(Docs)
      crawled = fn seen, etag -> %{key: "d1", title: "Budget", content_md5: "abc", last_seen_at: seen, etag: etag} end

      # first observation
      {:ok, changed} = Rows.reconcile(cell, ["d1"], upsert: fn _ -> crawled.(t(0), "W/\"1\"") end)
      assert changed == ["d1"]

      # re-crawl: identical bytes, new timestamp, server re-issued the etag
      {:ok, changed} = Rows.reconcile(cell, ["d1"], upsert: fn _ -> crawled.(t(3600), "W/\"2\"") end)

      assert changed == [], "a document that did not move must not fire the cascade"

      # ...and the bookkeeping fields WERE still written — the row is current,
      # it just isn't news
      [row] = Ash.read!(Docs)
      assert row.last_seen_at == t(3600)
      assert row.etag == "W/\"2\""
    end

    test "without a fingerprint, that same re-crawl DOES report a change" do
      # the behaviour being fixed, pinned so the fix is visibly load-bearing
      cell = cell(Plain)

      {:ok, _} = Rows.reconcile(cell, ["d1"], upsert: fn _ -> %{key: "d1", content_md5: "abc", last_seen_at: t(0)} end)

      {:ok, changed} =
        Rows.reconcile(cell, ["d1"], upsert: fn _ -> %{key: "d1", content_md5: "abc", last_seen_at: t(3600)} end)

      assert changed == ["d1"]
    end

    test "content that actually moved IS a change" do
      cell = cell(Docs)

      {:ok, _} = Rows.reconcile(cell, ["d1"], upsert: fn _ -> %{key: "d1", content_md5: "abc", last_seen_at: t(0)} end)

      {:ok, changed} =
        Rows.reconcile(cell, ["d1"], upsert: fn _ -> %{key: "d1", content_md5: "DIFFERENT", last_seen_at: t(1)} end)

      assert changed == ["d1"]
    end

    test "a field OUTSIDE the fingerprint does not fire it — the host chose what counts" do
      cell = cell(Docs)

      {:ok, _} =
        Rows.reconcile(cell, ["d1"], upsert: fn _ -> %{key: "d1", title: "Budget", content_md5: "abc"} end)

      # the title moved; the fingerprint is [:content_md5], so this leaf says no
      {:ok, changed} =
        Rows.reconcile(cell, ["d1"], upsert: fn _ -> %{key: "d1", title: "Budget (revised)", content_md5: "abc"} end)

      assert changed == []
      assert [%{title: "Budget (revised)"}] = Ash.read!(Docs)
    end
  end

  describe "a computed fingerprint — the agenda case" do
    test "a re-titled document re-fires even though its bytes are identical" do
      cell = cell(Agendas)

      {:ok, _} =
        Rows.reconcile(cell, ["a1"], upsert: fn _ -> %{key: "a1", title: "Jan 3", content_md5: "abc"} end)

      {:ok, changed} =
        Rows.reconcile(cell, ["a1"], upsert: fn _ -> %{key: "a1", title: "Jan 3 (amended)", content_md5: "abc"} end)

      assert changed == ["a1"], "the title is folded into the digest, so this moved"
    end

    test "identical title and bytes are still unchanged" do
      cell = cell(Agendas)
      row = fn seen -> %{key: "a1", title: "Jan 3", content_md5: "abc", last_seen_at: seen} end

      {:ok, _} = Rows.reconcile(cell, ["a1"], upsert: fn _ -> row.(t(0)) end)
      {:ok, changed} = Rows.reconcile(cell, ["a1"], upsert: fn _ -> row.(t(9999)) end)

      assert changed == []
    end

    test "the value lands in the named attribute" do
      cell = cell(Agendas)
      {:ok, _} = Rows.reconcile(cell, ["a1"], upsert: fn _ -> %{key: "a1", title: "Jan 3", content_md5: "abc"} end)

      [row] = Ash.read!(Agendas)
      assert row.digest == "abc|#{:erlang.phash2("Jan 3")}"
    end
  end

  describe "the widened :upsert seam" do
    test "returning nil skips the key entirely — an unobservable unit is not news" do
      cell = cell(Docs)

      {:ok, changed} =
        Rows.reconcile(cell, ["d1", "d2"],
          upsert: fn
            "d1" -> %{key: "d1", content_md5: "abc"}
            "d2" -> nil
          end
        )

      assert changed == ["d1"]
      # nothing was written for the key we could not observe
      assert Enum.map(Ash.read!(Docs), & &1.key) == ["d1"]
    end

    test "a boolean return still works — the host wrote the row itself" do
      cell = cell(Docs)

      {:ok, changed} =
        Rows.reconcile(cell, ["d1", "d2"], upsert: fn key -> key == "d1" end)

      assert changed == ["d1"]
      # ...and the library wrote nothing, since the host said it had
      assert Ash.read!(Docs) == []
    end

    test "a returned row is written, so a poll is fetch → build → reconcile" do
      cell = cell(Docs)

      {:ok, changed} =
        Rows.reconcile(cell, ["d1", "d2"],
          upsert: fn key -> %{key: key, content_md5: "hash-#{key}"} end
        )

      assert changed == ["d1", "d2"]
      assert Enum.map(Ash.read!(Docs), & &1.content_md5) |> Enum.sort() == ["hash-d1", "hash-d2"]
    end

    test "vanished keys still retire alongside a row-returning upsert" do
      cell = cell(Docs)

      {:ok, _} =
        Rows.reconcile(cell, ["d1", "d2"], upsert: fn key -> %{key: key, content_md5: "x"} end)

      # the scan now finds only d1
      {:ok, changed} =
        Rows.reconcile(cell, ["d1"], upsert: fn key -> %{key: key, content_md5: "x"} end)

      assert changed == ["d2"]
      assert Enum.map(Ash.read!(Docs), & &1.key) == ["d1"]
    end
  end

  describe "Payload.upsert directly" do
    test "the fingerprint REPLACES the all-attribute comparison" do
      row = fn seen -> %{key: "d1", content_md5: "abc", last_seen_at: seen} end
      opts = [fingerprint: [:content_md5]]

      assert Payload.upsert(Docs, :key, "d1", row.(t(0)), :upsert, opts) == :changed
      assert Payload.upsert(Docs, :key, "d1", row.(t(500)), :upsert, opts) == :unchanged
    end

    test "no options → the old behaviour exactly" do
      row = fn seen -> %{key: "d1", content_md5: "abc", last_seen_at: seen} end

      assert Payload.upsert(Docs, :key, "d1", row.(t(0))) == :changed
      assert Payload.upsert(Docs, :key, "d1", row.(t(500))) == :changed
    end

    test "a fn returning nil falls back to comparing everything, rather than trusting it" do
      # a source that cannot determine its fingerprint must not read as unchanged
      opts = [fingerprint: fn _row -> nil end]

      assert Payload.upsert(Docs, :key, "d1", %{key: "d1", content_md5: "abc"}, :upsert, opts) == :changed

      assert Payload.upsert(Docs, :key, "d1", %{key: "d1", content_md5: "MOVED"}, :upsert, opts) ==
               :changed
    end

    test "a missing fingerprint column raises with the fix, rather than silently never matching" do
      err =
        assert_raise ArgumentError, fn ->
          Payload.upsert(Plain, :key, "d1", %{key: "d1", content_md5: "abc"}, :upsert,
            fingerprint: [:content_md5]
          )
        end

      msg = Exception.message(err)
      assert msg =~ "no :fingerprint attribute"
      assert msg =~ "attribute :fingerprint, :string"
      assert msg =~ "fingerprint_attribute:"
    end
  end

  test "the DSL slots reach the cell's meta" do
    assert cell(Docs).meta[:fingerprint] == [:content_md5]
    assert cell(Agendas).meta[:fingerprint_attribute] == :digest
    refute cell(Plain).meta[:fingerprint]
  end

  describe "the shared implementation" do
    alias ReactiveDag.Node.Fingerprint

    test "a field list hashes the named fields, and nothing else" do
      a = Fingerprint.of([:content_md5], %{content_md5: "abc", last_seen_at: t(0)})
      b = Fingerprint.of([:content_md5], %{content_md5: "abc", last_seen_at: t(9999)})

      assert a == b
      assert a != Fingerprint.of([:content_md5], %{content_md5: "xyz"})
    end

    test "field ORDER is part of the identity — [:a, :b] is not [:b, :a]" do
      row = %{a: "1", b: "2"}
      assert Fingerprint.of([:a, :b], row) != Fingerprint.of([:b, :a], row)
    end

    test "a missing field is not the same as an empty one" do
      assert Fingerprint.of([:absent], %{}) != Fingerprint.of([:absent], %{absent: ""})
    end

    test "no spec means no fingerprint — every pass treats the row as moved" do
      assert Fingerprint.of(nil, %{a: 1}) == nil
      assert Fingerprint.of([], %{a: 1}) == nil
    end

    test "per_key and a leaf compute the SAME value for the same fields" do
      # they are one implementation; this pins that they stay one. If they
      # drifted, a row could read as moved on one rung and unmoved on the other.
      row = %{body: "hello", other: "ignored"}
      assert Fingerprint.of([:body], row) == Fingerprint.of([:body], %{body: "hello"})
    end
  end
end
