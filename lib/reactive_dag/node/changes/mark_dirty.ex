defmodule ReactiveDag.Node.Changes.MarkDirty do
  @moduledoc """
  Marks a written record's key dirty on its own cell, so the next drain picks
  the change up. Wired automatically by `dirties_on` — hosts do not add it.

  ## Why a change, not a notifier

  The obvious tool is an `Ash.Notifier`, and it is the wrong one: Ash dispatches
  notifications **after** the transaction commits. A mark written there is not
  covered by the write's transaction, so a crash between commit and dispatch
  loses it — and a lost mark is silent staleness, the failure mode this feature
  exists to remove.

  An `after_action` hook runs **inside** the transaction (its counterpart is
  named `before_transaction` precisely because that one does not). The dirty
  write therefore joins the host's transaction through the same repo:

    * a rolled-back write leaves **no** dirty key, and
    * a committed write **always** leaves one.

  Both halves matter. Neither is true of a notifier.

  ## Cost

  The mark is one small `INSERT … ON CONFLICT DO NOTHING` per write, inside a
  transaction the host already holds open. A host writing at a rate where that
  contends should feed the leaf from a `ReactiveDag.Source` poll instead — see
  the sources guide.
  """
  use Ash.Resource.Change

  @impl true
  def change(changeset, opts, _context) do
    Ash.Changeset.after_action(changeset, fn _changeset, result ->
      mark(result, opts)
      {:ok, result}
    end)
  end

  # a destroy still marks: the row is gone but its key is exactly what
  # downstream needs to reprice (a `:group` claim then degrades to whole-cell,
  # since the lookup can no longer resolve it — correct, just coarse).
  @impl true
  def atomic(changeset, opts, context), do: {:ok, change(changeset, opts, context)}

  defp mark(record, opts) do
    cell = Keyword.fetch!(opts, :cell)

    case key_of(record, opts) do
      nil ->
        # A record with no derivable key cannot be named downstream. Escalating
        # to a whole-cell claim is the honest fallback: correct, and loud in the
        # Report rather than silently missing.
        ReactiveDag.Frontier.mark_dirty(cell, ["*"], "written (no key)")

      key ->
        ReactiveDag.Frontier.mark_dirty(cell, [key], "written")
    end
  end

  # the same derivation the payload loop uses: identity fields serialized in
  # primary-key order, else the single payload key attribute.
  defp key_of(record, opts) do
    case Keyword.get(opts, :identity_fields) do
      fields when is_list(fields) and fields != [] ->
        if Enum.all?(fields, &(not is_nil(Map.get(record, &1)))) do
          Enum.map_join(fields, "|", &to_string(Map.get(record, &1)))
        end

      _ ->
        case Map.get(record, Keyword.fetch!(opts, :payload_key)) do
          nil -> nil
          value -> to_string(value)
        end
    end
  end
end
