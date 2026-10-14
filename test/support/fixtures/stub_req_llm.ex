defmodule ReactiveDag.Fixtures.StubReqLLM do
  @moduledoc """
  A stand-in for `req_llm`, so an `ash_ai` prompt action can be exercised
  without a network.

  In `test/support` rather than a test file because the LLM fixture names it at
  COMPILE time — `prompt/2` expands `req_llm:` while the module is being
  defined, so the stub has to exist before any test does.
  """
  def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
  def contexts, do: Agent.get(__MODULE__, &Enum.reverse/1)

  def generate_object(_model, context, _schema, _opts) do
    Agent.update(__MODULE__, &([inspect(context, limit: :infinity), &1] |> then(fn [h, t] -> [h | t] end)))
    {:ok, %{object: %{"sentiment" => "positive"}}}
  end
end
