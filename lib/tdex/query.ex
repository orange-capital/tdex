defmodule TDex.Query do
  @moduledoc """
  `TDex.Query` provide api to work with query syntax
  """

  @type t :: %__MODULE__{
    name: iodata,
    statement: iodata,
    cache: TDex.Wrapper.stmt_t,
    spec: [{atom, atom, non_neg_integer}],
    mode: :stmt | :query,
    async: bool
  }

  defstruct [
    :name,
    :statement,
    :cache,
    :spec,
    :mode,
    :async
  ]

  def close(%__MODULE__{cache: cache}) do
    if cache != nil do
      TDex.Wrapper.taos_stmt_free(cache)
    end
  end
  def close(_), do: :ok
end

defimpl DBConnection.Query, for: TDex.Query do
  def decode(_query, result, _opts) do
    result
  end

  def describe(query, _opts) do
    query
  end

  def encode(_query, params, _opts) do
    params
  end

  def parse(query, _opts) do
    query
  end
end
