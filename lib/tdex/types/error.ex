defmodule TDex.Error do
  @moduledoc """
  `TDex.Error` provide struct for query error
  """

  defexception [:message, :conn, :query]

  def exception(opts) do
    message = Keyword.get(opts, :message)
    conn = Keyword.get(opts, :conn)
    query = Keyword.get(opts, :query)
    %TDex.Error{message: message, conn: conn, query: query}
  end

  def message(e) do
    e.message
  end
end
