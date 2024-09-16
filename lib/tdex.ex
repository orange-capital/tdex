defmodule TDex do
  use Application
  alias TDex.Query
  import TDex.Utils

  def start(_type, _args) do
    Skn.Counter.create_db()
    children = [
      TDex.Native.Sup
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def start_link() do
    opts = Application.get_env(:tdex, TDex.Repo) |> default_opts()
    DBConnection.start_link(TDex.DBConnection, opts)
  end

  def start_link(opts) do
    opts = default_opts(opts)
    DBConnection.start_link(TDex.DBConnection, opts)
  end

  def query(conn, statement, params, opts \\ [])
  def query(conn, statement, params, opts) when is_binary(statement) do
    query(conn, %Query{name: "", statement: statement}, params, opts)
  end
  def query(conn, query, params, opts) do
    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, query, result} -> {:ok, query, result}
      {:error, _} = error -> error
    end
  end

  def query!(conn, statement, params, opts \\ [])
  def query!(conn, statement, params, opts) when is_binary(statement) do
    query!(conn, %Query{name: "", statement: statement}, params, opts)
  end
  def query!(conn, query, params, opts) do
    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, _, result} -> result
      {:error, error} -> raise error
    end
  end

  def execute(conn, query, params, opts \\ []) do
    DBConnection.execute(conn, query, params, opts)
  end

  def execute!(conn, query, params, opts \\ []) do
    DBConnection.execute!(conn, query, params, opts)
  end
end
