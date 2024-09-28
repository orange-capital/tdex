defmodule TDex do
  use Application
  alias TDex.Query

  def start(_type, _args) do
    Skn.Counter.create_db()
    children = [
      TDex.Native.Sup
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def stop(_state) do
    TDex.Wrapper.stop_nif()
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
    default_mode = if String.contains?(statement, "?"), do: :stmt, else: :query
    {spec, opts} = Keyword.pop(opts, :spec)
    {mode, opts} = Keyword.pop(opts, :mode, default_mode)
    {async, opts} = Keyword.pop(opts, :async, false)
    query(conn, %Query{name: "", statement: statement, mode: mode, async: async, spec: spec}, params, opts)
  end
  def query(conn, query, params, opts) do
    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, query, result} ->
        TDex.Query.close(query, conn)
        {:ok, result}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def query!(conn, statement, params, opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  def execute(conn, query, params, opts \\ []) do
    DBConnection.execute(conn, query, params, opts)
  end

  def execute!(conn, query, params, opts \\ []) do
    DBConnection.execute!(conn, query, params, opts)
  end

  def default_opts(opts) do
    opts
    |> Keyword.put_new(:username, "root")
    |> Keyword.put_new(:password, "taosdata")
    |> Keyword.put_new(:database, "taos")
    |> Keyword.put_new(:hostname, "localhost")
    |> Keyword.put_new(:timeout, 10000)
    |> Keyword.put_new(:port, default_port(Keyword.get(opts, :protocol)))
    |> Keyword.update(:protocol, TDex.Native, &handle_protocol/1)
    |> Keyword.update!(:port, &normalize_port/1)
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
  end

  defp normalize_port(port) when is_binary(port), do: String.to_integer(port)
  defp normalize_port(port), do: port

  defp handle_protocol(:native), do: TDex.Native
  defp handle_protocol(:ws), do: TDex.Ws

  defp default_port(:native), do: 6030
  defp default_port(:ws), do: 6041
end
