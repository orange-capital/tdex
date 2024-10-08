defmodule TDex.Native do
  @moduledoc """
  `TDex.Native`  provide api for DbConnection call
  """

  alias TDex.Wrapper

  def connect(args, opts) do
    Wrapper.taos_connect(args.hostname, args.port, args.username, args.password, args.database, opts)
  end

  def query(conn, statement, opts) do
    Wrapper.taos_query(conn, statement, opts)
  end

  def query_a(conn, statement) do
    Wrapper.taos_query_a(conn, statement)
  end

  def prepare(conn, sql) do
    Wrapper.taos_stmt_prepare(conn, sql)
  end

  def execute(conn, stmt, spec, data, opts) do
    Wrapper.taos_stmt_execute({conn, stmt}, spec, data, opts)
  end

  def close_stmt(conn, stmt) do
    if stmt != nil do
      Wrapper.taos_stmt_close({conn, stmt})
    end
  end

  def close(conn) do
    Wrapper.taos_close(conn)
  end
end
