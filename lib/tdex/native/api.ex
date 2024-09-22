defmodule TDex.Native do
  @moduledoc """
  `TDex.Native`  provide api for DbConnection call
  """

  alias TDex.Wrapper

  def connect(args) do
    Wrapper.taos_connect(args.hostname, args.port, args.username, args.password, args.database)
  end

  def query(conn, statement) do
    Wrapper.taos_query(conn, statement)
  end

  def query_a(conn, statement) do
    TDex.Native.Async.query(conn, statement)
  end

  def prepare(conn, sql) do
    Wrapper.taos_stmt_prepare(conn, sql)
  end

  def execute(stmt, spec, data) do
    Wrapper.taos_stmt_execute(stmt, spec, data)
  end

  def close(conn) do
    Wrapper.taos_close(conn)
  end
end
