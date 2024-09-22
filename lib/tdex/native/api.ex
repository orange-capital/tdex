defmodule TDex.Native do
  @moduledoc """
  `TDex.Native`  provide api for DbConnection call
  """

  alias TDex.Wrapper

  def connect(args) do
    Wrapper.taos_connect(args.hostname, args.username, args.password, args.database, args.port)
  end

  def query(_conn, _statement) do
    :ok
  end

  def stmt_prepare(conn, sql) do
    Wrapper.taos_stmt_prepare(conn, sql)
  end

  def stmt_execute(stmt, spec, data) do
    Wrapper.taos_stmt_execute(stmt, spec, data)
  end

  def stmt_free(stmt) do
    Wrapper.taos_stmt_free(stmt)
  end

  def stop(conn) do
    Wrapper.taos_close(conn)
  end
end
