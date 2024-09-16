defmodule TDex.Native do
  alias TDex.{Wrapper, Binary, Native.Rows}

  def connect(args) do
    Wrapper.taos_connect(args.hostname, args.username, args.password, args.database, args.port)
  end

  def query(conn, statement) do
    {:ok, res} = Wrapper.taos_query(conn, :erlang.binary_to_list(statement))
    try do
      {:ok, 0} = Wrapper.taos_errno(res)
      {:ok, fields} = Wrapper.taos_fetch_fields(res)
      {:ok, precision} = Wrapper.taos_result_precision(res)
      fieldNames = Binary.parse_field(fields, [])
      Rows.read_row(res, fieldNames, precision, [])
    catch _, _ex ->
      {:ok, err_msg} = Wrapper.taos_errstr(res)
      {:error, %TDex.Error{message: err_msg}}
    after
      Wrapper.taos_free_result(res)
    end
  end

  def stmt_prepare(conn, sql) do
    Wrapper.taos_stmt_prepare(conn, sql)
  end

  def stmt_execute(stmt) do
    Wrapper.taos_stmt_execute(stmt)
  end

  def stmt_free(stmt) do
    Wrapper.taos_stmt_free(stmt)
  end

  def stop(conn) do
    Wrapper.taos_kill_query(conn)
    Wrapper.taos_close(conn)
  end
end
