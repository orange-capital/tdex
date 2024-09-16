defmodule TDex.Wrapper do
  @moduledoc """
  `TDex.Wrapper` for calling taos c/c++ api
  """

  @compile {:autoload, false}
  @on_load {:load_nifs, 0}
  @vsn "0.1.0"

  @opaque conn_t :: any | none
  @opaque stmt_t :: any | none
  @opaque query_resp_t :: any | none

  def load_nifs() do
    path = :filename.join(:code.priv_dir(:tdex), ~C"lib_taos_nif")
    :erlang.load_nif(path, @vsn)
  end

  @spec taos_connect(ip :: String.t(), port :: non_neg_integer, user :: String.t(), passwd:: {:md5, String.t()} | String.t(), db :: String.t()) :: {:ok, conn_t} | {:error, term}
  def taos_connect(ip, port, user, passwd, db) do
    case passwd do
      {:md5, hash} ->
        taos_connect_nif(nif_string(ip), port, nif_string(user), nif_string(hash), 1, nif_string(db))
      _ ->
        taos_connect_nif(nif_string(ip), port, nif_string(user), nif_string(passwd), 0, nif_string(db))
    end
  end

  @spec taos_connect_nif(ip :: String.t(), port :: non_neg_integer, user :: String.t(), passwd :: String.t(), is_md5 :: integer, db :: String.t()) :: conn_t
  def taos_connect_nif(_ip, _port, _user, _pass, _is_md5, _db) do
    raise "taos_connect_nif not implemented"
  end

  @spec taos_close(conn :: conn_t) :: no_return
  def taos_close(_conn) do
    raise "taos_close not implemented"
  end

  @spec taos_options(op :: 0..3, value :: String.t()) :: no_return
  def taos_options(op, value) do
    taos_options_nif(op, nif_string(value))
  end
  def taos_options_nif(_op, _value) do
    raise "taos_options not implemented"
  end

  @spec taos_select_db(conn :: conn_t, db :: String.t()) :: no_return
  def taos_select_db(conn, db) do
    taos_select_db_nif(conn, nif_string(db))
  end
  def taos_select_db_nif(_conn, _db) do
    raise "taos_select_db not implemented"
  end

  @spec taos_get_current_db(conn :: conn_t) :: String.t()
  def taos_get_current_db(_conn) do
    raise "taos_get_current_db not implemented"
  end

  @spec taos_get_server_info(conn :: conn_t) :: String.t()
  def taos_get_server_info(_conn) do
    raise "taos_get_server_info not implemented"
  end

  @spec taos_get_client_info() :: String.t()
  def taos_get_client_info() do
    raise "taos_get_client_info not implemented"
  end

  @spec taos_query_free(resp :: query_resp_t) :: no_return
  def taos_query_free(_resp) do
    raise "taos_query_free not implemented"
  end

  @spec taos_kill_query(conn :: conn_t) :: no_return
  def taos_kill_query(_conn) do
    raise "taos_kill_query not implemented"
  end

  @spec taos_query(conn :: conn_t, sql :: String.t()) :: {:ok, tuple} | {:error, term}
  def taos_query(conn, sql) do
    taos_query_nif(conn, nif_string(sql))
  end
  def taos_query_nif(_conn, _sql) do
    raise "taos_query not implemented"
  end

  @spec taos_query_a(conn :: conn_t, sql :: String.t(), cb_pid :: pid, cb_id :: non_neg_integer) :: no_return
  def taos_query_a(conn, sql, cb_pid, cb_id) do
    taos_query_a_nif(conn, nif_string(sql), cb_pid, cb_id)
  end
  def taos_query_a_nif(_conn, _sql, _cb_pid, _cb_id) do
    raise "taos_query_a not implemented"
  end

  @spec taos_stmt_free(stmt :: stmt_t) :: no_return
  def taos_stmt_free(_stmt) do
    raise "taos_stmt_free not loaded"
  end

  @spec taos_stmt_prepare(conn ::conn_t, sql :: String.t()) :: {:ok, stmt_t} | {:error, term}
  def taos_stmt_prepare(_conn, _sql) do
    raise "taos_stmt_prepare not loaded"
  end

  @spec taos_stmt_execute(stmt :: stmt_t) :: {:ok, tuple, list} | {:error, term}
  def taos_stmt_execute(_stmt) do
    raise "taos_stmt_execute not loaded"
  end

  @spec taos_stmt_execute_a(stmt :: stmt_t, cb_pid :: pid, cb_id :: non_neg_integer) :: :ok | {:error, term}
  def taos_stmt_execute_a(_stmt, _cb_pid, _cb_id) do
    raise "taos_stmt_execute_a not loaded"
  end

  defp nif_string(value) do
    if is_list(value), do: to_string(value) <> <<0>>, else: value <> <<0>>
  end

  @option_value_map [TSDB_OPTION_LOCALE: 0, TSDB_OPTION_CHARSET: 1, TSDB_OPTION_TIMEZONE: 2, TSDB_OPTION_CONFIGDIR: 3]
  Enum.each(@option_value_map, fn {name, value} ->
    def option_value(unquote(name)), do: unquote(value)
  end)
  Enum.each(@option_value_map, fn {name, value} ->
    def option_name(unquote(value)), do: unquote(name)
  end)

  @data_type_map [
    TSDB_DATA_TYPE_NULL: 0,
    TSDB_DATA_TYPE_BOOL: 1,
    TSDB_DATA_TYPE_TINYINT: 2,
    TSDB_DATA_TYPE_SMALLINT: 3,
    TSDB_DATA_TYPE_INT: 4,
    TSDB_DATA_TYPE_BIGINT: 5,
    TSDB_DATA_TYPE_FLOAT: 6,
    TSDB_DATA_TYPE_DOUBLE: 7,
    TSDB_DATA_TYPE_BINARY: 8,
    TSDB_DATA_TYPE_TIMESTAMP: 9,
    TSDB_DATA_TYPE_NCHAR: 10,
    TSDB_DATA_TYPE_UTINYINT: 11,
    TSDB_DATA_TYPE_USMALLINT: 12,
    TSDB_DATA_TYPE_UINT: 13,
    TSDB_DATA_TYPE_UBIGINT: 14,
    TSDB_DATA_TYPE_JSON: 15,
    TSDB_DATA_TYPE_VARBINARY: 16,
    TSDB_DATA_TYPE_DECIMAL: 17,
    TSDB_DATA_TYPE_BLOB: 18,
    TSDB_DATA_TYPE_MEDIUMBLOB: 19,
    TSDB_DATA_TYPE_GEOMETRY: 20
  ]
  Enum.each(@data_type_map, fn {name, value} ->
    def data_type_value(unquote(name)), do: unquote(value)
  end)
  def data_type_value(:TSDB_DATA_TYPE_VARCHAR), do: 8

  Enum.each(@data_type_map, fn {name, value} ->
    def data_type_name(unquote(value)), do: unquote(name)
  end)
end
