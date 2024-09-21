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
  def taos_stmt_prepare(conn, sql) do
    taos_stmt_prepare_nif(conn, nif_string(sql))
  end
  def taos_stmt_prepare_nif(_conn, _sql) do
    raise "taos_stmt_prepare not loaded"
  end

  @spec taos_stmt_execute(stmt :: stmt_t, data_spec :: tuple, data :: tuple) :: {:ok, tuple, list} | {:error, term}
  def taos_stmt_execute(stmt, spec, data) do
    taos_stmt_execute_nif(stmt, stmt_format_spec(spec), stmt_format_data(spec, data), length(data))
  end
  def taos_stmt_execute_nif(_stmt, _spec, _data, _data_size) do
    raise "taos_stmt_execute_nif not loaded"
  end

  def stmt_format_spec(specs) do
    Enum.map(specs, fn {_name, type} ->
      if is_atom(type), do: data_type_value(type), else: data_type_value(elem(type, 0))
    end)
    |> List.to_tuple()
  end

  def stmt_format_data(specs, data) do
    Enum.map(specs, fn {row_name, row_type} ->
      {acc_bin, acc_len} = stmt_format_data_by_type(row_name, row_type, data)
      acc_len = Enum.reverse(acc_len) |> List.to_tuple()
      case row_type do
        {_var_type, var_len} ->
          {var_len, acc_len, acc_bin}
        _ ->
          acc_bin
      end
    end)
    |> List.to_tuple()
  end

  defp size_of_iodata(row_val) when is_binary(row_val), do: byte_size(row_val)
  defp size_of_iodata(%TDex.Timestamp{}), do: 8
  defp size_of_iodata(row_val), do: length(row_val)

  defp stmt_format_data_by_type(row_name, row_type, data) do
    Enum.reduce(data, {"", []}, fn row, {acc, acc_len} ->
      case Map.get(row, row_name) do
        row_val when row_val != nil ->
          {acc <> encode_data(row_type, row_val), [size_of_iodata(row_val)| acc_len]}
        nil when (acc == "" or acc == nil) ->
          {nil, acc_len}
        nil ->
          raise "All stmt row is not same type"
      end
    end)
  end

  defp nif_string(value) do
    if is_list(value), do: to_string(value) <> <<0>>, else: value <> <<0>>
  end

  defp encode_data(:NULL, _value), do: ""
  defp encode_data(:BOOL, value) do
    if value, do: encode_data(:UTINYINT, 1), else: encode_data(:UTINYINT, 0)
  end
  defp encode_data(:TINYINT, value), do: <<value>>
  defp encode_data(:UTINYINT, value), do: <<value>>
  defp encode_data(:SMALLINT, value), do: <<value :: 16-little-signed>>
  defp encode_data(:USMALLINT, value), do: <<value :: 16-little>>
  defp encode_data(:INT, value), do: <<value :: 32-little-signed>>
  defp encode_data(:UINT, value), do: <<value :: 32-little>>
  defp encode_data(:BIGINT, value), do: <<value :: 64-little-signed>>
  defp encode_data(:UBIGINT, value), do: <<value :: 64-little>>
  defp encode_data(:FLOAT, value), do: <<value :: 32-little-float>>
  defp encode_data(:DOUBLE, value), do: <<value :: 64-little-float>>
  defp encode_data({:TIMESTAMP, unit}, value) when is_map(value) do
    epoch = TDex.Timestamp.to_unix(value, unit)
    <<epoch :: 64-little-signed>>
  end
  defp encode_data(:TIMESTAMP, value) when is_map(value) do
    epoch = TDex.Timestamp.to_unix(value, :millisecond)
    <<epoch :: 64-little-signed>>
  end
  defp encode_data(:TIMESTAMP, value) when is_integer(value), do: <<value :: 64-little-signed>>
  defp encode_data({var_type, var_len}, value) when is_list(value) do
    encode_data({var_type, var_len}, :erlang.iolist_to_binary(value))
  end
  defp encode_data({_var_type, var_len}, value) when is_binary(value) do
    pad_or_trunc(value, var_len)
  end

  defp pad_or_trunc(value, var_len) do
    value_size = byte_size(value)
    pad_size = var_len - value_size
    cond do
      pad_size == 0 -> value
      pad_size > 0 -> value <> :binary.copy(<<0>>, pad_size)
      true -> :binary.part(value, 0, var_len)
    end
  end

  def precision_to_unit(0), do: :millisecond
  def precision_to_unit(1), do: :microsecond
  def precision_to_unit(2), do: :nanosecond

  @option_value_map [TSDB_OPTION_LOCALE: 0, TSDB_OPTION_CHARSET: 1, TSDB_OPTION_TIMEZONE: 2, TSDB_OPTION_CONFIGDIR: 3]
  Enum.each(@option_value_map, fn {name, value} ->
    def option_value(unquote(name)), do: unquote(value)
  end)
  Enum.each(@option_value_map, fn {name, value} ->
    def option_name(unquote(value)), do: unquote(name)
  end)

  @data_type_map [
    NULL: 0,
    BOOL: 1,
    TINYINT: 2,
    SMALLINT: 3,
    INT: 4,
    BIGINT: 5,
    FLOAT: 6,
    DOUBLE: 7,
    BINARY: 8,
    TIMESTAMP: 9,
    NCHAR: 10,
    UTINYINT: 11,
    USMALLINT: 12,
    UINT: 13,
    UBIGINT: 14,
    JSON: 15,
    VARBINARY: 16,
    DECIMAL: 17,
    BLOB: 18,
    MEDIUMBLOB: 19,
    GEOMETRY: 20
  ]
  Enum.each(@data_type_map, fn {name, value} ->
    def data_type_value(unquote(name)), do: unquote(value)
  end)
  def data_type_value(:VARCHAR), do: 8

  Enum.each(@data_type_map, fn {name, value} ->
    def data_type_name(unquote(value)), do: unquote(name)
  end)
end
