defmodule Tdex.Wrapper do
  @on_load :load_nifs

  def load_nifs do
    # path = :filename.join(:code.priv_dir(:tdex), 'lib_taos_nif')
    :erlang.load_nif("/home/hoangbui/Code/tdex/c_src/lib_taos_nif", 0)
  end

  def taos_connect(_ip, _user, _pass, _db, _port) do
    raise "taos_connect not implemented"
  end

  def taos_select_db(_connect, _db) do
    raise "taos_select_db not implemented"
  end

  def taos_fetch_fields(_res) do
    raise "taos_fetch_fields not implemented"
  end

  def taos_field_count(_res) do
    raise "taos_field_count not implemented"
  end

  def taos_print_row(_row, _field, _num_fields) do
    raise "taos_print_row not implemented"
  end

  def taos_fetch_raw_block(_res) do
    raise "taos_fetch_raw_block not implemented"
  end

  def taos_query(_connect, _sql) do
    raise "taos_query_a not implemented"
  end

  def taos_errstr(_res) do
    raise "taos_errno not implemented"
  end

  def taos_errno(_res) do
    raise "taos_errstr not implemented"
  end

  def taos_fetch_row(_res) do
    raise "taos_fetch_row not implemented"
  end

  def taos_query_a(_connect, _sql, _callback, _params) do
    raise "taos_query_a not implemented"
  end

  def taos_close(_connect) do
    raise "taos_close not implemented"
  end
end
