defmodule WrapperTest do
  use ExUnit.Case

  test "login ok", _context do
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    {:ok, current_db} = TDex.Wrapper.taos_get_current_db(conn)
    assert current_db == "tdex_test"
    TDex.Wrapper.taos_query(conn, "CREATE DATABASE IF NOT EXISTS test2")
    :ok = TDex.Wrapper.taos_select_db(conn, "test2")
    {:ok, current_db} = TDex.Wrapper.taos_get_current_db(conn)
    assert current_db == "test2"
    :ok = TDex.Wrapper.taos_select_db(conn, "tdex_test")
    {:ok, current_db} = TDex.Wrapper.taos_get_current_db(conn)
    assert current_db == "tdex_test"
    client_info = TDex.Wrapper.taos_get_client_info()
    server_info = TDex.Wrapper.taos_get_server_info(conn)
    assert String.contains?(client_info, "3.3.")
    assert String.contains?(server_info, "ver:3.3.")
    :ok = TDex.Wrapper.taos_close(conn)
  end

  test "login failure", _context do
    {:error, "Invalid user"} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "rootx", "taosdata", "tdex_test")
    {:error, "Authentication failure"} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata2", "tdex_test")
  end

end
