defmodule WrapperTest do
  use ExUnit.Case
  use TDex.Timestamp

  test "login ok", _context do
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    {:ok, current_db} = TDex.Wrapper.taos_get_current_db(conn)
    assert current_db == "tdex_test"
    assert  {:ok, {0, 0, []}, []} = TDex.Wrapper.taos_query(conn, "CREATE DATABASE IF NOT EXISTS test2")
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

  test "select_no_tab", _context do
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    {:ok, {0, 1, [{~c"'ẽ'", 8}]}, [["ẽ"]]} = TDex.Wrapper.taos_query(conn, "SELECT 'ẽ'")
    :ok = TDex.Wrapper.taos_close(conn)
  end

  @tag wip: true
  test "sync_api", _context do
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    {:ok, {0, 0, []}, []} = TDex.Wrapper.taos_query(conn, "DROP TABLE IF EXISTS test1")
    {:ok, {0, 0, []}, []} = TDex.Wrapper.taos_query(conn, "CREATE TABLE IF NOT EXISTS test1 (ts TIMESTAMP, text VARCHAR(255))")
    {:ok, {0, 0, [{~c"ts", 9}, {~c"text", 8}]}, []} = TDex.Wrapper.taos_query(conn, "SELECT * from test1")
     {:ok, stmt} = TDex.Wrapper.taos_stmt_prepare(conn, "INSERT INTO test1 VALUES (?, ?)")
    spec = [{:ts, :TIMESTAMP}, {:text, {:VARCHAR, 255}}]
    data = [
      %{ts: ~X[2018-11-15 10:00:00.000Z], text: "hoang1"},
      %{ts: ~X[2018-11-15 10:00:00.001Z], text: "hoang2"}
    ]
    {:ok, {0, 0, []}, []} = TDex.Wrapper.taos_stmt_execute({conn, stmt}, spec, data)
    :ok = TDex.Wrapper.taos_stmt_close({conn, stmt})
    :ok = TDex.Wrapper.taos_close(conn)
  end

end
