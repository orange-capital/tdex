defmodule WrapperTest do
  use ExUnit.Case
  use TDex.Timestamp

  test "login ok", _context do
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    {:ok, current_db} = TDex.Wrapper.taos_get_current_db(conn)
    assert current_db == "tdex_test"
    ret = TDex.Wrapper.taos_query(conn, "CREATE DATABASE IF NOT EXISTS test2")
    assert ret == {:ok, {0, 0, {}}, []}
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

  def recv_taos_async() do
    receive do
      msg -> msg
    after
      2000 ->
        :timeout
    end
  end

  test "async_api", _context do
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    :ok = TDex.Wrapper.taos_query_a(conn, "DROP TABLE IF EXISTS test1", self(), 1)
    {:taos_reply, 1, {:ok, {0, 0, {}}}} = recv_taos_async()
    :ok = TDex.Wrapper.taos_query_a(conn, "CREATE TABLE IF NOT EXISTS test1 (ts TIMESTAMP, text VARCHAR(255))", self(), 2)
    {:taos_reply, 2, {:ok, {0, 0, {}}}} = recv_taos_async()
    :ok = TDex.Wrapper.taos_query_a(conn, "SELECT * from test1", self(), 3)
    {:taos_reply, 3, {:ok, {0, 0, {{~c"ts", 9}, {~c"text", 8}}}}} = recv_taos_async()
    :ok = TDex.Wrapper.taos_close(conn)
  end

  test "async_select_no_tab", _context do
    # Seem async api not working for this case
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    :ok = TDex.Wrapper.taos_query_a(conn, "SELECT NULL", self(), 1)
    recv_taos_async()
    recv_taos_async()
    :ok = TDex.Wrapper.taos_query_a(conn, "SELECT 'e'", self(), 1)
    recv_taos_async()
    recv_taos_async()
    :ok = TDex.Wrapper.taos_close(conn)
  end

  @tag dev: true
  test "select_no_tab", _context do
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    {:ok, {0, 1, {{~c"'e'", 8}}}, [{"e"}]} = TDex.Wrapper.taos_query(conn, "SELECT 'ẽ'")
    :ok = TDex.Wrapper.taos_close(conn)
  end

  test "sync_api", _context do
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "tdex_test")
    {:ok, {0, 0, {}}, []} = TDex.Wrapper.taos_query(conn, "DROP TABLE IF EXISTS test1")
    {:ok, {0, 0, {}}, []} = TDex.Wrapper.taos_query(conn, "CREATE TABLE IF NOT EXISTS test1 (ts TIMESTAMP, text VARCHAR(255))")
    {:ok, {0, 0, {{~c"ts", 9}, {~c"text", 8}}}, []} = TDex.Wrapper.taos_query(conn, "SELECT * from test1")
     {:ok, stmt} = TDex.Wrapper.taos_stmt_prepare(conn, "INSERT INTO test1 VALUES (?, ?)")
    spec = [{:ts, :TIMESTAMP}, {:text, {:VARCHAR, 255}}]
    data = [
      %{ts: ~X[2018-11-15 10:00:00.000Z], text: "hoang1"},
      %{ts: ~X[2018-11-15 10:00:00.001Z], text: "hoang2"}
    ]
    {:ok, {0, 0, {}}, []} = TDex.Wrapper.taos_stmt_execute(stmt, spec, data)
    :ok = TDex.Wrapper.taos_stmt_free(stmt)
    :ok = TDex.Wrapper.taos_close(conn)
  end

end
