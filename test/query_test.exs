defmodule QueryTest do
  use ExUnit.Case
  use TDex.Timestamp

  setup _context do
    opts = [database: "tdex_test", backoff_type: :stop, max_restarts: 0, protocol: :native, pool_size: 1]
    {:ok, pid} = TDex.start_link(opts)
    {:ok, %{pid: pid, options: opts}}
  end

  test "decode basic types", context do
    assert {:ok, %TDex.Result{rows: [%{null: nil}]}} = TDex.query(context.pid, "SELECT NULL", nil)
    assert {:ok, %TDex.Result{rows: [%{false: false, true: true}]}} = TDex.query(context.pid, "SELECT true, false", nil)
    assert {:ok, %TDex.Result{rows: [%{"'e'" => "e"}]}} = TDex.query(context.pid, "SELECT 'e'", nil, keys: :string)
    assert {:ok, %TDex.Result{rows: [%{"42" => 42}]}} = TDex.query(context.pid, "SELECT 42", nil, keys: :string)
    assert {:ok, %TDex.Result{rows: [%{"42.0" => 42.0}]}} = TDex.query(context.pid, "SELECT 42.0", nil, keys: :string)
    assert {:ok, %TDex.Result{rows: [%{"'NaN'" => "NaN"}]}} = TDex.query(context.pid, "SELECT 'NaN'", nil, keys: :string)
    assert {:ok, %TDex.Result{rows: [%{"'inf'" => "inf"}]}} = TDex.query(context.pid, "SELECT 'inf'", nil, keys: :string)
    assert {:ok, %TDex.Result{rows: [%{"'-inf'" => "-inf"}]}} = TDex.query(context.pid, "SELECT '-inf'", nil, keys: :string)
    assert {:ok, %TDex.Result{rows: [%{"'ẽ'" => "ẽ"}]}} = TDex.query(context.pid, "SELECT 'ẽ'", nil, keys: :string)
    assert {:ok, %TDex.Result{rows: [%{"'ẽric'" => "ẽric"}]}} = TDex.query(context.pid, "SELECT 'ẽric'", nil, keys: :string)
    assert {:ok, %TDex.Result{rows: [%{"'\\001\\002\\003'" => "001002003"}]}} = TDex.query(context.pid, "SELECT '\\001\\002\\003'", nil, keys: :string)
  end

  @tag wip: true
  test "placeholder query", context do
    assert {:ok, %TDex.Result{rows: [%{"1" => 1}]}} == TDex.query(context.pid, "SELECT ?", [%{"1" => 1}], spec: [{"1", :INT}])
  end

  test "result struct", context do
    assert {:ok, %TDex.Result{rows: [%{a: 123, b: 456}]}} = TDex.query(context.pid, "SELECT 123 AS a, 456 AS b", nil)
  end

  test "delete", context do
    assert {:ok, %TDex.Result{rows: []}} = TDex.query(context.pid, "DROP TABLE IF EXISTS test", [])
  end

  test "insert", context do
    assert :ok == TDex.query(context.pid, "DROP TABLE IF EXISTS test", [])
    assert :ok == TDex.query(context.pid, "CREATE TABLE IF NOT EXISTS test (ts TIMESTAMP, text VARCHAR(255))", [])
    assert :ok == TDex.query(context.pid, "SELECT * FROM test", [])
    assert :ok == TDex.query(context.pid, "INSERT INTO test VALUES (?, ?)", [~X[2018-11-15 10:00:00.000Z], "hoang"], [])
    assert [%{text: "hoang", ts: ~X[2018-11-15 10:00:00.000Z]}] == TDex.query(context.pid, "SELECT * FROM test LIMIT 1", [])
  end

  test "update", context do
    assert :ok == TDex.query(context.pid, "CREATE TABLE IF NOT EXISTS test (ts TIMESTAMP, text VARCHAR(255))", [])
    assert :ok == TDex.query(context.pid, "INSERT INTO test VALUES (?, ?)", [~X[2018-11-15 10:00:00.000Z], "hoang1"], [])
    assert [%{text: "hoang1", ts: ~X[2018-11-15 10:00:00.000Z]}] == TDex.query(context.pid, "SELECT * FROM test LIMIT 1", [])
  end

  test "multi row result struct", context do
    assert :ok == TDex.query(context.pid, "CREATE TABLE IF NOT EXISTS test1 (ts TIMESTAMP, text VARCHAR(255))", [])
    assert :ok == TDex.query(context.pid, "INSERT INTO test1 VALUES (?, ?)", [~X[2018-11-15 10:00:00.000Z], "hoang1"], [])
    assert :ok == TDex.query(context.pid, "INSERT INTO test1 VALUES (?, ?)", [~X[2018-11-16 10:00:00.000Z], "hoang2"], [])
    assert [
      %{text: "hoang1", ts: ~X[2018-11-15 10:00:00.000Z]},
      %{text: "hoang2", ts: ~X[2018-11-16 10:00:00.000Z]}
    ] == TDex.query(context.pid, "SELECT * FROM test1", [])
  end
end
