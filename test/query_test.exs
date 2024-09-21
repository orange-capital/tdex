defmodule QueryTest do
  use ExUnit.Case
  import TDex.TestHelper
  alias TDex, as: T
  use TDex.Timestamp

  setup _context do
    opts = [
      database: "tdex_test",
      backoff_type: :stop,
      max_restarts: 0,
      protocol: :native,
      pool_size: 1
    ]

    {:ok, pid} = T.start_link(opts)
    {:ok, %{pid: pid, options: opts}}
  end

  test "test timestamp" do
    t = 1702377891907976277
    t1 = ~X[2023-12-12 10:44:51.907976277Z]
    a = TDex.Timestamp.from_unix(t, :nanosecond)
    assert t1 == a
    assert t == TDex.Timestamp.to_unix(a, :nanosecond)
    t1 = ~X[2023-12-12 10:44:52.907976277Z]
    assert t1 == TDex.Timestamp.add(a, 1_000_000_000, :nanosecond)
    t1 = ~X[2023-12-12 10:44:51.907976278Z]
    assert t1 == TDex.Timestamp.add(a, 1, :nanosecond)
    t1 = ~X[2023-12-12 10:45:00.907976277Z]
    assert t1 == TDex.Timestamp.add(a, 9, :second)
    t1 = ~X[2023-12-12 11:04:51.907976277Z]
    assert t1 == TDex.Timestamp.add(a, 20, :minute)
    t1 = ~X[2023-12-13 06:44:51.907976277Z]
    assert t1 == TDex.Timestamp.add(a, 20, :hour)
    t1 = ~X[2024-01-01 10:44:51.907976277Z]
    assert t1 == TDex.Timestamp.add(a, 20, :day)
    assert TDex.Timestamp.to_string(t1) == "2024-01-01 10:44:51.907976277Z"
    assert (~X[2023-12-14 03:05:16.000001Z] |> TDex.Timestamp.to_unix()) == 1702523116000001000
  end

  test "decode basic types", context do
    assert [%{null: nil}] = query("SELECT NULL", [])
    assert [%{false: false, true: true}] = query("SELECT true, false", [])
    assert [%{~c"'e'" => "e"}] = query("SELECT 'e'", [])
    assert [%{~c"'ẽ'" => "ẽ"}] = query("SELECT 'ẽ'", [])
    assert [%{~c"42" => 42}] = query("SELECT 42", [])
    assert [%{~c"42.0" => 42.0}] = query("SELECT 42.0", [])
    assert [%{~c"'NaN'" => "NaN"}] = query("SELECT 'NaN'", [])
    assert [%{~c"'inf'" => "inf"}] = query("SELECT 'inf'", [])
    assert [%{~c"'-inf'" => "-inf"}] = query("SELECT '-inf'", [])
    assert [%{~c"'ẽric'" => "ẽric"}] = query("SELECT 'ẽric'", [])
    assert [%{~c"'ẽric'" => "ẽric"}] = query("SELECT 'ẽric'", [])
    assert [%{~c"'\\001\\002\\003'" => "001002003"}] = query("SELECT '\\001\\002\\003'", [])
  end

  test "decode numeric", context do
    assert [%{~c"42" => 42}] == query("SELECT 42", [])
    assert [%{~c"42.0" => 42.0}] == query("SELECT 42.0", [])
    assert [%{~c"1.001" =>  1.001}] == query("SELECT 1.001", [])
    assert [%{~c"0.4242" =>  0.4242}] == query("SELECT 0.4242", [])
    assert [%{~c"42.4242" =>  42.4242}] == query("SELECT 42.4242", [])
    assert [%{~c"12345.12345" =>  12345.12345}] == query("SELECT 12345.12345", [])
    assert [%{~c"0.00012345" => 0.00012345}] == query("SELECT 0.00012345", [])
    assert [%{~c"1000000000.0" => 1000000000.0}] == query("SELECT 1000000000.0", [])
    assert [%{~c"1000000000.1" => 1000000000.1}] == query("SELECT 1000000000.1", [])
    assert [%{~c"1000000000.1" => 1000000000.1}] == query("SELECT 1000000000.1", [])
    assert [%{~c"18446744073709551615" => 18446744073709551615}] == query("SELECT 18446744073709551615", [])
    assert [%{~c"123456789123456789123456789.123456789" => 123456789123456789123456789.123456789}] == query("SELECT 123456789123456789123456789.123456789", [])
    assert [%{~c"1.1234500000" => 1.1234500000}] == query("SELECT 1.1234500000", [])
    assert [%{~c"'NaN'" => "NaN"}] == query("SELECT 'NaN'", [])
  end

  test "decode name", context do
    assert [%{~c"'test'" => "test"}] == query("SELECT 'test'", [])
  end

  test "placeholder query", context do
    assert [%{~c"1" => 1}] == query("SELECT ?", [1])
    assert [%{~c"-1" => -1}] == query("SELECT ?", [-1])
    assert [%{~c"23.12" => 23.12}] == query("SELECT ?", [23.12])
    assert [%{~c"232.11111111111111" => 232.11111111111111}] == query("SELECT ?", [232.11111111111111111])
    assert [%{~c"'test'" => "test"}] == query("SELECT ?", ["test"])
    assert [%{true: true}] == query("SELECT ?", [true])
    assert [%{~c"'2000-01-01'" => "2000-01-01"}] == query("SELECT ?", [~D[2000-01-01]])
    assert [%{~c"'2018-11-15 10:00:00Z'" => "2018-11-15 10:00:00Z"}] == query("SELECT ?", [~U[2018-11-15 10:00:00Z]])
  end

  test "fail on parameter length mismatch", context do
    assert_raise UndefinedFunctionError, "function :arg_not_valid.exception/1 is undefined (module :arg_not_valid is not available)", fn ->
      query!("SELECT ?", [1,2])
    end

    assert_raise UndefinedFunctionError, "function :arg_not_valid.exception/1 is undefined (module :arg_not_valid is not available)", fn ->
      query!("SELECT 42", [1])
    end

    assert [%{~c"42" => 42}] = query("SELECT 42", [])
  end

  test "result struct", context do
    {:ok, _, res} = T.query(context[:pid], "SELECT 123 AS a, 456 AS b", [])
    assert match? %T.Result{}, res
    assert res.code == 0
    assert res.rows == [%{a: 123, b: 456}]
  end

  test "delete", context do
    assert :ok = query("DROP TABLE IF EXISTS test", [])
  end

  test "insert", context do
    assert :ok == query("DROP TABLE IF EXISTS test", [])
    assert :ok == query("CREATE TABLE IF NOT EXISTS test (ts TIMESTAMP, text VARCHAR(255))", [])
    assert :ok == query("SELECT * FROM test", [])
    assert :ok == query("INSERT INTO test VALUES (?, ?)", [~X[2018-11-15 10:00:00.000Z], "hoang"], [])
    assert [%{text: "hoang", ts: ~X[2018-11-15 10:00:00.000Z]}] == query("SELECT * FROM test LIMIT 1", [])
  end

  test "update", context do
    assert :ok == query("CREATE TABLE IF NOT EXISTS test (ts TIMESTAMP, text VARCHAR(255))", [])
    assert :ok == query("INSERT INTO test VALUES (?, ?)", [~X[2018-11-15 10:00:00.000Z], "hoang1"], [])
    assert [%{text: "hoang1", ts: ~X[2018-11-15 10:00:00.000Z]}] == query("SELECT * FROM test LIMIT 1", [])
  end

  test "multi row result struct", context do
    assert :ok == query("CREATE TABLE IF NOT EXISTS test1 (ts TIMESTAMP, text VARCHAR(255))", [])
    assert :ok == query("INSERT INTO test1 VALUES (?, ?)", [~X[2018-11-15 10:00:00.000Z], "hoang1"], [])
    assert :ok == query("INSERT INTO test1 VALUES (?, ?)", [~X[2018-11-16 10:00:00.000Z], "hoang2"], [])
    assert [
      %{text: "hoang1", ts: ~X[2018-11-15 10:00:00.000Z]},
      %{text: "hoang2", ts: ~X[2018-11-16 10:00:00.000Z]}
    ] == query("SELECT * FROM test1", [])
  end
end
