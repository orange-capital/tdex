defmodule FuncTest do
  use ExUnit.Case
  use TDex.Timestamp

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
end