# Tdex


Tdengine driver for Elixir.

Documentation: 

## Examples

```iex
iex> {:ok, pid} = {:ok, pid} = Tdex.start_link(hostname: "localhost", port: 6041, username: "root", password: "taosdata", database: "test")
{:ok, #PID<0.69.0>}

iex> Tdex.query!(pid, "SELECT ts,bid  FROM tick LIMIT 10", [])
%Tdex.Result{
  code: 0,
  req_id: 2,
  rows: [%{"bid" => 1091.752, "ts" => ~U[2015-08-09 17:00:00.000Z]}],
  affected_rows: 0,
  message: ""
}

iex> Tdex.query!(pid, "SELECT ts,bid FROM tick WHERE bid = ? AND ask = ? LIMIT 10", [1, 2])
%Tdex.Result{code: 0, req_id: 3, rows: [], affected_rows: 0, message: ""}
```

## Features

## JSON support

Tdex comes with JSON support out of the box via the [Jason](https://github.com/michalmuskala/jason) library. To use it, add :jason to your dependencies:

```elixir
{:jason, "~> 1.4"}
```
