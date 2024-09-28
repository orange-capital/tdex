# TDex

TDengine driver for Elixir.

Documentation: 

## Install TDengine C/C++ driver
- [Install Client Driver](https://docs.tdengine.com/reference/connector/#Install-Client-Driver)

## Dev
+ Valgrind with Wrap
```elixir
    {:ok, conn} = TDex.Wrapper.taos_connect("127.0.0.1", 6030, "root", "taosdata", "ticks")
    sql = "CREATE STABLE IF NOT EXISTS fx_tick (ts TIMESTAMP, ts_recv TIMESTAMP, ts_update TIMESTAMP, bid FLOAT, ask FLOAT, `last` FLOAT, bid_volume FLOAT, ask_volume FLOAT, flag UTINYINT) TAGS (symbol VARCHAR(16)}, broker_id UINT)"
    TDex.Wrapper.taos_query(conn, sql)
    
    :ok = TDex.Wrapper.taos_close(conn)
    :init.stop()
```


+ Valgrind with DbConnection
```elixir
  opts = [database: "ticks", backoff_type: :stop, max_restarts: 0, protocol: :native, pool_size: 4]
  {:ok, pid} = TDex.start_link(opts)
  sql = "CREATE STABLE IF NOT EXISTS fx_tick (ts TIMESTAMP, ts_recv TIMESTAMP, ts_update TIMESTAMP, bid FLOAT, ask FLOAT, `last` FLOAT, bid_volume FLOAT, ask_volume FLOAT, flag UTINYINT) TAGS (symbol VARCHAR(16)}, broker_id UINT)"
  TDex.query(pid, sql, nil)

  :init.stop()
```

## Examples

### 1. Connect TDengine
#### 1.1. Use params
```elixir
# native
{:ok, pid} = TDex.start_link(protocol: :native, hostname: "localhost", port: 6030, username: "root", password: "taosdata", database: "tdex_test", pool_size: 1)
```

#### 1.2. Use file config
+ Where the configuration for the Repo must be in your application environment, usually defined in your config/config.exs
```elixir
# native connect
config :tdex, TDex.Repo,
  protocol: :native,
  username: "root",
  database: "test",
  hostname: "127.0.0.1",
  password: "taosdata",
  port: 6030,
  pool_size: 16
```

+ After configuration is complete, run the command:
```elixir
{:ok, pid} = TDex.start_link()
```

### 2. Query

## Install
```elixir
{:tdex, git: "git@github.com:orange-capital/tdex.git", branch: "main"}
```