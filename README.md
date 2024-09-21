# TDex

TDengine driver for Elixir.

Documentation: 

## Install TDengine C/C++ driver
- [Install Client Driver](https://docs.tdengine.com/reference/connector/#Install-Client-Driver)

## Dev

```elixir
  {:ok, conn} = TDex.Wrapper.taos_connect "localhost", 6030, "root", "taosdata", "tdex_test"
  TDex.Wrapper.taos_get_current_db(conn)
  TDex.Wrapper.taos_query conn, "CREATE DATABASE IF NOT EXISTS test2"
  TDex.Wrapper.taos_select_db conn, "test2"
  TDex.Wrapper.taos_get_current_db(conn)
  TDex.Wrapper.taos_select_db conn, "tdex_test"
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