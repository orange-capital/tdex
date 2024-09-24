defmodule TDex.DBConnection do
  @moduledoc """
  `TDex.DBConnection` implement DBConnection behavior
  """
  use DBConnection
  require Logger
  require Skn.Log

  defp map_keep_keys(map, keys) do
    drop_keys = Map.keys(map) -- keys
    Map.drop(map, drop_keys)
  end

  @impl true
  def connect(opts) do
    params = Map.new(opts)
    compact_params = map_keep_keys(params, [:hostname, :port, :username, :password, :database, :timeout])
    case params.protocol.connect(compact_params) do
      {:ok, protocol_state} -> {:ok, %{params: compact_params, protocol: params.protocol, protocol_state: protocol_state}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def disconnect(_error, %{protocol: protocol, protocol_state: protocol_state}) do
    if protocol_state != nil do
      protocol.close(protocol_state)
    end
    :ok
  end

  @impl true
  def checkout(state) do
    {:ok, state}
  end

  @impl true
  def handle_rollback(_opts, state) do
    {:ok, nil, state}
  end

  @impl true
  def handle_begin(_opts, state) do
    {:ok, nil, state}
  end

  @impl true
  def handle_fetch(_query, _cursor, _opts, state) do
    {:cont, nil, state}
  end

  @impl true
  def handle_declare(query, _params, _opts, state) do
    {:ok, query, nil, state}
  end

  @impl true
  def handle_commit(_opts, state) do
    {:ok, nil, state}
  end

  @impl true
  def ping(state) do
    {:ok, state}
  end

  @impl true
  def handle_status(_opts, state) do
    {:idle, state}
  end

  @impl true
  def handle_prepare(query, _opts, %{protocol: protocol, protocol_state: protocol_state} = state) do
    case query.mode do
      :stmt ->
        case protocol.prepare(protocol_state, query.statement) do
          {:ok, stmt} ->
            {:ok, %{query| cache: stmt}, state}
          {:error, reason} ->
            {:error, %TDex.Error{query: query, message: reason}, state}
        end
      _ ->
        {:ok, query, state}
    end
  end

  defp to_result(opts, specs, {precision, affected_rows, names}, body) do
    keys = Keyword.get(opts, :keys, :atoms)
    {key_names, columns} = if specs != nil do
      {Enum.map(specs, fn {name, _type} -> name end), specs}
    else
      cols = if keys == :atoms do
        Enum.map(names, fn {name, type} -> {List.to_atom(name), TDex.Wrapper.data_type_name(type)} end)
      else
        Enum.map(names, fn {name, type} -> {:unicode.characters_to_binary(name), TDex.Wrapper.data_type_name(type)} end)
      end
      {Enum.map(cols, fn {name, _type} -> name end), cols}
    end
    rows = Enum.reduce(body, [], fn x, acc ->
      row = List.zip([key_names, x]) |> Map.new()
      [row| acc]
    end) |> Enum.reverse()
    %TDex.Result{
      precision: TDex.Wrapper.precision_to_unit(precision),
      affected_rows: affected_rows,
      columns: columns,
      rows: rows
    }
  end

  @impl true
  def handle_execute(query, params, opts, %{protocol: protocol, protocol_state: protocol_state} = state) do
   result = if query.mode == :stmt do
     protocol.execute(protocol_state, query.cache, query.spec, params)
   else
    if query.async == false do
      protocol.query(protocol_state, query.statement)
    else
      protocol.query_a(protocol_state, query.statement)
    end
   end
   case result do
     {:ok, header, body} ->
       {:ok, query, to_result(opts, query.spec, header, body), state}
     {:error, reason} ->
       {:error, %TDex.Error{message: reason}, state}
   end
  catch _, exp ->
    Skn.Log.error("query exception: #{inspect __STACKTRACE__}")
    protocol.close_stmt(protocol_state, query.cache)
    {:error, %TDex.Error{message: exp}, state}
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:ok, nil, state}
  end

  @impl true
  def handle_close(query, _opts, %{protocol_state: protocol_state} = state) do
    :ok = TDex.Query.close(query, protocol_state)
    {:ok, nil, state}
  end
end
