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
    key_names = if specs != nil do
      Enum.map(specs, fn {name, _type} -> name end)
    else
      if keys == :atoms do
        Enum.map(Tuple.to_list(names), fn {name, _type} -> List.to_atom(name) end)
      else
        Enum.map(Tuple.to_list(names), fn {name, _type} -> :unicode.characters_to_binary(name) end)
      end
    end
    rows = Enum.reduce(body, [], fn x, acc ->
      row = List.zip([key_names, Tuple.to_list(x)]) |> Map.new()
      [row| acc]
    end)
    %TDex.Result{
      precision: TDex.Wrapper.precision_to_unit(precision),
      affected_rows: affected_rows,
      rows: Enum.reverse(rows)
    }
  end

  @impl true
  def handle_execute(query, params, opts, %{protocol: protocol, protocol_state: protocol_state} = state) do
   result = if query.mode == :stmt do
     protocol.execute(query.cache, query.spec, params)
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
       TDex.Query.close(query)
       {:error, %TDex.Error{message: reason}, state}
   end
  catch _, exp ->
    TDex.Query.close(query)
    {:error, %TDex.Error{message: exp}, state}
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:ok, nil, state}
  end

  @impl true
  def handle_close(query, _opts, state) do
    :ok = TDex.Query.close(query)
    {:ok, nil, state}
  end
end
