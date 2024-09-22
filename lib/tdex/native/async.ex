defmodule TDex.Native.Async do
  @moduledoc """
  `TDex.Native.Async` provide async handler for native
  """
  use GenServer
  require Skn.Log
  require Logger
  import Skn.Util, only: [
    reset_timer: 3
  ]
  @worker_size 8

  def query(conn, sql, timeout \\ 10_000) do
    async_id = Skn.Counter.update_counter(:tdex_async_id, 1)
    GenServer.call(worker_name(async_id), {:query, conn, sql, timeout}, 10_000)
  catch
    _, exp ->
      {:error, exp}
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: worker_name(args.id))
  end

  def init(args) do
    reset_timer(:check_tick, :check_tick, 1000)
    {:ok, %{id: args.id, queries: [], query_result: %{}, query_ptr: 0}}
  end

  def handle_call({:query, conn, sql, timeout}, from, %{queries: queries, query_ptr: query_ptr} = state) do
    case TDex.Wrapper.taos_query_a(conn, sql, self(), query_ptr) do
      :ok ->
        ts_now = System.system_time(:millisecond)
        queries = List.keystore(queries, query_ptr, 0, {query_ptr, from, ts_now + timeout})
        {:noreply, %{state| queries: queries, query_ptr: update_query_ptr(query_ptr)}}
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(_msg, _from, state) do
    {:reply, {:error, :bad_api}, state}
  end

  def handle_cast(msg, state) do
    Skn.Log.error("drop info: #{inspect msg}")
    {:noreply, state}
  end

  def handle_info({:taos_data, id, rows}, %{queries: queries, query_result: query_result} = state) do
    case List.keyfind(queries, id, 0) do
      nil ->
        # drop
        {:noreply, state}
      {^id, _from, _timeout_at} ->
        body = Map.get(query_result, id, []) ++ rows
        {:noreply, %{state| query_result: Map.put(query_result, id, body)}}
    end
  end

  def handle_info({:taos_reply, id, result}, %{queries: queries, query_result: query_result} = state) do
    reply = case result do
      {:error, reason} ->
        {:error, reason}
      {:ok, header} ->
        body = Map.get(query_result, id, [])
        {:ok, header, body}
    end
    case List.keyfind(queries, id, 0) do
      nil ->
        {:noreply, %{state| query_result: Map.delete(query_result, id)}}
      {^id, from, _timeout_at} ->
        GenServer.reply(from, reply)
        {:noreply, %{state| queries: List.keydelete(queries, id, 0), query_result: Map.delete(query_result, id)}}
    end
  end

  def handle_info(:check_tick, %{queries: queries, query_result: query_result} = state) do
    reset_timer(:check_tick, :check_tick, 1000)
    ts_now = System.system_time(:millisecond)
    {queries, drop} = Enum.reduce(queries, {[], []}, fn {id, from, timeout_at}, {acc_queries, acc_drop} ->
      if ts_now > timeout_at do
        GenServer.reply(from, {:error, :timeout})
        {acc_queries, [id| acc_drop]}
      else
        {[{id, from, timeout_at}| acc_queries], acc_drop}
      end
    end)
    {:noreply, %{state| queries: queries, query_result: Map.drop(query_result, drop)}}
  end

  def handle_info(msg, state) do
    Skn.Log.error("drop info: #{inspect msg}")
    {:noreply, state}
  end

  def terminate(reason, state) do
    Skn.Log.debug("id=#{state.id} stop by #{inspect reason}")
    :ok
  end

  defp update_query_ptr(query_ptr) do
    new_query_ptr = query_ptr + 1
    if new_query_ptr >= 0xFFFFFFFFFFFFFFFF, do: 0, else: new_query_ptr
  end

  def worker_size, do: @worker_size

  Enum.each(0..(@worker_size - 1), fn x ->
    def worker_name(unquote(x)), do: unquote(String.to_atom("tdex_native_async#{x}"))
  end)
  def worker_name(id) when is_integer(id) do
    worker_name(rem(id, @worker_size))
  end
  def worker_name(id) do
    worker_name(rem(:erlang.phash2(id), @worker_size))
  end
end

defmodule TDex.Native.Sup do
  @moduledoc """
  supervisor for `TDex.Native.Async`
  """
  use Supervisor
  @name :tdex_native_sup

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: @name)
  end

  def init(_args) do
    children = Enum.map(0..(TDex.Native.Async.worker_size() - 1), fn x ->
      Supervisor.child_spec({TDex.Native.Async, %{id: x}}, id: TDex.Native.Async.worker_name(x), restart: :transient, shutdown: 5000)
    end)
    Supervisor.init(children, strategy: :one_for_one)
  end
end
