defmodule TDex.Native.Async do
  @moduledoc """
  `TDex.Native.Async` provide async handler for native
  """
  use GenServer
  require Skn.Log
  require Logger
  @worker_size 8

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: worker_name(args.id))
  end

  def init(args) do
    {:ok, %{id: args.id, queries: []}}
  end

  def handle_info(msg, state) do
    Skn.Log.error("drop info: #{inspect msg}")
    {:noreply, state}
  end

  def terminate(reason, state) do
    Skn.Log.debug("id=#{state.id} stop by #{inspect reason}")
    :ok
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
