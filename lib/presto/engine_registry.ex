defmodule Presto.EngineRegistry do
  @moduledoc """
  Registry for named Presto rule engines.

  Provides a centralized registry for engine processes, enabling:
  - Named engine lookup and management
  - Automatic cleanup on engine process death
  - Engine discovery and enumeration
  - Health monitoring and status tracking

  ## Usage

      # Register an engine with a name
      :ok = Presto.EngineRegistry.register_engine(:payroll, engine_pid)

      # Lookup an engine by name
      {:ok, pid} = Presto.EngineRegistry.lookup_engine(:payroll)

      # List all registered engines
      engines = Presto.EngineRegistry.list_engines()
  """

  use GenServer
  require Logger

  alias Presto.Telemetry

  @type engine_name :: atom()
  @type engine_info :: %{
          pid: pid(),
          name: engine_name(),
          registered_at: DateTime.t(),
          engine_id: String.t()
        }

  ## Client API

  @doc """
  Starts the engine registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers an engine with a name.

  ## Examples

      :ok = Presto.EngineRegistry.register_engine(:payroll_engine, pid)
  """
  @spec register_engine(engine_name(), pid()) :: :ok | {:error, term()}
  def register_engine(name, pid) when is_atom(name) and is_pid(pid) do
    GenServer.call(__MODULE__, {:register, name, pid})
  end

  @doc """
  Looks up an engine by name.

  ## Examples

      {:ok, pid} = Presto.EngineRegistry.lookup_engine(:payroll_engine)
      :error = Presto.EngineRegistry.lookup_engine(:nonexistent)
  """
  @spec lookup_engine(engine_name()) :: {:ok, pid()} | :error
  def lookup_engine(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:lookup, name})
  end

  @doc """
  Unregisters an engine by name.

  ## Examples

      :ok = Presto.EngineRegistry.unregister_engine(:payroll_engine)
  """
  @spec unregister_engine(engine_name()) :: :ok
  def unregister_engine(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:unregister, name})
  end

  @doc """
  Lists all registered engines.

  Returns a list of engine information maps.
  """
  @spec list_engines() :: [engine_info()]
  def list_engines do
    GenServer.call(__MODULE__, :list_engines)
  end

  @doc """
  Gets detailed information about a registered engine.
  """
  @spec get_engine_info(engine_name()) :: {:ok, engine_info()} | :error
  def get_engine_info(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:get_info, name})
  end

  @doc """
  Checks if an engine name is registered.
  """
  @spec registered?(engine_name()) :: boolean()
  def registered?(name) when is_atom(name) do
    case lookup_engine(name) do
      {:ok, _pid} -> true
      :error -> false
    end
  end

  @doc """
  Gets the count of registered engines.
  """
  @spec count() :: non_neg_integer()
  def count do
    GenServer.call(__MODULE__, :count)
  end

  @doc """
  Performs health check on all registered engines.
  """
  @spec health_check() :: %{
          total: non_neg_integer(),
          alive: non_neg_integer(),
          dead: non_neg_integer(),
          details: [%{name: engine_name(), status: :alive | :dead, pid: pid()}]
        }
  def health_check do
    GenServer.call(__MODULE__, :health_check)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Presto.EngineRegistry", opts: inspect(opts))

    # Setup ETS table for fast lookups
    :ets.new(:engine_registry, [
      :set,
      :named_table,
      :public,
      {:read_concurrency, true}
    ])

    state = %{
      engines: %{},
      monitors: %{},
      stats: %{
        total_registrations: 0,
        current_count: 0
      }
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:register, name, pid}, _from, state) do
    case Map.get(state.engines, name) do
      nil ->
        # Check if the process is alive
        if Process.alive?(pid) do
          # Monitor the process for automatic cleanup
          monitor_ref = Process.monitor(pid)

          # Get engine ID from the process
          engine_id = get_engine_id(pid)

          engine_info = %{
            pid: pid,
            name: name,
            registered_at: DateTime.utc_now(),
            engine_id: engine_id,
            monitor_ref: monitor_ref
          }

          # Store in both ETS and state for different access patterns
          :ets.insert(:engine_registry, {name, engine_info})

          new_engines = Map.put(state.engines, name, engine_info)
          new_monitors = Map.put(state.monitors, monitor_ref, name)

          new_stats = %{
            state.stats
            | total_registrations: state.stats.total_registrations + 1,
              current_count: state.stats.current_count + 1
          }

          new_state = %{
            state
            | engines: new_engines,
              monitors: new_monitors,
              stats: new_stats
          }

          # Emit telemetry event (optional)
          emit_telemetry_safe(:engine_registered, %{
            name: name,
            pid: pid,
            engine_id: engine_id
          })

          Logger.info("Engine registered",
            name: name,
            pid: inspect(pid),
            engine_id: engine_id
          )

          {:reply, :ok, new_state}
        else
          Logger.warning("Attempted to register dead process",
            name: name,
            pid: inspect(pid)
          )

          {:reply, {:error, :process_not_alive}, state}
        end

      _existing ->
        Logger.warning("Engine name already registered",
          name: name,
          existing_pid: inspect(state.engines[name].pid)
        )

        {:reply, {:error, :already_registered}, state}
    end
  end

  @impl GenServer
  def handle_call({:lookup, name}, _from, state) do
    case :ets.lookup(:engine_registry, name) do
      [{^name, engine_info}] ->
        # Double-check that the process is still alive
        if Process.alive?(engine_info.pid) do
          {:reply, {:ok, engine_info.pid}, state}
        else
          # Clean up dead entry
          cleanup_dead_engine(name, state)
          {:reply, :error, state}
        end

      [] ->
        {:reply, :error, state}
    end
  end

  @impl GenServer
  def handle_call({:unregister, name}, _from, state) do
    case Map.get(state.engines, name) do
      nil ->
        # Already unregistered
        {:reply, :ok, state}

      engine_info ->
        new_state = remove_engine(name, engine_info, state)

        Logger.info("Engine unregistered", name: name, pid: inspect(engine_info.pid))
        {:reply, :ok, new_state}
    end
  end

  @impl GenServer
  def handle_call(:list_engines, _from, state) do
    # Return list from ETS for consistency
    engines =
      :ets.tab2list(:engine_registry)
      |> Enum.map(fn {_name, info} -> Map.drop(info, [:monitor_ref]) end)
      |> Enum.filter(fn info -> Process.alive?(info.pid) end)

    {:reply, engines, state}
  end

  @impl GenServer
  def handle_call({:get_info, name}, _from, state) do
    case :ets.lookup(:engine_registry, name) do
      [{^name, engine_info}] ->
        if Process.alive?(engine_info.pid) do
          clean_info = Map.drop(engine_info, [:monitor_ref])
          {:reply, {:ok, clean_info}, state}
        else
          cleanup_dead_engine(name, state)
          {:reply, :error, state}
        end

      [] ->
        {:reply, :error, state}
    end
  end

  @impl GenServer
  def handle_call(:count, _from, state) do
    {:reply, state.stats.current_count, state}
  end

  @impl GenServer
  def handle_call(:health_check, _from, state) do
    engines = :ets.tab2list(:engine_registry)

    {alive_engines, dead_engines} =
      Enum.split_with(engines, fn {_name, info} ->
        Process.alive?(info.pid)
      end)

    # Clean up dead engines
    Enum.each(dead_engines, fn {name, _info} ->
      cleanup_dead_engine(name, state)
    end)

    health_report = %{
      total: length(engines),
      alive: length(alive_engines),
      dead: length(dead_engines),
      details:
        Enum.map(engines, fn {name, info} ->
          %{
            name: name,
            status: if(Process.alive?(info.pid), do: :alive, else: :dead),
            pid: info.pid
          }
        end)
    }

    {:reply, health_report, state}
  end

  @impl GenServer
  def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
    case Map.get(state.monitors, monitor_ref) do
      nil ->
        # Unknown monitor - shouldn't happen but handle gracefully
        {:noreply, state}

      name ->
        Logger.info("Engine process terminated",
          name: name,
          pid: inspect(pid),
          reason: inspect(reason)
        )

        # Clean up the registration
        engine_info = Map.get(state.engines, name)
        new_state = remove_engine(name, engine_info, state)

        # Emit telemetry event (optional)
        if engine_info do
          emit_telemetry_safe(:engine_unregistered, %{
            name: name,
            pid: pid,
            engine_id: engine_info.engine_id,
            reason: reason
          })
        end

        {:noreply, new_state}
    end
  end

  ## Private functions

  defp get_engine_id(pid) do
    try do
      case GenServer.call(pid, :get_engine_statistics, 1000) do
        %{engine_id: engine_id} -> engine_id
        _ -> "unknown"
      end
    rescue
      _ -> "unknown"
    catch
      :exit, _ -> "unknown"
    end
  end

  defp remove_engine(name, engine_info, state) do
    if engine_info do
      # Demonitor the process
      Process.demonitor(engine_info.monitor_ref, [:flush])

      # Remove from ETS
      :ets.delete(:engine_registry, name)

      # Remove from state
      new_engines = Map.delete(state.engines, name)
      new_monitors = Map.delete(state.monitors, engine_info.monitor_ref)

      new_stats = %{
        state.stats
        | current_count: max(0, state.stats.current_count - 1)
      }

      %{
        state
        | engines: new_engines,
          monitors: new_monitors,
          stats: new_stats
      }
    else
      state
    end
  end

  defp cleanup_dead_engine(name, state) do
    case Map.get(state.engines, name) do
      nil -> state
      engine_info -> remove_engine(name, engine_info, state)
    end
  end

  # Safe telemetry emission that handles missing dependencies
  defp emit_telemetry_safe(event_type, metadata) do
    try do
      case event_type do
        :engine_registered ->
          Telemetry.emit_engine_registered(metadata.name, metadata.pid, metadata.engine_id)

        :engine_unregistered ->
          Telemetry.emit_engine_unregistered(
            metadata.name,
            metadata.pid,
            metadata.engine_id,
            metadata.reason
          )
      end
    rescue
      # Telemetry not available, continue normally
      _ -> :ok
    end
  end
end
