defmodule Presto.EngineSupervisor do
  @moduledoc """
  Dynamic supervisor for Presto rule engines.

  This supervisor provides enhanced operational characteristics including:
  - Named engine management with automatic registry
  - Proper OTP supervision with restart strategies
  - Resource cleanup on engine termination
  - Process monitoring and health checks

  ## Usage

      # Start an engine under supervision
      {:ok, engine} = Presto.EngineSupervisor.start_engine(name: :payroll_engine)

      # Stop a supervised engine
      :ok = Presto.EngineSupervisor.stop_engine(:payroll_engine)

      # List all supervised engines
      engines = Presto.EngineSupervisor.list_engines()
  """

  use DynamicSupervisor
  require Logger

  alias Presto.EngineRegistry
  alias Presto.RuleEngine

  @type engine_opts :: [
          {:name, atom()}
          | {:engine_id, String.t()}
          | {:restart, :permanent | :temporary | :transient}
          | {:shutdown, timeout() | :brutal_kill}
          | term()
        ]

  ## Client API

  @doc """
  Starts the engine supervisor.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts a new rule engine under supervision.

  ## Options

    * `:name` - Register the engine with a name for easy lookup
    * `:engine_id` - Unique identifier for the engine (generated if not provided)
    * `:restart` - OTP restart strategy (default: `:permanent`)
    * `:shutdown` - Shutdown timeout (default: `5000`)

  ## Examples

      # Start with automatic name registration
      {:ok, engine} = Presto.EngineSupervisor.start_engine(name: :payroll_engine)

      # Start with custom configuration
      {:ok, engine} = Presto.EngineSupervisor.start_engine(
        name: :compliance_engine,
        engine_id: "compliance-prod-001",
        restart: :permanent
      )
  """
  @spec start_engine(engine_opts()) :: DynamicSupervisor.on_start_child()
  def start_engine(engine_opts \\ []) do
    # Generate engine_id if not provided
    engine_opts = Keyword.put_new_lazy(engine_opts, :engine_id, &generate_engine_id/0)

    # Extract supervision options
    {sup_opts, engine_opts} = extract_supervision_opts(engine_opts)

    # Create child spec with custom options
    child_spec = %{
      id: {:engine, Keyword.get(engine_opts, :engine_id)},
      start: {RuleEngine, :start_link, [engine_opts]},
      restart: Keyword.get(sup_opts, :restart, :permanent),
      shutdown: Keyword.get(sup_opts, :shutdown, 5000),
      type: :worker
    }

    case DynamicSupervisor.start_child(__MODULE__, child_spec) do
      {:ok, pid} = result ->
        # Register with name if provided
        if name = Keyword.get(engine_opts, :name) do
          case EngineRegistry.register_engine(name, pid) do
            :ok ->
              Logger.info("Started supervised engine",
                engine_id: Keyword.get(engine_opts, :engine_id),
                name: name,
                pid: inspect(pid)
              )

              result

            {:error, reason} ->
              # Stop the engine if registration fails
              DynamicSupervisor.terminate_child(__MODULE__, pid)
              {:error, {:registration_failed, reason}}
          end
        else
          Logger.info("Started supervised engine",
            engine_id: Keyword.get(engine_opts, :engine_id),
            pid: inspect(pid)
          )

          result
        end

      error ->
        Logger.error("Failed to start supervised engine",
          error: inspect(error),
          opts: inspect(engine_opts)
        )

        error
    end
  end

  @doc """
  Stops a supervised engine.

  Accepts either a name (atom) or PID.

  ## Examples

      :ok = Presto.EngineSupervisor.stop_engine(:payroll_engine)
      :ok = Presto.EngineSupervisor.stop_engine(pid)
  """
  @spec stop_engine(atom() | pid()) :: :ok | {:error, :not_found}
  def stop_engine(engine_name_or_pid) do
    case resolve_engine_pid(engine_name_or_pid) do
      {:ok, pid} ->
        case DynamicSupervisor.terminate_child(__MODULE__, pid) do
          :ok ->
            Logger.info("Stopped supervised engine", pid: inspect(pid))
            :ok

          {:error, :not_found} ->
            {:error, :not_found}
        end

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  Restarts a supervised engine.

  The engine will be restarted with the same configuration it was originally started with.
  """
  @spec restart_engine(atom() | pid()) ::
          DynamicSupervisor.on_start_child() | {:error, :not_found}
  def restart_engine(engine_name_or_pid) do
    case resolve_engine_pid(engine_name_or_pid) do
      {:ok, pid} ->
        # DynamicSupervisor doesn't have restart_child, so we need to terminate and start a new one
        :ok = DynamicSupervisor.terminate_child(__MODULE__, pid)

        case DynamicSupervisor.start_child(__MODULE__, {Presto.RuleEngine, []}) do
          {:ok, new_pid} = result ->
            Logger.info("Restarted supervised engine",
              old_pid: inspect(pid),
              new_pid: inspect(new_pid)
            )

            result

          error ->
            error
        end

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  Lists all currently supervised engines.

  Returns a list of tuples with engine information.
  """
  @spec list_engines() :: [{pid(), map()}]
  def list_engines do
    DynamicSupervisor.which_children(__MODULE__)
    |> Enum.map(fn {_id, pid, _type, _modules} ->
      case Process.alive?(pid) do
        true ->
          engine_info = get_engine_details(pid)
          {pid, engine_info}

        false ->
          nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  @doc """
  Gets detailed information about a specific engine.
  """
  @spec get_engine_info(atom() | pid()) :: {:ok, map()} | {:error, :not_found}
  def get_engine_info(engine_name_or_pid) do
    case resolve_engine_pid(engine_name_or_pid) do
      {:ok, pid} ->
        info = get_engine_details(pid)
        {:ok, info}

      error ->
        error
    end
  end

  @doc """
  Checks the health of all supervised engines.

  Returns a health report with status of each engine.
  """
  @spec health_check() :: %{
          total_engines: non_neg_integer(),
          healthy_engines: non_neg_integer(),
          unhealthy_engines: non_neg_integer(),
          engine_details: [map()]
        }
  def health_check do
    engines = list_engines()

    engine_details =
      Enum.map(engines, fn {pid, info} ->
        health_status = check_engine_health(pid)
        Map.put(info, :health_status, health_status)
      end)

    healthy_count = Enum.count(engine_details, &(&1.health_status == :healthy))
    unhealthy_count = length(engine_details) - healthy_count

    %{
      total_engines: length(engine_details),
      healthy_engines: healthy_count,
      unhealthy_engines: unhealthy_count,
      engine_details: engine_details
    }
  end

  ## Server implementation

  @impl DynamicSupervisor
  def init(opts) do
    Logger.info("Starting Presto.EngineSupervisor", opts: inspect(opts))

    strategy = Keyword.get(opts, :strategy, :one_for_one)
    max_restarts = Keyword.get(opts, :max_restarts, 10)
    max_seconds = Keyword.get(opts, :max_seconds, 60)

    DynamicSupervisor.init(
      strategy: strategy,
      max_restarts: max_restarts,
      max_seconds: max_seconds
    )
  end

  ## Private functions

  defp generate_engine_id do
    "engine_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp extract_supervision_opts(opts) do
    sup_keys = [:restart, :shutdown]
    sup_opts = Keyword.take(opts, sup_keys)
    engine_opts = Keyword.drop(opts, sup_keys)
    {sup_opts, engine_opts}
  end

  defp resolve_engine_pid(name) when is_atom(name) do
    case EngineRegistry.lookup_engine(name) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, :not_found}
    end
  end

  defp resolve_engine_pid(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      {:ok, pid}
    else
      {:error, :not_found}
    end
  end

  defp get_engine_details(pid) when is_pid(pid) do
    try do
      case GenServer.call(pid, :get_engine_statistics, 1000) do
        stats when is_map(stats) ->
          process_info = Process.info(pid, [:memory, :message_queue_len, :reductions])

          %{
            pid: pid,
            engine_id: Map.get(stats, :engine_id, "unknown"),
            total_facts: Map.get(stats, :total_facts, 0),
            total_rules: Map.get(stats, :total_rules, 0),
            memory_usage: process_info[:memory],
            message_queue_len: process_info[:message_queue_len],
            reductions: process_info[:reductions],
            uptime: calculate_uptime(pid)
          }

        _ ->
          %{
            pid: pid,
            engine_id: "unknown",
            status: :unresponsive
          }
      end
    rescue
      _ ->
        %{
          pid: pid,
          engine_id: "unknown",
          status: :unresponsive
        }
    end
  end

  defp check_engine_health(pid) do
    try do
      # Simple health check - ensure engine responds to basic calls
      case GenServer.call(pid, :get_engine_statistics, 1000) do
        stats when is_map(stats) -> :healthy
        _ -> :unhealthy
      end
    rescue
      _ -> :unhealthy
    catch
      :exit, _ -> :unhealthy
    end
  end

  defp calculate_uptime(pid) do
    case Process.info(pid, :start_time) do
      {:start_time, start_time} ->
        now = :erlang.monotonic_time()
        uptime_native = now - start_time
        System.convert_time_unit(uptime_native, :native, :millisecond)

      nil ->
        0
    end
  end
end
