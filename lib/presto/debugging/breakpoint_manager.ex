defmodule Presto.Debugging.BreakpointManager do
  @moduledoc """
  Breakpoint management system for rule debugging.

  Provides comprehensive breakpoint management including:
  - Conditional breakpoints
  - Rule-specific breakpoints
  - Fact-based breakpoints
  - Breakpoint hit counting
  - Dynamic breakpoint enabling/disabling
  """

  use GenServer
  require Logger

  @type breakpoint :: %{
          id: String.t(),
          type: breakpoint_type(),
          target: String.t() | atom(),
          condition: String.t() | nil,
          enabled: boolean(),
          hit_count: non_neg_integer(),
          max_hits: pos_integer() | :unlimited,
          created_at: DateTime.t(),
          last_hit: DateTime.t() | nil
        }

  @type breakpoint_type :: :rule | :fact | :condition | :action

  @type manager_state :: %{
          breakpoints: %{String.t() => breakpoint()},
          global_enabled: boolean(),
          hit_notifications: [pid()],
          stats: map()
        }

  ## Client API

  @doc """
  Starts the breakpoint manager.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Sets a breakpoint.
  """
  @spec set_breakpoint(breakpoint_type(), String.t() | atom(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def set_breakpoint(type, target, opts \\ []) do
    GenServer.call(__MODULE__, {:set_breakpoint, type, target, opts})
  end

  @doc """
  Removes a breakpoint.
  """
  @spec remove_breakpoint(String.t()) :: :ok | {:error, :not_found}
  def remove_breakpoint(breakpoint_id) do
    GenServer.call(__MODULE__, {:remove_breakpoint, breakpoint_id})
  end

  @doc """
  Enables a breakpoint.
  """
  @spec enable_breakpoint(String.t()) :: :ok | {:error, :not_found}
  def enable_breakpoint(breakpoint_id) do
    GenServer.call(__MODULE__, {:enable_breakpoint, breakpoint_id})
  end

  @doc """
  Disables a breakpoint.
  """
  @spec disable_breakpoint(String.t()) :: :ok | {:error, :not_found}
  def disable_breakpoint(breakpoint_id) do
    GenServer.call(__MODULE__, {:disable_breakpoint, breakpoint_id})
  end

  @doc """
  Lists all breakpoints.
  """
  @spec list_breakpoints() :: [breakpoint()]
  def list_breakpoints do
    GenServer.call(__MODULE__, :list_breakpoints)
  end

  @doc """
  Checks if a breakpoint should trigger.
  """
  @spec check_breakpoint(breakpoint_type(), String.t() | atom(), map()) ::
          {:break, String.t()} | :continue
  def check_breakpoint(type, target, context \\ %{}) do
    GenServer.call(__MODULE__, {:check_breakpoint, type, target, context})
  end

  @doc """
  Clears all breakpoints.
  """
  @spec clear_all_breakpoints() :: :ok
  def clear_all_breakpoints do
    GenServer.call(__MODULE__, :clear_all_breakpoints)
  end

  @doc """
  Enables/disables all breakpoints globally.
  """
  @spec set_global_enabled(boolean()) :: :ok
  def set_global_enabled(enabled) do
    GenServer.call(__MODULE__, {:set_global_enabled, enabled})
  end

  @doc """
  Gets breakpoint statistics.
  """
  @spec get_stats() :: map()
  def get_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  @doc """
  Subscribes to breakpoint hit notifications.
  """
  @spec subscribe_hits(pid()) :: :ok
  def subscribe_hits(subscriber_pid) do
    GenServer.call(__MODULE__, {:subscribe_hits, subscriber_pid})
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    state = %{
      breakpoints: %{},
      global_enabled: Keyword.get(opts, :global_enabled, true),
      hit_notifications: [],
      stats: %{
        total_breakpoints: 0,
        total_hits: 0,
        enabled_breakpoints: 0
      }
    }

    Logger.info("Starting Breakpoint Manager")
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:set_breakpoint, type, target, opts}, _from, state) do
    breakpoint_id = generate_breakpoint_id()

    breakpoint = %{
      id: breakpoint_id,
      type: type,
      target: target,
      condition: Keyword.get(opts, :condition),
      enabled: Keyword.get(opts, :enabled, true),
      hit_count: 0,
      max_hits: Keyword.get(opts, :max_hits, :unlimited),
      created_at: DateTime.utc_now(),
      last_hit: nil
    }

    new_breakpoints = Map.put(state.breakpoints, breakpoint_id, breakpoint)

    new_stats = %{
      state.stats
      | total_breakpoints: state.stats.total_breakpoints + 1,
        enabled_breakpoints:
          state.stats.enabled_breakpoints + if(breakpoint.enabled, do: 1, else: 0)
    }

    Logger.info("Set breakpoint",
      id: breakpoint_id,
      type: type,
      target: target
    )

    {:reply, {:ok, breakpoint_id}, %{state | breakpoints: new_breakpoints, stats: new_stats}}
  end

  @impl GenServer
  def handle_call({:remove_breakpoint, breakpoint_id}, _from, state) do
    case Map.get(state.breakpoints, breakpoint_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      breakpoint ->
        new_breakpoints = Map.delete(state.breakpoints, breakpoint_id)

        new_stats = %{
          state.stats
          | total_breakpoints: state.stats.total_breakpoints - 1,
            enabled_breakpoints:
              state.stats.enabled_breakpoints - if(breakpoint.enabled, do: 1, else: 0)
        }

        Logger.info("Removed breakpoint", id: breakpoint_id)
        {:reply, :ok, %{state | breakpoints: new_breakpoints, stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_call({:enable_breakpoint, breakpoint_id}, _from, state) do
    case Map.get(state.breakpoints, breakpoint_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      breakpoint ->
        if breakpoint.enabled do
          {:reply, :ok, state}
        else
          updated_breakpoint = %{breakpoint | enabled: true}
          new_breakpoints = Map.put(state.breakpoints, breakpoint_id, updated_breakpoint)
          new_stats = %{state.stats | enabled_breakpoints: state.stats.enabled_breakpoints + 1}

          {:reply, :ok, %{state | breakpoints: new_breakpoints, stats: new_stats}}
        end
    end
  end

  @impl GenServer
  def handle_call({:disable_breakpoint, breakpoint_id}, _from, state) do
    case Map.get(state.breakpoints, breakpoint_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      breakpoint ->
        if not breakpoint.enabled do
          {:reply, :ok, state}
        else
          updated_breakpoint = %{breakpoint | enabled: false}
          new_breakpoints = Map.put(state.breakpoints, breakpoint_id, updated_breakpoint)
          new_stats = %{state.stats | enabled_breakpoints: state.stats.enabled_breakpoints - 1}

          {:reply, :ok, %{state | breakpoints: new_breakpoints, stats: new_stats}}
        end
    end
  end

  @impl GenServer
  def handle_call(:list_breakpoints, _from, state) do
    breakpoints = Map.values(state.breakpoints)
    {:reply, breakpoints, state}
  end

  @impl GenServer
  def handle_call({:check_breakpoint, type, target, context}, _from, state) do
    if state.global_enabled do
      case find_matching_breakpoint(type, target, context, state.breakpoints) do
        nil ->
          {:reply, :continue, state}

        {breakpoint_id, breakpoint} ->
          # Update hit count
          updated_breakpoint = %{
            breakpoint
            | hit_count: breakpoint.hit_count + 1,
              last_hit: DateTime.utc_now()
          }

          new_breakpoints = Map.put(state.breakpoints, breakpoint_id, updated_breakpoint)
          new_stats = %{state.stats | total_hits: state.stats.total_hits + 1}

          # Check if we should disable due to max hits
          final_breakpoints =
            if should_disable_breakpoint?(updated_breakpoint) do
              disabled_breakpoint = %{updated_breakpoint | enabled: false}
              Map.put(new_breakpoints, breakpoint_id, disabled_breakpoint)
            else
              new_breakpoints
            end

          # Notify subscribers
          notify_hit_subscribers(
            breakpoint_id,
            updated_breakpoint,
            context,
            state.hit_notifications
          )

          Logger.debug("Breakpoint hit",
            id: breakpoint_id,
            type: type,
            target: target,
            hit_count: updated_breakpoint.hit_count
          )

          {:reply, {:break, breakpoint_id},
           %{state | breakpoints: final_breakpoints, stats: new_stats}}
      end
    else
      {:reply, :continue, state}
    end
  end

  @impl GenServer
  def handle_call(:clear_all_breakpoints, _from, state) do
    new_stats = %{
      state.stats
      | total_breakpoints: 0,
        enabled_breakpoints: 0
    }

    Logger.info("Cleared all breakpoints")
    {:reply, :ok, %{state | breakpoints: %{}, stats: new_stats}}
  end

  @impl GenServer
  def handle_call({:set_global_enabled, enabled}, _from, state) do
    Logger.info("Set global breakpoints enabled", enabled: enabled)
    {:reply, :ok, %{state | global_enabled: enabled}}
  end

  @impl GenServer
  def handle_call(:get_stats, _from, state) do
    enhanced_stats =
      Map.merge(state.stats, %{
        global_enabled: state.global_enabled,
        active_breakpoints: count_active_breakpoints(state.breakpoints)
      })

    {:reply, enhanced_stats, state}
  end

  @impl GenServer
  def handle_call({:subscribe_hits, subscriber_pid}, _from, state) do
    if subscriber_pid in state.hit_notifications do
      {:reply, :ok, state}
    else
      Process.monitor(subscriber_pid)
      new_notifications = [subscriber_pid | state.hit_notifications]
      {:reply, :ok, %{state | hit_notifications: new_notifications}}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Remove dead subscriber
    new_notifications = List.delete(state.hit_notifications, pid)
    {:noreply, %{state | hit_notifications: new_notifications}}
  end

  ## Private functions

  defp find_matching_breakpoint(type, target, context, breakpoints) do
    Enum.find_value(breakpoints, fn {id, breakpoint} ->
      if breakpoint_matches?(breakpoint, type, target, context) do
        {id, breakpoint}
      end
    end)
  end

  defp breakpoint_matches?(breakpoint, type, target, context) do
    breakpoint.enabled and
      breakpoint.type == type and
      target_matches?(breakpoint.target, target) and
      condition_matches?(breakpoint.condition, context)
  end

  defp target_matches?(breakpoint_target, actual_target) do
    case {breakpoint_target, actual_target} do
      {same, same} ->
        true

      {pattern, target} when is_binary(pattern) and is_atom(target) ->
        String.contains?(Atom.to_string(target), pattern)

      {pattern, target} when is_binary(pattern) and is_binary(target) ->
        String.contains?(target, pattern)

      _ ->
        false
    end
  end

  defp condition_matches?(nil, _context), do: true

  defp condition_matches?(condition, context) when is_binary(condition) do
    # Simple condition evaluation - in production this would use a proper expression evaluator
    try do
      # For now, just check if any context values match the condition string
      context
      |> Map.values()
      |> Enum.any?(fn value -> String.contains?(inspect(value), condition) end)
    rescue
      _ -> true
    end
  end

  defp should_disable_breakpoint?(breakpoint) do
    case breakpoint.max_hits do
      :unlimited -> false
      max_hits -> breakpoint.hit_count >= max_hits
    end
  end

  defp notify_hit_subscribers(breakpoint_id, breakpoint, context, subscribers) do
    hit_notification = %{
      breakpoint_id: breakpoint_id,
      breakpoint: breakpoint,
      context: context,
      timestamp: DateTime.utc_now()
    }

    Enum.each(subscribers, fn subscriber ->
      send(subscriber, {:breakpoint_hit, hit_notification})
    end)
  end

  defp count_active_breakpoints(breakpoints) do
    breakpoints
    |> Map.values()
    |> Enum.count(& &1.enabled)
  end

  defp generate_breakpoint_id do
    "bp_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end
end
