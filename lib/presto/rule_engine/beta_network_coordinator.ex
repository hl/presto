defmodule Presto.RuleEngine.BetaNetworkCoordinator do
  @moduledoc """
  Beta network lifecycle coordination for the Presto Rule Engine.

  This module handles the coordination between the rule engine and the beta network,
  including:
  - Starting and stopping beta network processes
  - Managing beta network lifecycle
  - Coordinating between alpha network and beta network
  - Beta network state management

  The beta network handles multi-pattern joins and aggregations in the RETE algorithm.
  This coordinator ensures proper lifecycle management and integration with the
  overall rule engine architecture.
  """

  require Logger
  alias Presto.BetaNetwork
  alias Presto.Logger, as: PrestoLogger
  alias Presto.RuleEngine.State

  @doc """
  Starts a beta network for the given state.

  This function creates a new beta network process and updates the state
  with the beta network pid.
  """
  @spec start_beta_network(State.t()) :: {:ok, State.t()} | {:error, term()}
  def start_beta_network(%State{} = state) do
    engine_id = State.get_engine_id(state)
    alpha_memories = State.get_alpha_memories(state)

    PrestoLogger.log_engine_lifecycle(:debug, engine_id, "starting_beta_network", %{
      alpha_memories_table: alpha_memories
    })

    case BetaNetwork.start_link(
           rule_engine: self(),
           alpha_memories_table: alpha_memories
         ) do
      {:ok, beta_network_pid} ->
        updated_state = State.set_beta_network(state, beta_network_pid)

        PrestoLogger.log_engine_lifecycle(:debug, engine_id, "beta_network_started", %{
          pid: beta_network_pid
        })

        {:ok, updated_state}

      {:error, reason} = error ->
        PrestoLogger.log_engine_lifecycle(:error, engine_id, "beta_network_start_failed", %{
          reason: reason
        })

        error
    end
  end

  @doc """
  Stops the beta network for the given state.

  This function gracefully shuts down the beta network process.
  """
  @spec stop_beta_network(State.t()) :: State.t()
  def stop_beta_network(%State{} = state) do
    case State.get_beta_network(state) do
      nil ->
        state

      beta_network_pid ->
        engine_id = State.get_engine_id(state)

        PrestoLogger.log_engine_lifecycle(:debug, engine_id, "stopping_beta_network", %{
          pid: beta_network_pid
        })

        GenServer.stop(beta_network_pid)

        PrestoLogger.log_engine_lifecycle(:debug, engine_id, "beta_network_stopped", %{
          pid: beta_network_pid
        })

        State.set_beta_network(state, nil)
    end
  end

  @doc """
  Restarts the beta network for the given state.

  This function stops the existing beta network and starts a new one.
  Useful for recovering from beta network failures.
  """
  @spec restart_beta_network(State.t()) :: {:ok, State.t()} | {:error, term()}
  def restart_beta_network(%State{} = state) do
    engine_id = State.get_engine_id(state)

    PrestoLogger.log_engine_lifecycle(:info, engine_id, "restarting_beta_network", %{})

    state
    |> stop_beta_network()
    |> start_beta_network()
  end

  @doc """
  Processes alpha network changes through the beta network.

  This function triggers the beta network to process any pending changes
  from the alpha network.
  """
  @spec process_alpha_changes(State.t()) :: :ok | {:error, term()}
  def process_alpha_changes(%State{} = state) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        BetaNetwork.process_alpha_changes(beta_network_pid)
    end
  end

  @doc """
  Creates a beta node in the beta network.

  This function delegates beta node creation to the beta network process.
  """
  @spec create_beta_node(State.t(), tuple()) :: {:ok, String.t()} | {:error, term()}
  def create_beta_node(%State{} = state, node_spec) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        BetaNetwork.create_beta_node(beta_network_pid, node_spec)
    end
  end

  @doc """
  Removes a beta node from the beta network.
  """
  @spec remove_beta_node(State.t(), String.t()) :: :ok | {:error, term()}
  def remove_beta_node(%State{} = state, node_id) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        BetaNetwork.remove_beta_node(beta_network_pid, node_id)
    end
  end

  @doc """
  Creates a snapshot of the beta network state.

  This function creates a snapshot of the current beta network state
  including all beta nodes, tokens, and network structure.
  """
  @spec create_snapshot(State.t()) :: {:ok, map()} | {:error, term()}
  def create_snapshot(%State{} = state) do
    case State.get_beta_network(state) do
      nil ->
        {:ok, %{beta_network: nil, timestamp: DateTime.utc_now()}}

      beta_network_pid ->
        try do
          # Get beta network state
          case BetaNetwork.get_state(beta_network_pid) do
            {:ok, beta_state} ->
              {:ok,
               %{
                 beta_network: beta_state,
                 timestamp: DateTime.utc_now(),
                 engine_id: State.get_engine_id(state)
               }}

            {:error, reason} ->
              {:error, reason}
          end
        rescue
          error ->
            Logger.debug("Beta network snapshot fallback due to: #{inspect(error)}")

            # Fallback to basic snapshot if get_state is not implemented
            {:ok,
             %{
               beta_network: %{basic_snapshot: true, pid: beta_network_pid},
               timestamp: DateTime.utc_now(),
               engine_id: State.get_engine_id(state)
             }}
        end
    end
  end

  @doc """
  Restores the beta network from a snapshot.

  This function restores the beta network state from a previously created snapshot.
  """
  @spec restore_snapshot(State.t(), map()) :: {:ok, State.t()} | {:error, term()}
  def restore_snapshot(%State{} = state, snapshot) do
    try do
      case Map.get(snapshot, :beta_network) do
        nil -> restore_empty_snapshot(state)
        %{basic_snapshot: true} -> restore_basic_snapshot(state)
        beta_state -> restore_full_snapshot(state, beta_state)
      end
    rescue
      error ->
        {:error, {:snapshot_restore_failed, error}}
    end
  end

  defp restore_empty_snapshot(state) do
    # No beta network in snapshot, ensure current beta network is stopped
    {:ok, stop_beta_network(state)}
  end

  defp restore_basic_snapshot(state) do
    # Basic snapshot - just restart beta network
    restart_beta_network(state)
  end

  defp restore_full_snapshot(state, beta_state) do
    # Full beta network state - restart and restore
    case restart_beta_network(state) do
      {:ok, new_state} -> restore_beta_network_state(new_state, beta_state)
      error -> error
    end
  end

  defp restore_beta_network_state(state, beta_state) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        attempt_state_restore(beta_network_pid, beta_state, state)
    end
  end

  defp attempt_state_restore(beta_network_pid, beta_state, state) do
    try do
      case BetaNetwork.restore_state(beta_network_pid, beta_state) do
        :ok -> {:ok, state}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error ->
        Logger.debug("Beta network restore fallback due to: #{inspect(error)}")
        # If restore_state is not implemented, just use the restarted network
        {:ok, state}
    end
  end

  @doc """
  Creates an aggregation node in the beta network.
  """
  @spec create_aggregation_node(State.t(), tuple()) :: {:ok, String.t()} | {:error, term()}
  def create_aggregation_node(%State{} = state, node_spec) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        BetaNetwork.create_aggregation_node(beta_network_pid, node_spec)
    end
  end

  @doc """
  Gets beta memory for a given node ID.
  """
  @spec get_beta_memory(State.t(), String.t()) :: [map()] | {:error, term()}
  def get_beta_memory(%State{} = state, node_id) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        BetaNetwork.get_beta_memory(beta_network_pid, node_id)
    end
  end

  @doc """
  Gets beta node info for a given node ID.
  """
  @spec get_beta_node_info(State.t(), String.t()) :: map() | nil | {:error, term()}
  def get_beta_node_info(%State{} = state, node_id) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        BetaNetwork.get_beta_node_info(beta_network_pid, node_id)
    end
  end

  @doc """
  Checks if the beta network is running and healthy.
  """
  @spec beta_network_healthy?(State.t()) :: boolean()
  def beta_network_healthy?(%State{} = state) do
    case State.get_beta_network(state) do
      nil ->
        false

      beta_network_pid ->
        Process.alive?(beta_network_pid)
    end
  end

  @doc """
  Gets statistics about the beta network.
  """
  @spec get_beta_network_stats(State.t()) :: map() | {:error, term()}
  def get_beta_network_stats(%State{} = state) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        try do
          # Try to get basic process info
          info = Process.info(beta_network_pid)

          %{
            pid: beta_network_pid,
            alive: Process.alive?(beta_network_pid),
            message_queue_len: info[:message_queue_len] || 0,
            memory: info[:memory] || 0
          }
        rescue
          _ ->
            %{
              pid: beta_network_pid,
              alive: false,
              message_queue_len: 0,
              memory: 0
            }
        end
    end
  end

  @doc """
  Monitors the beta network process and handles failures.

  This function sets up monitoring for the beta network and provides
  a callback for handling failures.
  """
  @spec monitor_beta_network(State.t(), function() | nil) :: {:ok, reference()} | {:error, term()}
  def monitor_beta_network(%State{} = state, failure_callback \\ nil) do
    case State.get_beta_network(state) do
      nil ->
        {:error, :beta_network_not_started}

      beta_network_pid ->
        monitor_ref = Process.monitor(beta_network_pid)
        maybe_setup_failure_handler(monitor_ref, beta_network_pid, failure_callback)
        {:ok, monitor_ref}
    end
  end

  defp maybe_setup_failure_handler(_monitor_ref, _beta_network_pid, nil), do: :ok

  defp maybe_setup_failure_handler(monitor_ref, beta_network_pid, failure_callback) do
    spawn_link(fn ->
      receive do
        {:DOWN, ^monitor_ref, :process, ^beta_network_pid, reason} ->
          failure_callback.(reason)
      end
    end)
  end
end
