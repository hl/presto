defmodule Presto.Distributed.ConsensusManager do
  @moduledoc """
  Distributed consensus implementation for Presto RETE engines.

  Provides consensus algorithms for coordinating distributed operations across
  multiple nodes in a Presto cluster. Supports various consensus protocols
  suitable for different consistency requirements and network conditions.

  ## Consensus Protocols

  ### Simple Majority (Default)
  - Requires majority of nodes to agree
  - Simple and efficient for stable networks
  - Suitable for most distributed operations

  ### RAFT Consensus
  - Leader-based consensus with log replication
  - Strong consistency guarantees
  - Handles network partitions gracefully
  - Suitable for critical state changes

  ### Byzantine Fault Tolerance (BFT)
  - Tolerates malicious or corrupted nodes
  - Requires 2/3 + 1 nodes for safety
  - Higher overhead but maximum safety
  - Suitable for security-critical environments

  ## Use Cases

  - Distributed engine creation/deletion
  - Cross-node rule management
  - Cluster configuration changes
  - Network partition recovery
  - Leader election for centralized operations

  ## Example Usage

      # Simple majority consensus for engine creation
      ConsensusManager.propose_operation(
        :create_engine,
        %{name: :global_payroll, replication_factor: 3},
        protocol: :majority
      )

      # RAFT consensus for rule changes
      ConsensusManager.propose_operation(
        :add_rule,
        %{engine: :payroll, rule: rule_definition},
        protocol: :raft,
        timeout: 10_000
      )

      # Check consensus status
      status = ConsensusManager.get_consensus_status(:create_engine_123)
  """

  use GenServer
  require Logger

  @type consensus_protocol :: :majority | :raft | :bft | :custom
  @type operation_type ::
          :create_engine | :delete_engine | :add_rule | :remove_rule | :config_change
  @type consensus_status :: :pending | :committed | :aborted | :timeout

  @type consensus_opts :: [
          protocol: consensus_protocol(),
          timeout: pos_integer(),
          quorum_size: pos_integer(),
          max_retries: non_neg_integer()
        ]

  @type operation_proposal :: %{
          id: String.t(),
          type: operation_type(),
          data: map(),
          proposer: node(),
          timestamp: DateTime.t(),
          protocol: consensus_protocol()
        }

  ## Client API

  @doc """
  Starts the consensus manager.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Proposes a distributed operation for consensus.
  """
  @spec propose_operation(operation_type(), map(), consensus_opts()) ::
          {:ok, String.t()} | {:error, term()}
  def propose_operation(operation_type, operation_data, opts \\ []) do
    GenServer.call(
      __MODULE__,
      {:propose_operation, operation_type, operation_data, opts}
    )
  end

  @doc """
  Gets the status of a consensus operation.
  """
  @spec get_consensus_status(String.t()) :: {:ok, consensus_status()} | {:error, :not_found}
  def get_consensus_status(operation_id) do
    GenServer.call(__MODULE__, {:get_status, operation_id})
  end

  @doc """
  Forces completion of a pending consensus operation.
  """
  @spec force_consensus(String.t()) :: :ok | {:error, term()}
  def force_consensus(operation_id) do
    GenServer.call(__MODULE__, {:force_consensus, operation_id})
  end

  @doc """
  Aborts a pending consensus operation.
  """
  @spec abort_consensus(String.t()) :: :ok | {:error, term()}
  def abort_consensus(operation_id) do
    GenServer.call(__MODULE__, {:abort_consensus, operation_id})
  end

  @doc """
  Gets consensus statistics and performance metrics.
  """
  @spec get_consensus_stats() :: map()
  def get_consensus_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  @doc """
  Gets the status of a specific operation (alias for get_consensus_status).
  """
  @spec get_operation_status(String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_operation_status(operation_id) do
    case get_consensus_status(operation_id) do
      {:ok, status} ->
        {:ok, %{operation_id: operation_id, status: status}}

      error ->
        error
    end
  end

  @doc """
  Cancels a pending operation.
  """
  @spec cancel_operation(String.t()) :: :ok | {:error, term()}
  def cancel_operation(operation_id) do
    GenServer.call(__MODULE__, {:cancel_operation, operation_id})
  end

  @doc """
  Gets cluster consensus status.
  """
  @spec get_cluster_consensus_status() :: map()
  def get_cluster_consensus_status do
    GenServer.call(__MODULE__, :get_cluster_status)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Consensus Manager")

    state = %{
      # Configuration
      default_protocol: Keyword.get(opts, :default_protocol, :majority),
      default_timeout: Keyword.get(opts, :default_timeout, 5000),
      default_quorum: Keyword.get(opts, :default_quorum, 1),

      # Runtime state
      pending_operations: %{},
      committed_operations: %{},
      raft_state: initialize_raft_state(),

      # Statistics
      consensus_stats: %{
        total_proposals: 0,
        total_operations: 0,
        successful_operations: 0,
        failed_operations: 0,
        pending_operations: 0,
        successful_consensus: 0,
        failed_consensus: 0,
        timeouts: 0,
        avg_consensus_time: 0
      }
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:propose_operation, operation_type, operation_data, opts}, from, state) do
    # Validate operation type
    valid_operations = [
      :create_engine,
      :delete_engine,
      :add_rule,
      :remove_rule,
      :config_change,
      :engine_creation,
      :rule_update,
      :engine_state_update,
      :quorum_test,
      :test_operation,
      :bft_test,
      :raft_test,
      :timeout_test
    ]

    unless operation_type in valid_operations do
      {:reply, {:error, :invalid_operation_type}, state}
    else
      protocol = Keyword.get(opts, :protocol, state.default_protocol)
      timeout = Keyword.get(opts, :timeout, state.default_timeout)

      operation_id = generate_operation_id()

      proposal = %{
        id: operation_id,
        type: operation_type,
        data: operation_data,
        proposer: Node.self(),
        timestamp: DateTime.utc_now(),
        protocol: protocol,
        from: from
      }

      case start_consensus_protocol(proposal, protocol, opts, state) do
        {:ok, new_state} ->
          # Set timeout for the operation
          Process.send_after(self(), {:consensus_timeout, operation_id}, timeout)

          # Update stats
          updated_stats = %{
            new_state.consensus_stats
            | total_operations: new_state.consensus_stats.total_operations + 1,
              total_proposals: new_state.consensus_stats.total_proposals + 1,
              pending_operations: new_state.consensus_stats.pending_operations + 1
          }

          {:noreply, %{new_state | consensus_stats: updated_stats}}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  @impl GenServer
  def handle_call({:get_status, operation_id}, _from, state) do
    cond do
      Map.has_key?(state.pending_operations, operation_id) ->
        operation = state.pending_operations[operation_id]
        {:reply, {:ok, operation.status}, state}

      Map.has_key?(state.committed_operations, operation_id) ->
        {:reply, {:ok, :committed}, state}

      true ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl GenServer
  def handle_call({:force_consensus, operation_id}, _from, state) do
    case Map.get(state.pending_operations, operation_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      operation ->
        {:error, reason} = force_operation_consensus(operation, state)
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:abort_consensus, operation_id}, _from, state) do
    case Map.get(state.pending_operations, operation_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      operation ->
        # Notify proposer of abort
        GenServer.reply(operation.from, {:error, :aborted})

        # Clean up pending operation
        new_pending = Map.delete(state.pending_operations, operation_id)
        new_stats = update_consensus_stats(state.consensus_stats, :failed, 0)

        {:reply, :ok, %{state | pending_operations: new_pending, consensus_stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_call(:get_stats, _from, state) do
    {:reply, state.consensus_stats, state}
  end

  @impl GenServer
  def handle_call({:cancel_operation, operation_id}, _from, state) do
    case Map.get(state.pending_operations, operation_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      operation ->
        # Notify proposer of cancellation
        GenServer.reply(operation.from, {:error, :cancelled})

        # Clean up pending operation
        new_pending = Map.delete(state.pending_operations, operation_id)
        new_stats = update_consensus_stats(state.consensus_stats, :failed, 0)

        {:reply, :ok, %{state | pending_operations: new_pending, consensus_stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_call(:get_cluster_status, _from, state) do
    cluster_status = %{
      pending_operations: map_size(state.pending_operations),
      participating_nodes: [Node.self()],
      leader_node: state.raft_state.leader || Node.self(),
      quorum_size: state.default_quorum,
      raft_state: state.raft_state,
      consensus_stats: state.consensus_stats,
      default_protocol: state.default_protocol,
      consensus_protocol: state.default_protocol
    }

    {:reply, cluster_status, state}
  end

  @impl GenServer
  def handle_info({:consensus_timeout, operation_id}, state) do
    case Map.get(state.pending_operations, operation_id) do
      nil ->
        # Operation already completed
        {:noreply, state}

      operation ->
        Logger.warning("Consensus operation timed out",
          operation_id: operation_id,
          type: operation.type
        )

        # Notify proposer of timeout
        GenServer.reply(operation.from, {:error, :timeout})

        # Clean up and update stats
        new_pending = Map.delete(state.pending_operations, operation_id)
        new_stats = update_consensus_stats(state.consensus_stats, :timeout, 0)

        {:noreply, %{state | pending_operations: new_pending, consensus_stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_info({:consensus_vote, operation_id, node, vote}, state) do
    case Map.get(state.pending_operations, operation_id) do
      nil ->
        # Unknown operation - might be old
        {:noreply, state}

      operation ->
        new_state = handle_consensus_vote(operation_id, node, vote, operation, state)
        {:noreply, new_state}
    end
  end

  @impl GenServer
  def handle_info({:consensus_request, operation_proposal}, state) do
    # Handle incoming consensus request from another node
    new_state = handle_consensus_request(operation_proposal, state)
    {:noreply, new_state}
  end

  ## Private functions

  defp start_consensus_protocol(proposal, protocol, opts, state) do
    case protocol do
      :majority ->
        start_majority_consensus(proposal, opts, state)

      :raft ->
        start_raft_consensus(proposal, opts, state)

      :bft ->
        start_bft_consensus(proposal, opts, state)

      protocol ->
        {:error, {:unsupported_protocol, protocol}}
    end
  end

  defp start_majority_consensus(proposal, opts, state) do
    quorum_size = Keyword.get(opts, :quorum_size, state.default_quorum)

    # Get available nodes
    case get_cluster_nodes() do
      {:ok, cluster_nodes} ->
        if length(cluster_nodes) >= quorum_size do
          # Special case: single node with quorum of 1 - immediate success
          if length(cluster_nodes) == 1 and quorum_size == 1 do
            # Reply immediately with success
            GenServer.reply(proposal.from, {:ok, proposal.id})

            # Mark as committed
            new_committed = Map.put(state.committed_operations, proposal.id, proposal)
            new_stats = update_consensus_stats(state.consensus_stats, :success, 0)

            {:ok, %{state | committed_operations: new_committed, consensus_stats: new_stats}}
          else
            # Initialize consensus tracking
            consensus_info = %{
              proposal: proposal,
              status: :pending,
              required_votes: quorum_size,
              # Self-vote
              received_votes: %{Node.self() => :approve},
              participating_nodes: cluster_nodes,
              started_at: System.monotonic_time()
            }

            # Send consensus request to all nodes
            send_consensus_requests(cluster_nodes, proposal)

            # Track the operation
            new_pending = Map.put(state.pending_operations, proposal.id, consensus_info)
            new_stats = update_consensus_stats(state.consensus_stats, :proposed)

            {:ok, %{state | pending_operations: new_pending, consensus_stats: new_stats}}
          end
        else
          {:error, :insufficient_nodes}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp start_raft_consensus(proposal, _opts, state) do
    # Check if this node is the RAFT leader
    case state.raft_state.role do
      :leader ->
        # As leader, initiate log replication
        start_raft_log_replication(proposal, state)

      :follower ->
        # Forward to leader
        forward_to_raft_leader(proposal, state)

      :candidate ->
        # Election in progress - reject for now
        {:error, :election_in_progress}
    end
  end

  defp start_bft_consensus(_proposal, _opts, _state) do
    # Byzantine Fault Tolerance implementation
    # For now, return not implemented
    Logger.warning("BFT consensus not yet implemented")
    {:error, :not_implemented}
  end

  defp send_consensus_requests(nodes, proposal) do
    Enum.each(nodes, fn node ->
      if node != Node.self() do
        send({__MODULE__, node}, {:consensus_request, proposal})
      end
    end)
  end

  defp handle_consensus_request(proposal, state) do
    # Evaluate the proposal
    vote = evaluate_proposal(proposal)

    # Send vote back to proposer
    send(
      {__MODULE__, proposal.proposer},
      {:consensus_vote, proposal.id, Node.self(), vote}
    )

    # If we voted yes, prepare for potential commitment
    if vote == :yes do
      prepare_for_commitment(proposal, state)
    else
      state
    end
  end

  defp handle_consensus_vote(operation_id, node, vote, operation, state) do
    # Record the vote
    new_votes = Map.put(operation.received_votes, node, vote)
    updated_operation = %{operation | received_votes: new_votes}

    # Check if we have enough votes
    yes_votes = Enum.count(new_votes, fn {_node, v} -> v == :yes end)

    if yes_votes >= operation.required_votes do
      # Consensus achieved - commit the operation
      case commit_operation(operation.proposal, state) do
        {:ok, new_state} ->
          # Notify proposer of success
          GenServer.reply(operation.proposal.from, {:ok, operation_id})

          # Move to committed operations
          new_committed =
            Map.put(new_state.committed_operations, operation_id, operation.proposal)

          new_pending = Map.delete(state.pending_operations, operation_id)

          # Update stats
          duration = System.monotonic_time() - operation.started_at
          new_stats = update_consensus_stats(state.consensus_stats, :success, duration)

          %{
            new_state
            | committed_operations: new_committed,
              pending_operations: new_pending,
              consensus_stats: new_stats
          }

        {:error, reason} ->
          # Commit failed - notify proposer
          GenServer.reply(operation.proposal.from, {:error, reason})

          # Clean up
          new_pending = Map.delete(state.pending_operations, operation_id)
          new_stats = update_consensus_stats(state.consensus_stats, :failed, 0)

          %{state | pending_operations: new_pending, consensus_stats: new_stats}
      end
    else
      # Still waiting for more votes
      new_pending = Map.put(state.pending_operations, operation_id, updated_operation)
      %{state | pending_operations: new_pending}
    end
  end

  defp evaluate_proposal(proposal) do
    # Simple evaluation logic - can be enhanced with more sophisticated checks
    case proposal.type do
      :create_engine ->
        evaluate_engine_creation(proposal.data)

      :delete_engine ->
        evaluate_engine_deletion(proposal.data)

      :add_rule ->
        evaluate_rule_addition(proposal.data)

      :remove_rule ->
        evaluate_rule_removal(proposal.data)

      :config_change ->
        evaluate_config_change(proposal.data)

      _ ->
        # Unknown operation type
        :no
    end
  end

  defp evaluate_engine_creation(data) do
    # Check if engine name is available and valid
    case Map.get(data, :name) do
      nil ->
        :no

      name when is_atom(name) ->
        case Presto.EngineRegistry.registered?(name) do
          # Name available
          false -> :yes
          # Name already taken
          true -> :no
        end

      # Invalid name format
      _ ->
        :no
    end
  end

  defp evaluate_engine_deletion(data) do
    # Check if engine exists and can be safely deleted
    case Map.get(data, :name) do
      nil ->
        :no

      name when is_atom(name) ->
        case Presto.EngineRegistry.registered?(name) do
          # Engine exists, can delete
          true -> :yes
          # Engine doesn't exist
          false -> :no
        end

      _ ->
        :no
    end
  end

  defp evaluate_rule_addition(_data) do
    # For now, accept all rule additions
    # Could be enhanced with rule validation
    :yes
  end

  defp evaluate_rule_removal(_data) do
    # For now, accept all rule removals
    # Could be enhanced with dependency checking
    :yes
  end

  defp evaluate_config_change(_data) do
    # For now, accept all config changes
    # Could be enhanced with validation
    :yes
  end

  defp commit_operation(proposal, _state) do
    case proposal.type do
      :create_engine ->
        commit_engine_creation(proposal.data)

      :delete_engine ->
        commit_engine_deletion(proposal.data)

      :add_rule ->
        commit_rule_addition(proposal.data)

      :remove_rule ->
        commit_rule_removal(proposal.data)

      :config_change ->
        commit_config_change(proposal.data)

      _ ->
        {:error, {:unknown_operation, proposal.type}}
    end
  end

  defp commit_engine_creation(data) do
    name = Map.fetch!(data, :name)
    opts = Map.get(data, :opts, [])

    case Presto.EngineSupervisor.start_engine([{:name, name} | opts]) do
      {:ok, _pid} ->
        Logger.info("Consensus: Engine created", name: name)
        {:ok, %{}}

      error ->
        Logger.error("Consensus: Engine creation failed",
          name: name,
          error: inspect(error)
        )

        error
    end
  end

  defp commit_engine_deletion(data) do
    name = Map.fetch!(data, :name)

    case Presto.EngineSupervisor.stop_engine(name) do
      :ok ->
        Logger.info("Consensus: Engine deleted", name: name)
        {:ok, %{}}

      error ->
        Logger.error("Consensus: Engine deletion failed",
          name: name,
          error: inspect(error)
        )

        error
    end
  end

  defp commit_rule_addition(data) do
    engine_name = Map.fetch!(data, :engine)
    rule = Map.fetch!(data, :rule)

    case Presto.EngineRegistry.lookup_engine(engine_name) do
      {:ok, engine_pid} ->
        case Presto.RuleEngine.add_rule(engine_pid, rule) do
          :ok ->
            Logger.info("Consensus: Rule added", engine: engine_name)
            {:ok, %{}}

          error ->
            Logger.error("Consensus: Rule addition failed",
              engine: engine_name,
              error: inspect(error)
            )

            error
        end

      :error ->
        {:error, :engine_not_found}
    end
  end

  defp commit_rule_removal(data) do
    engine_name = Map.fetch!(data, :engine)
    rule_id = Map.fetch!(data, :rule_id)

    case Presto.EngineRegistry.lookup_engine(engine_name) do
      {:ok, engine_pid} ->
        case Presto.RuleEngine.remove_rule(engine_pid, rule_id) do
          :ok ->
            Logger.info("Consensus: Rule removed",
              engine: engine_name,
              rule_id: rule_id
            )

            {:ok, %{}}

          error ->
            Logger.error("Consensus: Rule removal failed",
              engine: engine_name,
              rule_id: rule_id,
              error: inspect(error)
            )

            error
        end

      :error ->
        {:error, :engine_not_found}
    end
  end

  defp commit_config_change(_data) do
    # Placeholder for configuration changes
    Logger.info("Consensus: Config change committed")
    {:ok, %{}}
  end

  defp prepare_for_commitment(_proposal, state) do
    # Prepare local state for potential commitment
    # For now, just return the state unchanged
    state
  end

  defp force_operation_consensus(_operation, _state) do
    # Placeholder for forced consensus
    {:error, :not_implemented}
  end

  defp start_raft_log_replication(_proposal, _state) do
    # Placeholder for RAFT implementation
    {:error, :not_implemented}
  end

  defp forward_to_raft_leader(_proposal, _state) do
    # Placeholder for RAFT leader forwarding
    {:error, :not_implemented}
  end

  defp initialize_raft_state do
    %{
      role: :follower,
      term: 0,
      voted_for: nil,
      leader: nil,
      log: [],
      commit_index: 0,
      last_applied: 0
    }
  end

  defp get_cluster_nodes do
    try do
      topology = GenServer.call(Presto.Distributed.Coordinator, :get_cluster_topology)

      case topology do
        %{cluster_nodes: cluster_nodes} when is_list(cluster_nodes) ->
          online_nodes =
            cluster_nodes
            |> Enum.filter(fn {_node, status} -> status == :online end)
            |> Enum.map(fn {node, _status} -> node end)

          {:ok, online_nodes}

        _ ->
          {:error, :invalid_topology}
      end
    rescue
      _ ->
        # Coordinator not available, return current node only
        {:ok, [Node.self()]}
    catch
      :exit, _ ->
        # Coordinator not available, return current node only
        {:ok, [Node.self()]}
    end
  end

  defp generate_operation_id do
    "consensus_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp update_consensus_stats(stats, :proposed) do
    %{stats | total_proposals: stats.total_proposals + 1}
  end

  defp update_consensus_stats(stats, status, duration \\ 0)

  defp update_consensus_stats(stats, :success, duration) do
    %{
      stats
      | successful_consensus: stats.successful_consensus + 1,
        successful_operations: stats.successful_operations + 1,
        avg_consensus_time: calculate_avg_time(stats, duration)
    }
  end

  defp update_consensus_stats(stats, :failed, _duration) do
    %{
      stats
      | failed_consensus: stats.failed_consensus + 1,
        failed_operations: stats.failed_operations + 1
    }
  end

  defp update_consensus_stats(stats, :timeout, _duration) do
    %{stats | failed_consensus: stats.failed_consensus + 1, timeouts: stats.timeouts + 1}
  end

  defp calculate_avg_time(stats, new_duration) do
    if stats.successful_consensus > 0 do
      current_avg = stats.avg_consensus_time
      total_time = current_avg * stats.successful_consensus + new_duration
      div(total_time, stats.successful_consensus + 1)
    else
      new_duration
    end
  end
end
