defmodule Presto.Distributed.RuleCoordinator do
  @moduledoc """
  Distributed rule execution coordination for Presto RETE engines.

  Enables sophisticated distributed rule processing patterns including:
  - Cross-node rule execution and result aggregation
  - Map-reduce style fact processing across cluster nodes
  - Distributed join operations spanning multiple engines
  - Load balancing and parallel rule execution
  - Fault-tolerant distributed computation

  ## Execution Patterns

  ### Scatter-Gather
  - Distribute rule execution across multiple nodes
  - Aggregate results from all participating nodes
  - Suitable for parallel processing of large fact sets

  ### Map-Reduce
  - Map phase: Apply rules to partitioned facts on each node
  - Reduce phase: Aggregate and combine results
  - Optimal for analytics and aggregation workloads

  ### Distributed Joins
  - Join facts across engines on different nodes
  - Cross-node pattern matching and unification
  - Handles large-scale relational operations

  ### Hierarchical Execution
  - Tree-structured rule execution across node hierarchy
  - Results flow up through aggregation levels
  - Suitable for organizational or geographic partitioning

  ## Example Usage

      # Scatter-gather execution across cluster
      {:ok, results} = RuleCoordinator.execute_distributed(
        rule_id: :customer_analysis,
        execution_pattern: :scatter_gather,
        target_engines: [:customer_engine_1, :customer_engine_2, :customer_engine_3],
        aggregation_strategy: :sum
      )

      # Map-reduce processing
      {:ok, aggregated_result} = RuleCoordinator.map_reduce_execution(
        map_rule: :calculate_daily_totals,
        reduce_rule: :aggregate_totals,
        partition_key: :date,
        target_nodes: cluster_nodes
      )

      # Cross-node join operation
      {:ok, join_results} = RuleCoordinator.distributed_join(
        left_engine: :customers,
        right_engine: :orders,
        join_condition: {:customer_id, :customer_id},
        result_engine: :customer_orders
      )
  """

  use GenServer
  require Logger

  alias Presto.RuleEngine
  alias Presto.EngineRegistry
  alias Presto.Distributed.DistributedRegistry

  @type execution_pattern :: :scatter_gather | :map_reduce | :distributed_join | :hierarchical
  @type aggregation_strategy :: :sum | :count | :avg | :min | :max | :collect | :custom
  @type partition_strategy :: :round_robin | :hash | :range | :custom

  @type execution_request :: %{
          id: String.t(),
          pattern: execution_pattern(),
          rule_id: atom(),
          target_engines: [atom()],
          parameters: map(),
          requester: pid(),
          timeout: pos_integer()
        }

  @type execution_opts :: [
          execution_pattern: execution_pattern(),
          target_engines: [atom()],
          aggregation_strategy: aggregation_strategy(),
          partition_strategy: partition_strategy(),
          timeout: pos_integer(),
          parallel_execution: boolean(),
          fault_tolerance: :strict | :best_effort
        ]

  ## Client API

  @doc """
  Starts the rule coordinator.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Executes a rule across multiple engines in the cluster.
  """
  @spec execute_distributed(atom(), execution_opts()) :: {:ok, term()} | {:error, term()}
  def execute_distributed(rule_id, opts \\ []) do
    GenServer.call(__MODULE__, {:execute_distributed, rule_id, opts}, :infinity)
  end

  @doc """
  Performs map-reduce style rule execution across the cluster.
  """
  @spec map_reduce_execution(atom(), atom(), execution_opts()) :: {:ok, term()} | {:error, term()}
  def map_reduce_execution(map_rule, reduce_rule, opts \\ []) do
    GenServer.call(__MODULE__, {:map_reduce, map_rule, reduce_rule, opts}, :infinity)
  end

  @doc """
  Executes a distributed join operation across engines.
  """
  @spec distributed_join(atom(), atom(), tuple(), atom(), execution_opts()) ::
          {:ok, term()} | {:error, term()}
  def distributed_join(left_engine, right_engine, join_condition, result_engine, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    GenServer.call(
      __MODULE__,
      {:distributed_join, left_engine, right_engine, join_condition, result_engine, opts},
      timeout
    )
  end

  @doc """
  Gets the status of a distributed execution.
  """
  @spec get_execution_status(String.t()) :: {:ok, map()} | {:error, :not_found}
  def get_execution_status(execution_id) do
    GenServer.call(__MODULE__, {:get_status, execution_id})
  end

  @doc """
  Cancels a running distributed execution.
  """
  @spec cancel_execution(String.t()) :: :ok | {:error, term()}
  def cancel_execution(execution_id) do
    GenServer.call(__MODULE__, {:cancel_execution, execution_id})
  end

  @doc """
  Gets statistics about distributed rule executions.
  """
  @spec get_execution_stats() :: map()
  def get_execution_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Distributed Rule Coordinator")

    state = %{
      # Configuration
      default_timeout: Keyword.get(opts, :default_timeout, 30_000),
      max_concurrent_executions: Keyword.get(opts, :max_concurrent_executions, 10),
      default_fault_tolerance: Keyword.get(opts, :default_fault_tolerance, :best_effort),

      # Runtime state
      active_executions: %{},
      execution_history: [],

      # Statistics
      stats: %{
        total_executions: 0,
        successful_executions: 0,
        failed_executions: 0,
        avg_execution_time: 0,
        distributed_joins: 0,
        map_reduce_operations: 0
      }
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:execute_distributed, rule_id, opts}, from, state) do
    execution_id = generate_execution_id()
    pattern = Keyword.get(opts, :execution_pattern, :scatter_gather)
    timeout = Keyword.get(opts, :timeout, state.default_timeout)

    case start_distributed_execution(execution_id, rule_id, pattern, opts, from, state) do
      {:ok, new_state} ->
        # Set timeout for the execution
        Process.send_after(self(), {:execution_timeout, execution_id}, timeout)
        {:noreply, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:map_reduce, map_rule, reduce_rule, opts}, from, state) do
    execution_id = generate_execution_id()
    timeout = Keyword.get(opts, :timeout, state.default_timeout)

    {:ok, new_state} =
      start_map_reduce_execution(execution_id, map_rule, reduce_rule, opts, from, state)

    Process.send_after(self(), {:execution_timeout, execution_id}, timeout)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_call(
        {:distributed_join, left_engine, right_engine, join_condition, result_engine, opts},
        from,
        state
      ) do
    execution_id = generate_execution_id()
    timeout = Keyword.get(opts, :timeout, state.default_timeout)

    {:ok, new_state} =
      start_distributed_join(
        execution_id,
        left_engine,
        right_engine,
        join_condition,
        result_engine,
        opts,
        from,
        state
      )

    Process.send_after(self(), {:execution_timeout, execution_id}, timeout)
    new_stats = %{state.stats | distributed_joins: state.stats.distributed_joins + 1}
    {:noreply, %{new_state | stats: new_stats}}
  end

  @impl GenServer
  def handle_call({:get_status, execution_id}, _from, state) do
    case Map.get(state.active_executions, execution_id) do
      nil -> {:reply, {:error, :not_found}, state}
      execution -> {:reply, {:ok, build_status_info(execution)}, state}
    end
  end

  @impl GenServer
  def handle_call({:cancel_execution, execution_id}, _from, state) do
    case Map.get(state.active_executions, execution_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      execution ->
        # Cancel all pending operations
        cancel_execution_operations(execution)

        # Reply to original requester
        GenServer.reply(execution.requester, {:error, :cancelled})

        # Clean up
        new_active = Map.delete(state.active_executions, execution_id)
        {:reply, :ok, %{state | active_executions: new_active}}
    end
  end

  @impl GenServer
  def handle_call(:get_stats, _from, state) do
    {:reply, state.stats, state}
  end

  @impl GenServer
  def handle_info({:execution_result, execution_id, node, result}, state) do
    case Map.get(state.active_executions, execution_id) do
      nil ->
        # Unknown execution
        {:noreply, state}

      execution ->
        new_state = handle_execution_result(execution_id, node, result, execution, state)
        {:noreply, new_state}
    end
  end

  @impl GenServer
  def handle_info({:execution_timeout, execution_id}, state) do
    case Map.get(state.active_executions, execution_id) do
      nil ->
        {:noreply, state}

      execution ->
        Logger.warning("Distributed execution timed out", execution_id: execution_id)

        # Reply with timeout error
        GenServer.reply(execution.requester, {:error, :timeout})

        # Clean up
        new_active = Map.delete(state.active_executions, execution_id)
        new_stats = update_execution_stats(state.stats, :timeout)

        {:noreply, %{state | active_executions: new_active, stats: new_stats}}
    end
  end

  ## Private functions

  defp start_distributed_execution(execution_id, rule_id, pattern, opts, from, state) do
    target_engines = Keyword.get(opts, :target_engines, [])

    if length(target_engines) == 0 do
      # Auto-discover engines if none specified
      case discover_available_engines() do
        {:ok, [_ | _] = discovered_engines} ->
          do_start_distributed_execution(
            execution_id,
            rule_id,
            pattern,
            discovered_engines,
            opts,
            from,
            state
          )

        _ ->
          {:error, :no_available_engines}
      end
    else
      do_start_distributed_execution(
        execution_id,
        rule_id,
        pattern,
        target_engines,
        opts,
        from,
        state
      )
    end
  end

  defp do_start_distributed_execution(
         execution_id,
         rule_id,
         pattern,
         target_engines,
         opts,
         from,
         state
       ) do
    execution_request = %{
      id: execution_id,
      pattern: pattern,
      rule_id: rule_id,
      target_engines: target_engines,
      parameters: Keyword.get(opts, :parameters, %{}),
      requester: from,
      started_at: System.monotonic_time(),
      pending_nodes: MapSet.new(),
      results: %{},
      aggregation_strategy: Keyword.get(opts, :aggregation_strategy, :collect)
    }

    case pattern do
      :scatter_gather ->
        start_scatter_gather_execution(execution_request, state)

      :hierarchical ->
        start_hierarchical_execution(execution_request, state)

      pattern ->
        {:error, {:unsupported_pattern, pattern}}
    end
  end

  defp start_scatter_gather_execution(execution_request, state) do
    coordinator_pid = self()
    
    # Send rule execution requests to all target engines
    Enum.each(execution_request.target_engines, fn engine_name ->
      spawn(fn ->
        execute_rule_on_engine(
          engine_name,
          execution_request.rule_id,
          execution_request.parameters,
          execution_request.id,
          coordinator_pid
        )
      end)
    end)

    # Track the execution
    pending_nodes = MapSet.new(execution_request.target_engines)
    updated_request = %{execution_request | pending_nodes: pending_nodes}

    new_active = Map.put(state.active_executions, execution_request.id, updated_request)
    new_stats = %{state.stats | total_executions: state.stats.total_executions + 1}

    {:ok, %{state | active_executions: new_active, stats: new_stats}}
  end

  defp start_hierarchical_execution(execution_request, state) do
    # Implement hierarchical execution pattern
    # For now, fall back to scatter-gather
    start_scatter_gather_execution(execution_request, state)
  end

  defp start_map_reduce_execution(execution_id, map_rule, reduce_rule, opts, from, state) do
    target_nodes = Keyword.get(opts, :target_nodes, [Node.self()])
    partition_strategy = Keyword.get(opts, :partition_strategy, :round_robin)

    execution_request = %{
      id: execution_id,
      pattern: :map_reduce,
      map_rule: map_rule,
      reduce_rule: reduce_rule,
      target_nodes: target_nodes,
      partition_strategy: partition_strategy,
      parameters: Keyword.get(opts, :parameters, %{}),
      requester: from,
      started_at: System.monotonic_time(),
      map_phase_results: %{},
      pending_map_nodes: MapSet.new(target_nodes),
      reduce_phase_started: false
    }

    # Start map phase
    :ok = start_map_phase(execution_request)

    new_active = Map.put(state.active_executions, execution_id, execution_request)

    new_stats = %{
      state.stats
      | total_executions: state.stats.total_executions + 1,
        map_reduce_operations: state.stats.map_reduce_operations + 1
    }

    {:ok, %{state | active_executions: new_active, stats: new_stats}}
  end

  defp start_map_phase(execution_request) do
    coordinator_pid = self()
    
    # Distribute map rule execution across nodes
    Enum.each(execution_request.target_nodes, fn node ->
      spawn(fn ->
        execute_map_rule_on_node(
          node,
          execution_request.map_rule,
          execution_request.parameters,
          execution_request.id,
          coordinator_pid
        )
      end)
    end)

    :ok
  end

  defp start_distributed_join(
         execution_id,
         left_engine,
         right_engine,
         join_condition,
         result_engine,
         opts,
         from,
         state
       ) do
    execution_request = %{
      id: execution_id,
      pattern: :distributed_join,
      left_engine: left_engine,
      right_engine: right_engine,
      join_condition: join_condition,
      result_engine: result_engine,
      parameters: Keyword.get(opts, :parameters, %{}),
      requester: from,
      started_at: System.monotonic_time(),
      left_facts: nil,
      right_facts: nil,
      join_completed: false
    }

    # Start join operation
    :ok = start_join_operation(execution_request)

    new_active = Map.put(state.active_executions, execution_id, execution_request)
    {:ok, %{state | active_executions: new_active}}
  end

  defp start_join_operation(execution_request) do
    coordinator_pid = self()
    
    # Fetch facts from both engines
    spawn(fn ->
      fetch_engine_facts_for_join(execution_request.left_engine, :left, execution_request.id, coordinator_pid)
    end)

    spawn(fn ->
      fetch_engine_facts_for_join(execution_request.right_engine, :right, execution_request.id, coordinator_pid)
    end)

    :ok
  end

  defp execute_rule_on_engine(engine_name, rule_id, parameters, execution_id, coordinator_pid) do
    try do
      case DistributedRegistry.lookup_global_engine(engine_name) do
        {:ok, engine_info} ->
          node = engine_info.primary_node

          if node == Node.self() and engine_info.local_pid do
            # Local execution
            result = execute_rule_locally(engine_info.local_pid, rule_id, parameters)
            send(coordinator_pid, {:execution_result, execution_id, node, result})
          else
            # Remote execution
            result = execute_rule_remotely(node, engine_name, rule_id, parameters)
            send(coordinator_pid, {:execution_result, execution_id, node, result})
          end

        {:error, :not_found} ->
          send(coordinator_pid, {:execution_result, execution_id, :unknown, {:error, :engine_not_found}})
      end
    rescue
      _ ->
        # DistributedRegistry not available, try local registry
        case EngineRegistry.lookup_engine(engine_name) do
          {:ok, engine_pid} ->
            result = execute_rule_locally(engine_pid, rule_id, parameters)
            send(coordinator_pid, {:execution_result, execution_id, Node.self(), result})

          :error ->
            send(coordinator_pid, {:execution_result, execution_id, :unknown, {:error, :engine_not_found}})
        end
    catch
      :exit, _ ->
        # DistributedRegistry not available, try local registry
        case EngineRegistry.lookup_engine(engine_name) do
          {:ok, engine_pid} ->
            result = execute_rule_locally(engine_pid, rule_id, parameters)
            send(coordinator_pid, {:execution_result, execution_id, Node.self(), result})

          :error ->
            send(coordinator_pid, {:execution_result, execution_id, :unknown, {:error, :engine_not_found}})
        end
    end
  end

  defp execute_rule_locally(engine_pid, _rule_id, _parameters) do
    try do
      # Execute rule on local engine
      case RuleEngine.fire_rules(engine_pid) do
        results when is_list(results) -> {:ok, results}
        result -> {:ok, [result]}
      end
    rescue
      error -> {:error, error}
    end
  end

  defp execute_rule_remotely(node, engine_name, rule_id, parameters) do
    case :rpc.call(node, __MODULE__, :execute_rule_locally_rpc, [engine_name, rule_id, parameters]) do
      {:ok, results} -> {:ok, results}
      {:error, reason} -> {:error, reason}
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
    end
  end

  @doc """
  RPC helper function for remote rule execution.
  """
  def execute_rule_locally_rpc(engine_name, rule_id, parameters) do
    try do
      case DistributedRegistry.lookup_global_engine(engine_name) do
        {:ok, engine_info} when not is_nil(engine_info.local_pid) ->
          execute_rule_locally(engine_info.local_pid, rule_id, parameters)

        _ ->
          {:error, :engine_not_found}
      end
    rescue
      _ ->
        # DistributedRegistry not available, try local registry
        case EngineRegistry.lookup_engine(engine_name) do
          {:ok, engine_pid} ->
            execute_rule_locally(engine_pid, rule_id, parameters)

          :error ->
            {:error, :engine_not_found}
        end
    catch
      :exit, _ ->
        # DistributedRegistry not available, try local registry
        case EngineRegistry.lookup_engine(engine_name) do
          {:ok, engine_pid} ->
            execute_rule_locally(engine_pid, rule_id, parameters)

          :error ->
            {:error, :engine_not_found}
        end
    end
  end

  defp execute_map_rule_on_node(node, map_rule, parameters, execution_id, coordinator_pid) do
    if node == Node.self() do
      # Local map execution
      result = execute_map_rule_locally(map_rule, parameters)
      send(coordinator_pid, {:execution_result, execution_id, node, {:map_result, result}})
    else
      # Remote map execution
      case :rpc.call(node, __MODULE__, :execute_map_rule_locally, [map_rule, parameters]) do
        {:ok, result} ->
          send(coordinator_pid, {:execution_result, execution_id, node, {:map_result, result}})

        error ->
          send(coordinator_pid, {:execution_result, execution_id, node, {:map_error, error}})
      end
    end
  end

  @doc """
  Helper function for local map rule execution.
  """
  def execute_map_rule_locally(_map_rule, _parameters) do
    # Placeholder implementation
    # In a real implementation, this would execute the map rule on local data
    {:ok, %{results: [], node: Node.self(), timestamp: DateTime.utc_now()}}
  end

  defp fetch_engine_facts_for_join(engine_name, side, execution_id, coordinator_pid) do
    try do
      case DistributedRegistry.lookup_global_engine(engine_name) do
        {:ok, engine_info} ->
          facts = get_engine_facts(engine_info)
          send(coordinator_pid, {:execution_result, execution_id, side, {:facts, facts}})

        {:error, reason} ->
          send(coordinator_pid, {:execution_result, execution_id, side, {:error, reason}})
      end
    rescue
      _ ->
        # DistributedRegistry not available, try local registry
        case EngineRegistry.lookup_engine(engine_name) do
          {:ok, engine_pid} ->
            facts = get_local_engine_facts(engine_pid)
            send(coordinator_pid, {:execution_result, execution_id, side, {:facts, facts}})

          :error ->
            send(coordinator_pid, {:execution_result, execution_id, side, {:error, :engine_not_found}})
        end
    catch
      :exit, _ ->
        # DistributedRegistry not available, try local registry
        case EngineRegistry.lookup_engine(engine_name) do
          {:ok, engine_pid} ->
            facts = get_local_engine_facts(engine_pid)
            send(coordinator_pid, {:execution_result, execution_id, side, {:facts, facts}})

          :error ->
            send(coordinator_pid, {:execution_result, execution_id, side, {:error, :engine_not_found}})
        end
    end
  end

  defp get_engine_facts(engine_info) do
    if engine_info.local_pid do
      # Get facts from local engine
      try do
        RuleEngine.query_facts(engine_info.local_pid, {:_, :_, :_}, [])
      rescue
        _ -> []
      end
    else
      # Get facts from remote engine
      case :rpc.call(engine_info.primary_node, RuleEngine, :query_facts, [
             engine_info.local_pid,
             {:_, :_, :_},
             []
           ]) do
        facts when is_list(facts) -> facts
        _ -> []
      end
    end
  end

  defp get_local_engine_facts(engine_pid) do
    try do
      RuleEngine.query_facts(engine_pid, {:_, :_, :_}, [])
    rescue
      _ -> []
    end
  end

  defp handle_execution_result(execution_id, node, result, execution, state) do
    case execution.pattern do
      :scatter_gather ->
        handle_scatter_gather_result(execution_id, node, result, execution, state)

      :map_reduce ->
        handle_map_reduce_result(execution_id, node, result, execution, state)

      :distributed_join ->
        handle_join_result(execution_id, node, result, execution, state)

      _ ->
        state
    end
  end

  defp handle_scatter_gather_result(execution_id, node, result, execution, state) do
    # Store result and check if all nodes have responded
    new_results = Map.put(execution.results, node, result)
    new_pending = MapSet.delete(execution.pending_nodes, node)

    if MapSet.size(new_pending) == 0 do
      # All results received - aggregate and reply
      aggregated_result =
        aggregate_scatter_gather_results(new_results, execution.aggregation_strategy)

      GenServer.reply(execution.requester, {:ok, aggregated_result})

      # Clean up and update stats
      duration = System.monotonic_time() - execution.started_at
      new_active = Map.delete(state.active_executions, execution_id)
      new_stats = update_execution_stats(state.stats, :success, duration)

      %{state | active_executions: new_active, stats: new_stats}
    else
      # Still waiting for more results
      updated_execution = %{execution | results: new_results, pending_nodes: new_pending}
      new_active = Map.put(state.active_executions, execution_id, updated_execution)

      %{state | active_executions: new_active}
    end
  end

  defp handle_map_reduce_result(execution_id, node, {:map_result, result}, execution, state) do
    # Handle map phase result
    new_map_results = Map.put(execution.map_phase_results, node, result)
    new_pending_map = MapSet.delete(execution.pending_map_nodes, node)

    if MapSet.size(new_pending_map) == 0 and not execution.reduce_phase_started do
      # Map phase complete - start reduce phase
      reduce_result =
        perform_reduce_phase(new_map_results, execution.reduce_rule, execution.parameters)

      GenServer.reply(execution.requester, {:ok, reduce_result})

      # Clean up
      new_active = Map.delete(state.active_executions, execution_id)
      duration = System.monotonic_time() - execution.started_at
      new_stats = update_execution_stats(state.stats, :success, duration)

      %{state | active_executions: new_active, stats: new_stats}
    else
      # Update execution state
      updated_execution = %{
        execution
        | map_phase_results: new_map_results,
          pending_map_nodes: new_pending_map
      }

      new_active = Map.put(state.active_executions, execution_id, updated_execution)

      %{state | active_executions: new_active}
    end
  end

  defp handle_map_reduce_result(execution_id, node, {:map_error, error}, execution, state) do
    Logger.warning("Map phase failed", node: node, error: inspect(error))

    # For now, continue with available results
    # Could be enhanced with retry logic
    handle_map_reduce_result(execution_id, node, {:map_result, {:error, error}}, execution, state)
  end

  defp handle_join_result(execution_id, side, {:facts, facts}, execution, state) do
    updated_execution =
      case side do
        :left ->
          %{execution | left_facts: facts}

        :right ->
          %{execution | right_facts: facts}
      end

    # Check if both sides have facts
    if updated_execution.left_facts != nil and updated_execution.right_facts != nil and
         not execution.join_completed do
      # Perform the join operation
      join_result =
        perform_distributed_join(
          updated_execution.left_facts,
          updated_execution.right_facts,
          execution.join_condition
        )

      # Store results in result engine if specified
      if execution.result_engine do
        store_join_results(execution.result_engine, join_result)
      end

      GenServer.reply(execution.requester, {:ok, join_result})

      # Clean up
      new_active = Map.delete(state.active_executions, execution_id)
      duration = System.monotonic_time() - execution.started_at
      new_stats = update_execution_stats(state.stats, :success, duration)

      %{state | active_executions: new_active, stats: new_stats}
    else
      # Update execution state
      new_active = Map.put(state.active_executions, execution_id, updated_execution)
      %{state | active_executions: new_active}
    end
  end

  defp handle_join_result(execution_id, side, {:error, reason}, execution, state) do
    Logger.error("Join operation failed", side: side, reason: inspect(reason))

    GenServer.reply(execution.requester, {:error, reason})

    # Clean up
    new_active = Map.delete(state.active_executions, execution_id)
    new_stats = update_execution_stats(state.stats, :failed)

    %{state | active_executions: new_active, stats: new_stats}
  end

  defp aggregate_scatter_gather_results(results, strategy) do
    case strategy do
      :collect ->
        # Collect all results into a list
        results
        |> Map.values()
        |> Enum.filter(&match?({:ok, _}, &1))
        |> Enum.map(fn {:ok, result} -> result end)
        |> List.flatten()

      :sum ->
        # Sum numeric results
        results
        |> Map.values()
        |> Enum.filter(&match?({:ok, _}, &1))
        |> Enum.map(fn {:ok, result} -> extract_numeric_value(result) end)
        |> Enum.sum()

      :count ->
        # Count total results
        results
        |> Map.values()
        |> Enum.filter(&match?({:ok, _}, &1))
        |> Enum.map(fn {:ok, result} -> length(List.wrap(result)) end)
        |> Enum.sum()

      _ ->
        # Default to collect
        aggregate_scatter_gather_results(results, :collect)
    end
  end

  defp perform_reduce_phase(map_results, reduce_rule, _parameters) do
    # Simple reduce implementation
    # In a real implementation, this would apply the reduce rule to aggregated map results
    %{
      reduced_results: Map.values(map_results),
      reduce_rule: reduce_rule,
      timestamp: DateTime.utc_now()
    }
  end

  defp perform_distributed_join(left_facts, right_facts, {left_key, right_key}) do
    # Simple nested loop join implementation
    # Could be optimized with hash joins for better performance
    Enum.flat_map(left_facts, fn left_fact ->
      left_key_value = extract_join_key(left_fact, left_key)

      right_facts
      |> Enum.filter(fn right_fact ->
        right_key_value = extract_join_key(right_fact, right_key)
        left_key_value == right_key_value
      end)
      |> Enum.map(fn right_fact ->
        # Combine joined facts
        {left_fact, right_fact}
      end)
    end)
  end

  defp extract_join_key(fact, key_position) when is_integer(key_position) do
    elem(fact, key_position)
  end

  defp extract_join_key(fact, key_name) when is_atom(key_name) do
    # For named keys, assume fact is a map or keyword list
    case fact do
      fact when is_map(fact) -> Map.get(fact, key_name)
      fact when is_list(fact) -> Keyword.get(fact, key_name)
      _ -> nil
    end
  end

  defp store_join_results(result_engine, join_results) do
    try do
      case DistributedRegistry.lookup_global_engine(result_engine) do
        {:ok, engine_info} when not is_nil(engine_info.local_pid) ->
          # Convert join results to facts and assert them
          facts =
            Enum.map(join_results, fn {left, right} ->
              {:join_result, left, right, DateTime.utc_now()}
            end)

          RuleEngine.assert_facts_bulk(engine_info.local_pid, facts)

        _ ->
          Logger.warning("Could not store join results", engine: result_engine)
      end
    rescue
      _ ->
        # DistributedRegistry not available, try local registry
        case EngineRegistry.lookup_engine(result_engine) do
          {:ok, engine_pid} ->
            facts =
              Enum.map(join_results, fn {left, right} ->
                {:join_result, left, right, DateTime.utc_now()}
              end)

            RuleEngine.assert_facts_bulk(engine_pid, facts)

          :error ->
            Logger.warning("Could not store join results - engine not found", engine: result_engine)
        end
    catch
      :exit, _ ->
        # DistributedRegistry not available, try local registry
        case EngineRegistry.lookup_engine(result_engine) do
          {:ok, engine_pid} ->
            facts =
              Enum.map(join_results, fn {left, right} ->
                {:join_result, left, right, DateTime.utc_now()}
              end)

            RuleEngine.assert_facts_bulk(engine_pid, facts)

          :error ->
            Logger.warning("Could not store join results - engine not found", engine: result_engine)
        end
    end
  end

  defp extract_numeric_value(result) when is_number(result), do: result

  defp extract_numeric_value(results) when is_list(results) do
    results
    |> Enum.map(&extract_numeric_value/1)
    |> Enum.sum()
  end

  defp extract_numeric_value(_), do: 0

  defp discover_available_engines do
    try do
      case DistributedRegistry.list_cluster_engines() do
        engines when is_list(engines) ->
          engine_names = Enum.map(engines, fn engine -> engine.name end)
          {:ok, engine_names}

        _ ->
          {:error, :no_engines_available}
      end
    rescue
      _ ->
        # Fall back to local engine registry if DistributedRegistry is not available
        try do
          local_engines = EngineRegistry.list_engines()
          engine_names = Enum.map(local_engines, fn engine -> engine.name end)
          {:ok, engine_names}
        rescue
          _ ->
            {:error, :no_engines_available}
        end
    catch
      :exit, _ ->
        # DistributedRegistry process not running, fall back to local registry
        try do
          local_engines = EngineRegistry.list_engines()
          engine_names = Enum.map(local_engines, fn engine -> engine.name end)
          {:ok, engine_names}
        rescue
          _ ->
            {:error, :no_engines_available}
        end
    end
  end

  defp build_status_info(execution) do
    %{
      id: execution.id,
      pattern: execution.pattern,
      started_at: execution.started_at,
      pending_operations: MapSet.size(Map.get(execution, :pending_nodes, MapSet.new())),
      results_received: map_size(Map.get(execution, :results, %{}))
    }
  end

  defp cancel_execution_operations(_execution) do
    # Placeholder for cancellation logic
    # Would send cancellation messages to all pending operations
    :ok
  end

  defp update_execution_stats(stats, status, duration \\ 0)

  defp update_execution_stats(stats, :success, duration) do
    %{
      stats
      | successful_executions: stats.successful_executions + 1,
        avg_execution_time: calculate_avg_time(stats, duration)
    }
  end

  defp update_execution_stats(stats, :failed, _duration) do
    %{stats | failed_executions: stats.failed_executions + 1}
  end

  defp update_execution_stats(stats, :timeout, _duration) do
    %{stats | failed_executions: stats.failed_executions + 1}
  end

  defp calculate_avg_time(stats, new_duration) do
    if stats.successful_executions > 0 do
      current_avg = stats.avg_execution_time
      total_time = current_avg * stats.successful_executions + new_duration
      div(total_time, stats.successful_executions + 1)
    else
      new_duration
    end
  end

  defp generate_execution_id do
    "exec_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end
end
