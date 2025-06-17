defmodule Presto.Distributed.CommunicationProtocol do
  @moduledoc """
  Cross-node communication protocols for distributed RETE fact propagation.

  Implements efficient, reliable message passing between cluster nodes for:
  - Fact assertion/retraction propagation
  - Rule activation cascading across network segments
  - Alpha/beta network state synchronization
  - Distributed join operations and result aggregation
  """

  use GenServer
  require Logger

  alias Presto.Distributed.{NodeRegistry, PartitionManager}
  alias Presto.Logger, as: PrestoLogger

  @type node_id :: String.t()
  @type message_id :: String.t()
  @type fact :: tuple()
  @type rule_id :: atom()

  @type message_type ::
          :fact_assertion
          | :fact_retraction
          | :rule_activation
          | :join_request
          | :join_response
          | :aggregate_update
          | :network_sync
          | :heartbeat
          | :ack
          | :error

  @type propagation_message :: %{
          id: message_id(),
          type: message_type(),
          source_node: node_id(),
          target_nodes: [node_id()],
          payload: map(),
          timestamp: integer(),
          priority: :low | :normal | :high | :critical,
          consistency_level: :async | :sync | :quorum,
          retry_count: non_neg_integer(),
          expires_at: integer()
        }

  @type delivery_guarantee :: :at_most_once | :at_least_once | :exactly_once

  @type protocol_state :: %{
          local_node_id: node_id(),
          cluster_manager: pid(),
          node_registry: pid(),
          partition_manager: pid(),
          outbound_queue: %{node_id() => [propagation_message()]},
          inbound_buffer: %{message_id() => propagation_message()},
          pending_acks: %{message_id() => %{nodes: [node_id()], timeout: integer()}},
          message_handlers: %{message_type() => function()},
          connection_pool: %{node_id() => connection_state()},
          delivery_tracking: %{message_id() => delivery_record()},
          config: protocol_config()
        }

  @type connection_state :: %{
          node_id: node_id(),
          status: :connected | :connecting | :disconnected | :failed,
          last_activity: integer(),
          retry_count: non_neg_integer(),
          backoff_until: integer(),
          pending_messages: non_neg_integer()
        }

  @type delivery_record :: %{
          message_id: message_id(),
          target_nodes: [node_id()],
          delivered_nodes: [node_id()],
          failed_nodes: [node_id()],
          delivery_guarantee: delivery_guarantee(),
          created_at: integer(),
          completed_at: integer() | nil
        }

  @type protocol_config :: %{
          max_retries: pos_integer(),
          retry_backoff_base: pos_integer(),
          retry_backoff_max: pos_integer(),
          message_timeout: pos_integer(),
          batch_size: pos_integer(),
          queue_size_limit: pos_integer(),
          compression_enabled: boolean(),
          encryption_enabled: boolean(),
          heartbeat_interval: pos_integer(),
          connection_timeout: pos_integer()
        }

  @default_config %{
    max_retries: 3,
    retry_backoff_base: 1000,
    retry_backoff_max: 30_000,
    message_timeout: 10_000,
    batch_size: 100,
    queue_size_limit: 1000,
    compression_enabled: true,
    encryption_enabled: false,
    heartbeat_interval: 5_000,
    connection_timeout: 15_000
  }

  # Client API

  @spec start_link(pid(), pid(), pid(), keyword()) :: GenServer.on_start()
  def start_link(cluster_manager, node_registry, partition_manager, opts \\ []) do
    GenServer.start_link(__MODULE__, {cluster_manager, node_registry, partition_manager, opts},
      name: __MODULE__
    )
  end

  @spec propagate_fact_assertion(GenServer.server(), fact(), keyword()) :: :ok | {:error, term()}
  def propagate_fact_assertion(pid, fact, opts \\ []) do
    message = create_fact_assertion_message(fact, opts)
    GenServer.call(pid, {:send_message, message})
  end

  @spec propagate_fact_retraction(GenServer.server(), fact(), keyword()) :: :ok | {:error, term()}
  def propagate_fact_retraction(pid, fact, opts \\ []) do
    message = create_fact_retraction_message(fact, opts)
    GenServer.call(pid, {:send_message, message})
  end

  @spec propagate_rule_activation(GenServer.server(), rule_id(), map(), keyword()) ::
          :ok | {:error, term()}
  def propagate_rule_activation(pid, rule_id, activation_data, opts \\ []) do
    message = create_rule_activation_message(rule_id, activation_data, opts)
    GenServer.call(pid, {:send_message, message})
  end

  @spec send_join_request(GenServer.server(), node_id(), map()) :: :ok | {:error, term()}
  def send_join_request(pid, target_node, join_data) do
    message = create_join_request_message(target_node, join_data)
    GenServer.call(pid, {:send_message, message})
  end

  @spec send_aggregate_update(GenServer.server(), [node_id()], map()) :: :ok | {:error, term()}
  def send_aggregate_update(pid, target_nodes, aggregate_data) do
    message = create_aggregate_update_message(target_nodes, aggregate_data)
    GenServer.call(pid, {:send_message, message})
  end

  @spec broadcast_network_sync(GenServer.server(), map()) :: :ok | {:error, term()}
  def broadcast_network_sync(pid, sync_data) do
    message = create_network_sync_message(sync_data)
    GenServer.call(pid, {:broadcast_message, message})
  end

  @spec get_delivery_status(GenServer.server(), message_id()) :: delivery_record() | nil
  def get_delivery_status(pid, message_id) do
    GenServer.call(pid, {:get_delivery_status, message_id})
  end

  @spec get_connection_status(GenServer.server()) :: %{node_id() => connection_state()}
  def get_connection_status(pid) do
    GenServer.call(pid, :get_connection_status)
  end

  @spec register_message_handler(GenServer.server(), message_type(), function()) :: :ok
  def register_message_handler(pid, message_type, handler_function) do
    GenServer.call(pid, {:register_message_handler, message_type, handler_function})
  end

  # Server implementation

  @impl true
  def init({cluster_manager, node_registry, partition_manager, opts}) do
    config = Map.merge(@default_config, Map.new(opts))
    local_node_id = Keyword.get(opts, :local_node_id, generate_node_id())

    state = %{
      local_node_id: local_node_id,
      cluster_manager: cluster_manager,
      node_registry: node_registry,
      partition_manager: partition_manager,
      outbound_queue: %{},
      inbound_buffer: %{},
      pending_acks: %{},
      message_handlers: initialize_default_handlers(),
      connection_pool: %{},
      delivery_tracking: %{},
      config: config
    }

    # Schedule periodic tasks
    # Process messages every 100ms
    schedule_message_processing(100)
    schedule_heartbeat(config.heartbeat_interval)
    schedule_connection_maintenance(5_000)

    PrestoLogger.log_distributed(:info, local_node_id, "communication_protocol_started", %{
      batch_size: config.batch_size,
      compression_enabled: config.compression_enabled
    })

    {:ok, state}
  end

  @impl true
  def handle_call({:send_message, message}, _from, state) do
    case validate_message(message) do
      :ok ->
        new_state = enqueue_message(message, state)
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:broadcast_message, message}, _from, state) do
    case get_all_active_nodes(state) do
      [] ->
        {:reply, {:error, :no_active_nodes}, state}

      active_nodes ->
        broadcast_message = %{message | target_nodes: active_nodes}
        new_state = enqueue_message(broadcast_message, state)
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get_delivery_status, message_id}, _from, state) do
    delivery_record = Map.get(state.delivery_tracking, message_id)
    {:reply, delivery_record, state}
  end

  @impl true
  def handle_call(:get_connection_status, _from, state) do
    {:reply, state.connection_pool, state}
  end

  @impl true
  def handle_call({:register_message_handler, message_type, handler_function}, _from, state) do
    updated_handlers = Map.put(state.message_handlers, message_type, handler_function)
    new_state = %{state | message_handlers: updated_handlers}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:incoming_message, message}, state) do
    new_state = process_incoming_message(message, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:message_ack, message_id, source_node}, state) do
    new_state = process_message_ack(message_id, source_node, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:connection_status_change, node_id, new_status}, state) do
    new_state = update_connection_status(node_id, new_status, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:process_messages, state) do
    new_state = process_outbound_queues(state)
    schedule_message_processing(100)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:send_heartbeat, state) do
    new_state = send_heartbeat_messages(state)
    schedule_heartbeat(state.config.heartbeat_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:maintain_connections, state) do
    new_state = maintain_node_connections(state)
    schedule_connection_maintenance(5_000)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:retry_message, message_id}, state) do
    new_state = retry_failed_message(message_id, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:message_timeout, message_id}, state) do
    new_state = handle_message_timeout(message_id, state)
    {:noreply, new_state}
  end

  # Private implementation functions

  defp generate_node_id do
    "comm_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp generate_message_id do
    "msg_" <> (:crypto.strong_rand_bytes(12) |> Base.encode16(case: :lower))
  end

  defp initialize_default_handlers do
    %{
      :fact_assertion => &handle_fact_assertion/2,
      :fact_retraction => &handle_fact_retraction/2,
      :rule_activation => &handle_rule_activation/2,
      :join_request => &handle_join_request/2,
      :join_response => &handle_join_response/2,
      :aggregate_update => &handle_aggregate_update/2,
      :network_sync => &handle_network_sync/2,
      :heartbeat => &handle_heartbeat/2,
      :ack => &handle_ack/2,
      :error => &handle_error_message/2
    }
  end

  defp create_fact_assertion_message(fact, opts) do
    target_nodes = determine_target_nodes_for_fact(fact, opts)

    %{
      id: generate_message_id(),
      type: :fact_assertion,
      source_node: Keyword.get(opts, :source_node, "unknown"),
      target_nodes: target_nodes,
      payload: %{
        fact: fact,
        timestamp: System.monotonic_time(:millisecond),
        partition_hint: Keyword.get(opts, :partition_hint)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: Keyword.get(opts, :priority, :normal),
      consistency_level: Keyword.get(opts, :consistency, :async),
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, 30_000)
    }
  end

  defp create_fact_retraction_message(fact, opts) do
    target_nodes = determine_target_nodes_for_fact(fact, opts)

    %{
      id: generate_message_id(),
      type: :fact_retraction,
      source_node: Keyword.get(opts, :source_node, "unknown"),
      target_nodes: target_nodes,
      payload: %{
        fact: fact,
        timestamp: System.monotonic_time(:millisecond)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: Keyword.get(opts, :priority, :normal),
      consistency_level: Keyword.get(opts, :consistency, :async),
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, 30_000)
    }
  end

  defp create_rule_activation_message(rule_id, activation_data, opts) do
    target_nodes = Keyword.get(opts, :target_nodes, [])

    %{
      id: generate_message_id(),
      type: :rule_activation,
      source_node: Keyword.get(opts, :source_node, "unknown"),
      target_nodes: target_nodes,
      payload: %{
        rule_id: rule_id,
        activation_data: activation_data,
        timestamp: System.monotonic_time(:millisecond)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: Keyword.get(opts, :priority, :high),
      consistency_level: Keyword.get(opts, :consistency, :sync),
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, 10_000)
    }
  end

  defp create_join_request_message(target_node, join_data) do
    %{
      id: generate_message_id(),
      type: :join_request,
      source_node: "unknown",
      target_nodes: [target_node],
      payload: %{
        join_data: join_data,
        timestamp: System.monotonic_time(:millisecond)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: :high,
      consistency_level: :sync,
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + 15_000
    }
  end

  defp create_aggregate_update_message(target_nodes, aggregate_data) do
    %{
      id: generate_message_id(),
      type: :aggregate_update,
      source_node: "unknown",
      target_nodes: target_nodes,
      payload: %{
        aggregate_data: aggregate_data,
        timestamp: System.monotonic_time(:millisecond)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: :normal,
      consistency_level: :async,
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + 20_000
    }
  end

  defp create_network_sync_message(sync_data) do
    %{
      id: generate_message_id(),
      type: :network_sync,
      source_node: "unknown",
      # Will be set during broadcast
      target_nodes: [],
      payload: %{
        sync_data: sync_data,
        timestamp: System.monotonic_time(:millisecond)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: :low,
      consistency_level: :async,
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + 60_000
    }
  end

  defp determine_target_nodes_for_fact(fact, opts) do
    case Keyword.get(opts, :target_nodes) do
      nil ->
        # Determine target nodes based on partitioning
        determine_nodes_via_partitioning(fact, opts)

      explicit_nodes ->
        explicit_nodes
    end
  end

  defp determine_nodes_via_partitioning(fact, opts) do
    case Keyword.get(opts, :partition_manager) do
      nil ->
        []

      partition_manager ->
        get_partition_nodes(partition_manager, fact)
    end
  end

  defp get_partition_nodes(partition_manager, fact) do
    case PartitionManager.get_partition_for_fact(partition_manager, fact) do
      {:ok, partition_id} ->
        PartitionManager.get_nodes_for_partition(partition_manager, partition_id)

      {:error, _} ->
        []
    end
  end

  defp validate_message(message) do
    required_fields = [:id, :type, :target_nodes, :payload]

    case Enum.all?(required_fields, &Map.has_key?(message, &1)) do
      true ->
        if Enum.empty?(message.target_nodes) do
          {:error, :no_target_nodes}
        else
          :ok
        end

      false ->
        {:error, :missing_required_fields}
    end
  end

  defp enqueue_message(message, state) do
    # Update source node to local node
    updated_message = %{message | source_node: state.local_node_id}

    # Create delivery tracking record
    delivery_record = %{
      message_id: updated_message.id,
      target_nodes: updated_message.target_nodes,
      delivered_nodes: [],
      failed_nodes: [],
      delivery_guarantee: get_delivery_guarantee(updated_message.consistency_level),
      created_at: System.monotonic_time(:millisecond),
      completed_at: nil
    }

    # Add to delivery tracking
    updated_delivery_tracking =
      Map.put(state.delivery_tracking, updated_message.id, delivery_record)

    # Add to outbound queues for each target node
    updated_outbound_queue =
      Enum.reduce(updated_message.target_nodes, state.outbound_queue, fn node_id, acc ->
        existing_queue = Map.get(acc, node_id, [])

        # Check queue size limit
        if length(existing_queue) >= state.config.queue_size_limit do
          PrestoLogger.log_distributed(:warning, state.local_node_id, "queue_full", %{
            target_node: node_id,
            queue_size: length(existing_queue)
          })

          acc
        else
          updated_queue = existing_queue ++ [updated_message]
          Map.put(acc, node_id, updated_queue)
        end
      end)

    # Schedule timeout if needed
    if updated_message.consistency_level in [:sync, :quorum] do
      Process.send_after(
        self(),
        {:message_timeout, updated_message.id},
        state.config.message_timeout
      )
    end

    %{
      state
      | outbound_queue: updated_outbound_queue,
        delivery_tracking: updated_delivery_tracking
    }
  end

  defp get_delivery_guarantee(consistency_level) do
    case consistency_level do
      :async -> :at_most_once
      :sync -> :at_least_once
      :quorum -> :exactly_once
    end
  end

  defp get_all_active_nodes(state) do
    nodes = NodeRegistry.get_nodes_by_status(state.node_registry, :active)

    Enum.map(nodes, fn node -> node.node_id end)
    |> Enum.reject(fn node_id -> node_id == state.local_node_id end)
  end

  defp process_outbound_queues(state) do
    Enum.reduce(state.outbound_queue, state, fn {node_id, queue}, acc_state ->
      if Enum.empty?(queue) do
        acc_state
      else
        process_node_queue(node_id, queue, acc_state)
      end
    end)
  end

  defp process_node_queue(node_id, queue, state) do
    # Check connection status
    connection = Map.get(state.connection_pool, node_id, create_initial_connection(node_id))

    case connection.status do
      :connected ->
        # Send batch of messages
        {messages_to_send, remaining_queue} = Enum.split(queue, state.config.batch_size)

        case send_message_batch(node_id, messages_to_send, state) do
          :ok ->
            # Update queue and connection
            updated_queue = Map.put(state.outbound_queue, node_id, remaining_queue)

            updated_connection = %{
              connection
              | last_activity: System.monotonic_time(:millisecond),
                pending_messages: connection.pending_messages + length(messages_to_send)
            }

            updated_connections = Map.put(state.connection_pool, node_id, updated_connection)

            %{state | outbound_queue: updated_queue, connection_pool: updated_connections}

          {:error, reason} ->
            # Mark connection as failed
            failed_connection = %{
              connection
              | status: :failed,
                retry_count: connection.retry_count + 1,
                backoff_until: calculate_backoff_time(connection.retry_count, state.config)
            }

            updated_connections = Map.put(state.connection_pool, node_id, failed_connection)

            PrestoLogger.log_distributed(:warning, state.local_node_id, "message_send_failed", %{
              target_node: node_id,
              reason: reason,
              retry_count: failed_connection.retry_count
            })

            %{state | connection_pool: updated_connections}
        end

      _ ->
        # Connection not ready, try to establish
        establish_connection(node_id, state)
    end
  end

  defp create_initial_connection(node_id) do
    %{
      node_id: node_id,
      status: :disconnected,
      last_activity: 0,
      retry_count: 0,
      backoff_until: 0,
      pending_messages: 0
    }
  end

  defp send_message_batch(node_id, messages, state) do
    # Prepare batch payload
    batch_payload = %{
      source_node: state.local_node_id,
      messages: messages,
      batch_size: length(messages),
      compression: state.config.compression_enabled
    }

    # Apply compression if enabled
    final_payload =
      if state.config.compression_enabled do
        compress_payload(batch_payload)
      else
        batch_payload
      end

    # Send via network (simplified implementation)
    case send_to_node(node_id, final_payload) do
      :ok ->
        # Track messages as sent
        Enum.each(messages, fn message ->
          track_message_sent(message.id, node_id, state)
        end)

        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp send_to_node(node_id, payload) do
    # Placeholder for actual network communication
    # In real implementation, this would use distributed Erlang, TCP, or other protocols
    # Simulate network send
    case :rpc.call(
           String.to_atom(node_id),
           GenServer,
           :cast,
           [__MODULE__, {:incoming_message_batch, payload}],
           5000
         ) do
      {:badrpc, reason} -> {:error, reason}
      _ -> :ok
    end
  rescue
    error -> {:error, error}
  end

  defp compress_payload(payload) do
    # Simple compression using Erlang's built-in compression
    compressed_data = :erlang.term_to_binary(payload, [:compressed])

    %{
      compressed: true,
      data: compressed_data,
      original_size: byte_size(:erlang.term_to_binary(payload))
    }
  end

  defp track_message_sent(message_id, _node_id, state) do
    case Map.get(state.delivery_tracking, message_id) do
      nil ->
        :ok

      _delivery_record ->
        # Update delivery tracking (this would be more complex in real implementation)
        :ok
    end
  end

  defp establish_connection(node_id, state) do
    connection = Map.get(state.connection_pool, node_id, create_initial_connection(node_id))
    now = System.monotonic_time(:millisecond)

    if now >= connection.backoff_until do
      # Try to establish connection
      case attempt_connection(node_id) do
        :ok ->
          connected_connection = %{
            connection
            | status: :connected,
              last_activity: now,
              retry_count: 0,
              backoff_until: 0
          }

          updated_connections = Map.put(state.connection_pool, node_id, connected_connection)

          PrestoLogger.log_distributed(:info, state.local_node_id, "connection_established", %{
            target_node: node_id
          })

          %{state | connection_pool: updated_connections}

        {:error, _reason} ->
          failed_connection = %{
            connection
            | status: :failed,
              retry_count: connection.retry_count + 1,
              backoff_until: calculate_backoff_time(connection.retry_count, state.config)
          }

          updated_connections = Map.put(state.connection_pool, node_id, failed_connection)

          %{state | connection_pool: updated_connections}
      end
    else
      state
    end
  end

  defp attempt_connection(node_id) do
    # Placeholder for actual connection establishment
    # In real implementation, this would establish TCP connections, etc.
    case :net_adm.ping(String.to_atom(node_id)) do
      :pong -> :ok
      :pang -> {:error, :unreachable}
    end
  rescue
    _ -> {:error, :connection_failed}
  end

  defp calculate_backoff_time(retry_count, config) do
    backoff_ms =
      min(
        config.retry_backoff_base * :math.pow(2, retry_count),
        config.retry_backoff_max
      )

    System.monotonic_time(:millisecond) + round(backoff_ms)
  end

  defp process_incoming_message(message, state) do
    # Handle incoming message based on type
    case Map.get(state.message_handlers, message.type) do
      nil ->
        PrestoLogger.log_distributed(:warning, state.local_node_id, "unknown_message_type", %{
          message_type: message.type,
          source_node: message.source_node
        })

        state

      handler_function ->
        try do
          handler_function.(message, state)
        rescue
          error ->
            PrestoLogger.log_distributed(:error, state.local_node_id, "message_handler_error", %{
              message_type: message.type,
              error: error
            })

            # Send error response if needed
            send_error_response(message, error, state)
            state
        end
    end
  end

  defp send_error_response(original_message, error, state) do
    error_message = %{
      id: generate_message_id(),
      type: :error,
      source_node: state.local_node_id,
      target_nodes: [original_message.source_node],
      payload: %{
        original_message_id: original_message.id,
        error: inspect(error),
        timestamp: System.monotonic_time(:millisecond)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: :high,
      consistency_level: :async,
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + 10_000
    }

    enqueue_message(error_message, state)
  end

  # Message handler implementations

  defp handle_fact_assertion(message, state) do
    fact = message.payload.fact

    PrestoLogger.log_distributed(:debug, state.local_node_id, "fact_assertion_received", %{
      fact: fact,
      source_node: message.source_node
    })

    # Forward to local working memory or rule engine
    # This would integrate with the local RETE network

    # Send acknowledgment if required
    if message.consistency_level in [:sync, :quorum] do
      send_acknowledgment(message, state)
    end

    state
  end

  defp handle_fact_retraction(message, state) do
    fact = message.payload.fact

    PrestoLogger.log_distributed(:debug, state.local_node_id, "fact_retraction_received", %{
      fact: fact,
      source_node: message.source_node
    })

    # Forward to local working memory or rule engine

    # Send acknowledgment if required
    if message.consistency_level in [:sync, :quorum] do
      send_acknowledgment(message, state)
    end

    state
  end

  defp handle_rule_activation(message, state) do
    rule_id = message.payload.rule_id

    PrestoLogger.log_distributed(:debug, state.local_node_id, "rule_activation_received", %{
      rule_id: rule_id,
      source_node: message.source_node
    })

    # Process rule activation in local context

    # Send acknowledgment
    send_acknowledgment(message, state)

    state
  end

  defp handle_join_request(message, state) do
    join_data = message.payload.join_data

    PrestoLogger.log_distributed(:info, state.local_node_id, "join_request_received", %{
      source_node: message.source_node
    })

    # Process join request and send response
    response = create_join_response(message, join_data)
    enqueue_message(response, state)
  end

  defp handle_join_response(message, state) do
    PrestoLogger.log_distributed(:debug, state.local_node_id, "join_response_received", %{
      source_node: message.source_node
    })

    state
  end

  defp handle_aggregate_update(message, state) do
    PrestoLogger.log_distributed(:debug, state.local_node_id, "aggregate_update_received", %{
      source_node: message.source_node
    })

    # Process aggregate update

    state
  end

  defp handle_network_sync(message, state) do
    PrestoLogger.log_distributed(:debug, state.local_node_id, "network_sync_received", %{
      source_node: message.source_node
    })

    # Process network synchronization

    state
  end

  defp handle_heartbeat(message, state) do
    # Update connection status
    connection =
      Map.get(
        state.connection_pool,
        message.source_node,
        create_initial_connection(message.source_node)
      )

    updated_connection = %{
      connection
      | last_activity: System.monotonic_time(:millisecond),
        status: :connected
    }

    updated_connections = Map.put(state.connection_pool, message.source_node, updated_connection)

    %{state | connection_pool: updated_connections}
  end

  defp handle_ack(message, state) do
    # Process acknowledgment
    original_message_id = message.payload.original_message_id
    process_message_ack(original_message_id, message.source_node, state)
  end

  defp handle_error_message(message, state) do
    PrestoLogger.log_distributed(:warning, state.local_node_id, "error_message_received", %{
      source_node: message.source_node,
      error: message.payload.error
    })

    state
  end

  defp create_join_response(original_message, _join_data) do
    %{
      id: generate_message_id(),
      type: :join_response,
      # Will be set when enqueued
      source_node: "unknown",
      target_nodes: [original_message.source_node],
      payload: %{
        original_message_id: original_message.id,
        response_data: %{status: :accepted},
        timestamp: System.monotonic_time(:millisecond)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: :high,
      consistency_level: :sync,
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + 10_000
    }
  end

  defp send_acknowledgment(message, state) do
    ack_message = %{
      id: generate_message_id(),
      type: :ack,
      source_node: state.local_node_id,
      target_nodes: [message.source_node],
      payload: %{
        original_message_id: message.id,
        timestamp: System.monotonic_time(:millisecond)
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: :high,
      consistency_level: :async,
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + 5_000
    }

    enqueue_message(ack_message, state)
  end

  defp process_message_ack(message_id, source_node, state) do
    case Map.get(state.delivery_tracking, message_id) do
      nil ->
        state

      delivery_record ->
        updated_delivered_nodes = [source_node | delivery_record.delivered_nodes] |> Enum.uniq()
        updated_record = %{delivery_record | delivered_nodes: updated_delivered_nodes}

        # Check if delivery is complete
        if length(updated_record.delivered_nodes) >= length(updated_record.target_nodes) do
          completed_record = %{updated_record | completed_at: System.monotonic_time(:millisecond)}
          updated_tracking = Map.put(state.delivery_tracking, message_id, completed_record)
          %{state | delivery_tracking: updated_tracking}
        else
          updated_tracking = Map.put(state.delivery_tracking, message_id, updated_record)
          %{state | delivery_tracking: updated_tracking}
        end
    end
  end

  defp send_heartbeat_messages(state) do
    active_nodes = get_all_active_nodes(state)

    heartbeat_message = %{
      id: generate_message_id(),
      type: :heartbeat,
      source_node: state.local_node_id,
      target_nodes: active_nodes,
      payload: %{
        timestamp: System.monotonic_time(:millisecond),
        node_status: :active
      },
      timestamp: System.monotonic_time(:millisecond),
      priority: :low,
      consistency_level: :async,
      retry_count: 0,
      expires_at: System.monotonic_time(:millisecond) + state.config.heartbeat_interval * 2
    }

    if Enum.empty?(active_nodes) do
      state
    else
      enqueue_message(heartbeat_message, state)
    end
  end

  defp maintain_node_connections(state) do
    now = System.monotonic_time(:millisecond)

    # Clean up old connections and retry failed ones
    updated_connections =
      state.connection_pool
      |> Enum.into(%{}, fn {node_id, connection} ->
        cond do
          # Connection timed out
          connection.status == :connected and
              now - connection.last_activity > state.config.connection_timeout ->
            {node_id, %{connection | status: :disconnected}}

          # Ready to retry failed connection
          connection.status == :failed and now >= connection.backoff_until ->
            {node_id, %{connection | status: :disconnected}}

          true ->
            {node_id, connection}
        end
      end)

    %{state | connection_pool: updated_connections}
  end

  defp retry_failed_message(_message_id, state) do
    # Implementation for retrying failed messages
    state
  end

  defp handle_message_timeout(message_id, state) do
    case Map.get(state.delivery_tracking, message_id) do
      nil ->
        state

      delivery_record ->
        if delivery_record.completed_at == nil do
          # Message timed out
          PrestoLogger.log_distributed(:warning, state.local_node_id, "message_timeout", %{
            message_id: message_id,
            target_nodes: delivery_record.target_nodes,
            delivered_nodes: delivery_record.delivered_nodes
          })

          # Mark as completed with timeout
          timed_out_record = %{
            delivery_record
            | completed_at: System.monotonic_time(:millisecond),
              failed_nodes: delivery_record.target_nodes -- delivery_record.delivered_nodes
          }

          updated_tracking = Map.put(state.delivery_tracking, message_id, timed_out_record)
          %{state | delivery_tracking: updated_tracking}
        else
          state
        end
    end
  end

  defp update_connection_status(node_id, new_status, state) do
    connection = Map.get(state.connection_pool, node_id, create_initial_connection(node_id))

    updated_connection = %{
      connection
      | status: new_status,
        last_activity: System.monotonic_time(:millisecond)
    }

    updated_connections = Map.put(state.connection_pool, node_id, updated_connection)

    %{state | connection_pool: updated_connections}
  end

  defp schedule_message_processing(interval) do
    Process.send_after(self(), :process_messages, interval)
  end

  defp schedule_heartbeat(interval) do
    Process.send_after(self(), :send_heartbeat, interval)
  end

  defp schedule_connection_maintenance(interval) do
    Process.send_after(self(), :maintain_connections, interval)
  end
end
