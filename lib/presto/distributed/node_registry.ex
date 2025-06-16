defmodule Presto.Distributed.NodeRegistry do
  @moduledoc """
  Node registry for managing cluster membership and node discovery.

  Maintains a registry of all nodes in the cluster with their capabilities,
  status, and metadata. Provides efficient lookups and notifications for
  node changes.
  """

  use GenServer
  require Logger

  alias Presto.Logger, as: PrestoLogger

  @type node_entry :: %{
          node_id: String.t(),
          node: node(),
          pid: pid(),
          type: :coordinator | :worker | :storage | :gateway,
          status: :active | :suspect | :failed | :joining | :leaving,
          capabilities: map(),
          metadata: map(),
          registered_at: integer(),
          last_seen: integer()
        }

  @type registry_state :: %{
          nodes: %{String.t() => node_entry()},
          local_node_id: String.t(),
          watchers: %{pid() => [String.t()]},
          node_by_erlang_node: %{node() => String.t()}
        }

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec register_node(GenServer.server(), node_entry()) :: :ok | {:error, term()}
  def register_node(pid, node_entry) do
    GenServer.call(pid, {:register_node, node_entry})
  end

  @spec unregister_node(GenServer.server(), String.t()) :: :ok
  def unregister_node(pid, node_id) do
    GenServer.call(pid, {:unregister_node, node_id})
  end

  @spec update_node(GenServer.server(), String.t(), map()) :: :ok | {:error, term()}
  def update_node(pid, node_id, updates) do
    GenServer.call(pid, {:update_node, node_id, updates})
  end

  @spec get_node(GenServer.server(), String.t()) :: node_entry() | nil
  def get_node(pid, node_id) do
    GenServer.call(pid, {:get_node, node_id})
  end

  @spec get_all_nodes(GenServer.server()) :: [node_entry()]
  def get_all_nodes(pid) do
    GenServer.call(pid, :get_all_nodes)
  end

  @spec get_nodes_by_type(GenServer.server(), atom()) :: [node_entry()]
  def get_nodes_by_type(pid, node_type) do
    GenServer.call(pid, {:get_nodes_by_type, node_type})
  end

  @spec get_nodes_by_status(GenServer.server(), atom()) :: [node_entry()]
  def get_nodes_by_status(pid, status) do
    GenServer.call(pid, {:get_nodes_by_status, status})
  end

  @spec watch_node(GenServer.server(), String.t()) :: :ok
  def watch_node(pid, node_id) do
    GenServer.call(pid, {:watch_node, node_id})
  end

  @spec unwatch_node(GenServer.server(), String.t()) :: :ok
  def unwatch_node(pid, node_id) do
    GenServer.call(pid, {:unwatch_node, node_id})
  end

  @spec find_node_by_erlang_node(GenServer.server(), node()) :: String.t() | nil
  def find_node_by_erlang_node(pid, erlang_node) do
    GenServer.call(pid, {:find_node_by_erlang_node, erlang_node})
  end

  # Server implementation

  @impl true
  def init(opts) do
    local_node_id = Keyword.get(opts, :local_node_id, generate_node_id())

    state = %{
      nodes: %{},
      local_node_id: local_node_id,
      watchers: %{},
      node_by_erlang_node: %{}
    }

    PrestoLogger.log_distributed(:info, local_node_id, "node_registry_started", %{})

    {:ok, state}
  end

  @impl true
  def handle_call({:register_node, node_entry}, {from_pid, _}, state) do
    case validate_node_entry(node_entry) do
      :ok ->
        # Add monitoring for the registering process
        Process.monitor(from_pid)

        # Update registry
        updated_nodes = Map.put(state.nodes, node_entry.node_id, node_entry)

        updated_node_mapping =
          Map.put(state.node_by_erlang_node, node_entry.node, node_entry.node_id)

        new_state = %{state | nodes: updated_nodes, node_by_erlang_node: updated_node_mapping}

        # Notify watchers
        notify_node_watchers(node_entry.node_id, {:node_registered, node_entry}, state)

        PrestoLogger.log_distributed(:info, state.local_node_id, "node_registered", %{
          node_id: node_entry.node_id,
          node_type: node_entry.type,
          erlang_node: node_entry.node
        })

        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:unregister_node, node_id}, _from, state) do
    case Map.get(state.nodes, node_id) do
      nil ->
        {:reply, :ok, state}

      node_entry ->
        # Remove from registry
        updated_nodes = Map.delete(state.nodes, node_id)
        updated_node_mapping = Map.delete(state.node_by_erlang_node, node_entry.node)

        new_state = %{state | nodes: updated_nodes, node_by_erlang_node: updated_node_mapping}

        # Notify watchers
        notify_node_watchers(node_id, {:node_unregistered, node_entry}, state)

        PrestoLogger.log_distributed(:info, state.local_node_id, "node_unregistered", %{
          node_id: node_id
        })

        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:update_node, node_id, updates}, _from, state) do
    case Map.get(state.nodes, node_id) do
      nil ->
        {:reply, {:error, :node_not_found}, state}

      existing_node ->
        updated_node =
          Map.merge(existing_node, updates)
          |> Map.put(:last_seen, System.monotonic_time(:millisecond))

        updated_nodes = Map.put(state.nodes, node_id, updated_node)
        new_state = %{state | nodes: updated_nodes}

        # Notify watchers
        notify_node_watchers(node_id, {:node_updated, updated_node}, state)

        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get_node, node_id}, _from, state) do
    node_entry = Map.get(state.nodes, node_id)
    {:reply, node_entry, state}
  end

  @impl true
  def handle_call(:get_all_nodes, _from, state) do
    nodes = Map.values(state.nodes)
    {:reply, nodes, state}
  end

  @impl true
  def handle_call({:get_nodes_by_type, node_type}, _from, state) do
    nodes =
      state.nodes
      |> Map.values()
      |> Enum.filter(fn node -> node.type == node_type end)

    {:reply, nodes, state}
  end

  @impl true
  def handle_call({:get_nodes_by_status, status}, _from, state) do
    nodes =
      state.nodes
      |> Map.values()
      |> Enum.filter(fn node -> node.status == status end)

    {:reply, nodes, state}
  end

  @impl true
  def handle_call({:watch_node, node_id}, {from_pid, _}, state) do
    # Add watcher
    existing_watches = Map.get(state.watchers, from_pid, [])
    updated_watches = [node_id | existing_watches] |> Enum.uniq()
    updated_watchers = Map.put(state.watchers, from_pid, updated_watches)

    # Monitor the watcher process
    Process.monitor(from_pid)

    new_state = %{state | watchers: updated_watchers}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:unwatch_node, node_id}, {from_pid, _}, state) do
    # Remove specific watch
    existing_watches = Map.get(state.watchers, from_pid, [])
    updated_watches = List.delete(existing_watches, node_id)

    updated_watchers =
      if Enum.empty?(updated_watches) do
        Map.delete(state.watchers, from_pid)
      else
        Map.put(state.watchers, from_pid, updated_watches)
      end

    new_state = %{state | watchers: updated_watchers}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:find_node_by_erlang_node, erlang_node}, _from, state) do
    node_id = Map.get(state.node_by_erlang_node, erlang_node)
    {:reply, node_id, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Handle process death - clean up watchers and possibly nodes
    updated_watchers = Map.delete(state.watchers, pid)

    # If the dead process was a registered node, mark it as failed
    dead_node = Enum.find(state.nodes, fn {_id, node} -> node.pid == pid end)

    updated_nodes =
      case dead_node do
        {node_id, node_entry} ->
          failed_node = %{
            node_entry
            | status: :failed,
              last_seen: System.monotonic_time(:millisecond)
          }

          updated = Map.put(state.nodes, node_id, failed_node)

          # Notify watchers of node failure
          notify_node_watchers(node_id, {:node_failed, failed_node}, state)

          PrestoLogger.log_distributed(:warning, state.local_node_id, "node_failed", %{
            node_id: node_id,
            reason: "process_down"
          })

          updated

        nil ->
          state.nodes
      end

    new_state = %{state | watchers: updated_watchers, nodes: updated_nodes}
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodedown, erlang_node}, state) do
    # Handle Erlang node disconnection
    case Map.get(state.node_by_erlang_node, erlang_node) do
      nil ->
        {:noreply, state}

      node_id ->
        case Map.get(state.nodes, node_id) do
          nil ->
            {:noreply, state}

          node_entry ->
            # Mark node as failed due to network partition
            failed_node = %{
              node_entry
              | status: :failed,
                last_seen: System.monotonic_time(:millisecond),
                metadata: Map.put(node_entry.metadata, :failure_reason, :network_partition)
            }

            updated_nodes = Map.put(state.nodes, node_id, failed_node)
            new_state = %{state | nodes: updated_nodes}

            # Notify watchers
            notify_node_watchers(node_id, {:node_failed, failed_node}, state)

            PrestoLogger.log_distributed(:warning, state.local_node_id, "node_network_failure", %{
              node_id: node_id,
              erlang_node: erlang_node
            })

            {:noreply, new_state}
        end
    end
  end

  # Private functions

  defp generate_node_id() do
    "node_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp validate_node_entry(node_entry) do
    required_fields = [:node_id, :node, :pid, :type, :status]

    case Enum.all?(required_fields, &Map.has_key?(node_entry, &1)) do
      true ->
        if is_binary(node_entry.node_id) and byte_size(node_entry.node_id) > 0 do
          :ok
        else
          {:error, :invalid_node_id}
        end

      false ->
        {:error, :missing_required_fields}
    end
  end

  defp notify_node_watchers(node_id, message, state) do
    state.watchers
    |> Enum.filter(fn {_pid, watched_nodes} -> node_id in watched_nodes end)
    |> Enum.each(fn {watcher_pid, _} ->
      send(watcher_pid, message)
    end)
  end
end
