defmodule Presto.BetaNetwork do
  @moduledoc """
  Beta network implementation for the RETE algorithm.

  The beta network handles joining facts from different sources (alpha nodes or other beta nodes)
  and stores partial matches for efficient incremental processing.
  """

  use GenServer

  alias Presto.AlphaNetwork

  @type join_condition :: {:join, String.t(), String.t(), atom() | [atom()]}
  @type beta_node :: %{
          id: String.t(),
          left_input: String.t(),
          right_input: String.t(),
          join_keys: [atom()],
          left_type: :alpha | :beta,
          right_type: :alpha | :beta
        }

  # Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec create_beta_node(GenServer.server(), join_condition()) :: {:ok, String.t()}
  def create_beta_node(pid, join_condition) do
    GenServer.call(pid, {:create_beta_node, join_condition})
  end

  @spec remove_beta_node(GenServer.server(), String.t()) :: :ok
  def remove_beta_node(pid, node_id) do
    GenServer.call(pid, {:remove_beta_node, node_id})
  end

  @spec get_beta_nodes(GenServer.server()) :: %{String.t() => beta_node()}
  def get_beta_nodes(pid) do
    GenServer.call(pid, :get_beta_nodes)
  end

  @spec get_beta_node_info(GenServer.server(), String.t()) :: beta_node() | nil
  def get_beta_node_info(pid, node_id) do
    GenServer.call(pid, {:get_beta_node_info, node_id})
  end

  @spec process_alpha_changes(GenServer.server()) :: :ok
  def process_alpha_changes(pid) do
    GenServer.cast(pid, :process_alpha_changes)
  end

  @spec get_beta_memory(GenServer.server(), String.t()) :: [map()]
  def get_beta_memory(pid, node_id) do
    GenServer.call(pid, {:get_beta_memory, node_id})
  end

  @spec get_partial_matches(GenServer.server(), String.t()) :: [map()]
  def get_partial_matches(pid, node_id) do
    GenServer.call(pid, {:get_partial_matches, node_id})
  end

  @spec clear_beta_memory(GenServer.server(), String.t()) :: :ok
  def clear_beta_memory(pid, node_id) do
    GenServer.call(pid, {:clear_beta_memory, node_id})
  end

  @spec get_process_count(GenServer.server()) :: integer()
  def get_process_count(pid) do
    GenServer.call(pid, :get_process_count)
  end

  # Server implementation

  @impl true
  def init(opts) do
    alpha_network = Keyword.fetch!(opts, :alpha_network)

    state = %{
      alpha_network: alpha_network,
      beta_nodes: %{},
      # ETS table for beta memories (complete joins)
      beta_memories: :ets.new(:beta_memories, [:set, :private]),
      # ETS table for partial matches (incomplete joins)
      partial_matches: :ets.new(:partial_matches, [:set, :private]),
      # Track processing for performance monitoring
      process_count: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:create_beta_node, join_condition}, _from, state) do
    node_id = generate_node_id()

    case parse_join_condition(join_condition) do
      {:ok, beta_node} ->
        node = Map.put(beta_node, :id, node_id)
        new_nodes = Map.put(state.beta_nodes, node_id, node)

        # Initialize empty memories for this node
        :ets.insert(state.beta_memories, {node_id, []})
        :ets.insert(state.partial_matches, {node_id, []})

        new_state = %{state | beta_nodes: new_nodes}
        {:reply, {:ok, node_id}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:remove_beta_node, node_id}, _from, state) do
    new_nodes = Map.delete(state.beta_nodes, node_id)
    :ets.delete(state.beta_memories, node_id)
    :ets.delete(state.partial_matches, node_id)

    new_state = %{state | beta_nodes: new_nodes}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_beta_nodes, _from, state) do
    {:reply, state.beta_nodes, state}
  end

  @impl true
  def handle_call({:get_beta_node_info, node_id}, _from, state) do
    node_info = Map.get(state.beta_nodes, node_id)
    {:reply, node_info, state}
  end

  @impl true
  def handle_call({:get_beta_memory, node_id}, _from, state) do
    case :ets.lookup(state.beta_memories, node_id) do
      [{^node_id, matches}] -> {:reply, matches, state}
      [] -> {:reply, [], state}
    end
  end

  @impl true
  def handle_call({:get_partial_matches, node_id}, _from, state) do
    case :ets.lookup(state.partial_matches, node_id) do
      [{^node_id, matches}] -> {:reply, matches, state}
      [] -> {:reply, [], state}
    end
  end

  @impl true
  def handle_call({:clear_beta_memory, node_id}, _from, state) do
    :ets.insert(state.beta_memories, {node_id, []})
    :ets.insert(state.partial_matches, {node_id, []})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:get_process_count, _from, state) do
    {:reply, state.process_count, state}
  end

  @impl true
  def handle_cast(:process_alpha_changes, state) do
    new_state = process_all_joins(state)
    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    :ets.delete(state.beta_memories)
    :ets.delete(state.partial_matches)
    :ok
  end

  # Private functions

  defp generate_node_id do
    "beta_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end

  defp parse_join_condition({:join, left_input, right_input, join_keys}) do
    # Determine input types (alpha or beta)
    left_type = if String.starts_with?(left_input, "alpha_"), do: :alpha, else: :beta
    right_type = if String.starts_with?(right_input, "alpha_"), do: :alpha, else: :beta

    # Normalize join keys to list
    normalized_keys =
      case join_keys do
        key when is_atom(key) -> [key]
        keys when is_list(keys) -> keys
        _ -> {:error, :invalid_join_keys}
      end

    case normalized_keys do
      {:error, reason} ->
        {:error, reason}

      keys ->
        {:ok,
         %{
           left_input: left_input,
           right_input: right_input,
           join_keys: keys,
           left_type: left_type,
           right_type: right_type
         }}
    end
  end

  defp parse_join_condition(_) do
    {:error, :invalid_join_condition}
  end

  defp process_all_joins(state) do
    # Process beta nodes in dependency order (nodes that don't depend on other betas first)
    ordered_nodes = order_beta_nodes_by_dependencies(state.beta_nodes)

    new_state =
      Enum.reduce(ordered_nodes, state, fn {node_id, node}, acc_state ->
        process_beta_node_joins(node_id, node, acc_state)
      end)

    %{new_state | process_count: new_state.process_count + 1}
  end

  defp order_beta_nodes_by_dependencies(beta_nodes) do
    # Sort nodes so that those with alpha inputs come before those with beta inputs
    Enum.sort_by(beta_nodes, fn {_node_id, node} ->
      case {node.left_type, node.right_type} do
        # Both inputs are alpha nodes - process first
        {:alpha, :alpha} -> 0
        # One input is beta - process later
        {:alpha, :beta} -> 1
        # One input is beta - process later
        {:beta, :alpha} -> 1
        # Both inputs are beta - process last
        {:beta, :beta} -> 2
      end
    end)
  end

  defp process_beta_node_joins(node_id, node, state) do
    # Get input data from left and right sources
    left_data = get_input_data(node.left_input, node.left_type, state)
    right_data = get_input_data(node.right_input, node.right_type, state)

    # Perform joins
    joins = perform_joins(left_data, right_data, node.join_keys)

    # Update beta memory
    :ets.insert(state.beta_memories, {node_id, joins})

    # Update partial matches (for now, simple implementation)
    partial_matches = find_partial_matches(left_data, right_data, node.join_keys)
    :ets.insert(state.partial_matches, {node_id, partial_matches})

    state
  end

  defp get_input_data(input_id, :alpha, state) do
    # Get data from alpha network
    AlphaNetwork.get_alpha_memory(state.alpha_network, input_id)
  end

  defp get_input_data(input_id, :beta, state) do
    # Get data from another beta node
    case :ets.lookup(state.beta_memories, input_id) do
      [{^input_id, matches}] -> matches
      [] -> []
    end
  end

  defp perform_joins(left_data, right_data, join_keys) do
    # Perform cartesian product with join condition filtering
    for left_match <- left_data,
        right_match <- right_data,
        join_condition_met?(left_match, right_match, join_keys) do
      # Merge the matches
      Map.merge(left_match, right_match)
    end
  end

  defp join_condition_met?(left_match, right_match, join_keys) do
    Enum.all?(join_keys, fn key ->
      Map.get(left_match, key) == Map.get(right_match, key) and
        Map.has_key?(left_match, key) and
        Map.has_key?(right_match, key)
    end)
  end

  defp find_partial_matches(left_data, right_data, join_keys) do
    # Find left-side matches that don't have corresponding right-side matches
    left_without_right =
      for left_match <- left_data,
          not has_matching_right?(left_match, right_data, join_keys) do
        left_match
      end

    # Find right-side matches that don't have corresponding left-side matches
    right_without_left =
      for right_match <- right_data,
          not has_matching_left?(right_match, left_data, join_keys) do
        right_match
      end

    left_without_right ++ right_without_left
  end

  defp has_matching_right?(left_match, right_data, join_keys) do
    Enum.any?(right_data, fn right_match ->
      join_condition_met?(left_match, right_match, join_keys)
    end)
  end

  defp has_matching_left?(right_match, left_data, join_keys) do
    Enum.any?(left_data, fn left_match ->
      join_condition_met?(left_match, right_match, join_keys)
    end)
  end
end
