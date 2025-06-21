defmodule Presto.RuleEngine.State do
  @moduledoc """
  Core state management for the Presto Rule Engine.

  This module encapsulates the state structure and provides basic lifecycle
  operations for the RETE-based rule engine state. It handles only the state
  struct definition, creation, cleanup, and ETS table resource management.

  Domain-specific operations are handled by dedicated modules:
  - WorkingMemory: Fact operations
  - AlphaNetwork: Alpha network processing
  - BetaNetworkCoordinator: Beta network lifecycle
  - RuleManager: Rule management
  - Statistics: Performance tracking
  - Configuration: Optimization settings
  - FactLineage: Provenance tracking
  """

  require Logger
  alias Presto.Logger, as: PrestoLogger

  @enforce_keys [
    :engine_id,
    :facts_table,
    :changes_table,
    :alpha_memories,
    :compiled_patterns,
    :rule_statistics_table
  ]

  defstruct [
    # Engine Core
    :engine_id,
    :beta_network,

    # ETS Tables (resource management)
    :facts_table,
    :changes_table,
    :alpha_memories,
    :compiled_patterns,
    :rule_statistics_table,

    # Working Memory State
    tracking_changes: false,
    change_counter: 0,

    # Alpha Network State
    alpha_nodes: %{},
    fact_type_index: %{},

    # Rules Management
    rules: %{},
    rule_networks: %{},
    rule_analyses: %{},
    fast_path_rules: %{},
    last_execution_order: [],

    # Statistics
    rule_statistics: %{},
    engine_statistics: %{
      total_facts: 0,
      total_rules: 0,
      total_rule_firings: 0,
      last_execution_time: 0,
      fast_path_executions: 0,
      rete_network_executions: 0,
      alpha_nodes_saved_by_sharing: 0
    },

    # Incremental Processing
    last_incremental_execution: 0,
    facts_since_incremental: [],

    # Configuration
    optimization_config: %{
      enable_fast_path: false,
      enable_alpha_sharing: true,
      enable_rule_batching: true,
      fast_path_threshold: 2,
      sharing_threshold: 2,
      hash_join_threshold: 50,
      enable_pattern_compilation_cache: true,
      enable_statistics_collection: true,
      memory_pressure_threshold_mb: 1000,
      auto_tune_thresholds: false
    },

    # Fact Lineage Tracking
    fact_lineage: %{},
    fact_generation: 0,
    result_lineage: %{}
  ]

  @type t :: %__MODULE__{
          engine_id: String.t(),
          beta_network: pid() | nil,
          facts_table: :ets.table(),
          changes_table: :ets.table(),
          alpha_memories: :ets.table(),
          compiled_patterns: :ets.table(),
          rule_statistics_table: :ets.table(),
          tracking_changes: boolean(),
          change_counter: non_neg_integer(),
          alpha_nodes: %{String.t() => map()},
          fact_type_index: %{atom() => [String.t()]},
          rules: %{atom() => map()},
          rule_networks: %{atom() => map()},
          rule_analyses: %{atom() => map()},
          fast_path_rules: %{atom() => [atom()]},
          last_execution_order: [atom()],
          rule_statistics: %{atom() => map()},
          engine_statistics: map(),
          last_incremental_execution: integer(),
          facts_since_incremental: [tuple()],
          optimization_config: map(),
          fact_lineage: %{String.t() => map()},
          fact_generation: non_neg_integer(),
          result_lineage: %{String.t() => map()}
        }

  # Public API

  @doc """
  Creates a new rule engine state with the given options.

  This function only handles the core state initialization and ETS table creation.
  Beta network coordination is handled by BetaNetworkCoordinator.

  ## Options

  - `:engine_id` - Unique identifier for the engine instance
  - Other options are passed through for initialization

  ## Returns

  `{:ok, state}` on success, `{:error, reason}` on failure.
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts \\ []) do
    engine_id = Keyword.get(opts, :engine_id, generate_engine_id())
    PrestoLogger.log_engine_lifecycle(:info, engine_id, "initializing_state", %{opts: opts})

    try do
      # Create ETS tables for consolidated working memory and alpha network
      facts_table = create_facts_table()
      changes_table = create_changes_table()
      alpha_memories = create_alpha_memories_table()
      compiled_patterns = create_compiled_patterns_table()
      rule_statistics_table = create_rule_statistics_table()

      PrestoLogger.log_engine_lifecycle(:debug, engine_id, "ets_tables_created", %{
        facts_table: facts_table,
        changes_table: changes_table,
        alpha_memories: alpha_memories,
        compiled_patterns: compiled_patterns,
        rule_statistics_table: rule_statistics_table
      })

      state = %__MODULE__{
        engine_id: engine_id,
        facts_table: facts_table,
        changes_table: changes_table,
        alpha_memories: alpha_memories,
        compiled_patterns: compiled_patterns,
        rule_statistics_table: rule_statistics_table
      }

      PrestoLogger.log_engine_lifecycle(:info, engine_id, "state_initialized", %{
        state_keys: Map.keys(state)
      })

      {:ok, state}
    rescue
      error ->
        PrestoLogger.log_error(error, %{engine_id: engine_id, stage: "state_initialization"})
        {:error, {:state_initialization_failed, error}}
    end
  end

  @doc """
  Cleans up state resources, including ETS tables.
  Beta network cleanup is handled by BetaNetworkCoordinator.
  """
  @spec cleanup(t()) :: :ok
  def cleanup(%__MODULE__{} = state) do
    PrestoLogger.log_engine_lifecycle(:info, state.engine_id, "cleaning_up_state", %{})

    # Clean up ETS tables
    :ets.delete(state.facts_table)
    :ets.delete(state.changes_table)
    :ets.delete(state.alpha_memories)
    :ets.delete(state.compiled_patterns)
    :ets.delete(state.rule_statistics_table)

    PrestoLogger.log_engine_lifecycle(:info, state.engine_id, "state_cleaned_up", %{})
    :ok
  end

  @doc """
  Updates the beta network pid in the state.
  """
  @spec set_beta_network(t(), pid() | nil) :: t()
  def set_beta_network(%__MODULE__{} = state, beta_network_pid) do
    %{state | beta_network: beta_network_pid}
  end

  @doc """
  Gets the engine ID from the state.
  """
  @spec get_engine_id(t()) :: String.t()
  def get_engine_id(%__MODULE__{} = state), do: state.engine_id

  @doc """
  Gets the beta network pid from the state.
  """
  @spec get_beta_network(t()) :: pid() | nil
  def get_beta_network(%__MODULE__{} = state), do: state.beta_network

  @doc """
  Gets the facts table from the state.
  """
  @spec get_facts_table(t()) :: :ets.table()
  def get_facts_table(%__MODULE__{} = state), do: state.facts_table

  @doc """
  Gets the changes table from the state.
  """
  @spec get_changes_table(t()) :: :ets.table()
  def get_changes_table(%__MODULE__{} = state), do: state.changes_table

  @doc """
  Gets the alpha memories table from the state.
  """
  @spec get_alpha_memories(t()) :: :ets.table()
  def get_alpha_memories(%__MODULE__{} = state), do: state.alpha_memories

  @doc """
  Gets the compiled patterns table from the state.
  """
  @spec get_compiled_patterns(t()) :: :ets.table()
  def get_compiled_patterns(%__MODULE__{} = state), do: state.compiled_patterns

  @doc """
  Gets the rule statistics table from the state.
  """
  @spec get_rule_statistics_table(t()) :: :ets.table()
  def get_rule_statistics_table(%__MODULE__{} = state), do: state.rule_statistics_table

  # Private functions

  defp generate_engine_id do
    "engine_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp create_facts_table do
    :ets.new(:facts, [
      :set,
      :public,
      {:read_concurrency, true},
      {:write_concurrency, true},
      {:heir, self(), nil}
    ])
  end

  defp create_changes_table do
    :ets.new(:changes, [:ordered_set, :private])
  end

  defp create_alpha_memories_table do
    :ets.new(:alpha_memories, [
      :set,
      :public,
      {:read_concurrency, true},
      {:write_concurrency, true},
      {:heir, self(), nil}
    ])
  end

  defp create_compiled_patterns_table do
    :ets.new(:compiled_patterns, [
      :set,
      :public,
      {:read_concurrency, true},
      {:heir, self(), nil}
    ])
  end

  defp create_rule_statistics_table do
    :ets.new(:rule_statistics, [
      :set,
      :public,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])
  end
end
