defmodule Presto.Analytics.RuleProfiler do
  @moduledoc """
  Advanced rule performance profiling system for Presto RETE engines.

  Provides detailed profiling capabilities including:
  - Individual rule execution profiling
  - Alpha/Beta network traversal analysis
  - Memory allocation patterns per rule
  - Conflict resolution performance impact
  - Rule interaction and dependency analysis

  ## Profiling Modes

  ### Execution Profiling
  - Rule firing frequency and timing
  - Condition evaluation performance
  - Action execution costs
  - Memory allocations per rule

  ### Network Profiling
  - Alpha node activation patterns
  - Beta join efficiency
  - Working memory traversal costs
  - Pattern matching optimization opportunities

  ### Interaction Profiling
  - Rule conflict patterns
  - Fact sharing between rules
  - Rule execution cascades
  - Priority resolution impact

  ## Example Usage

      # Start profiling a specific rule
      RuleProfiler.start_rule_profiling(:customer_engine, :discount_rule, [
        profile_execution: true,
        profile_memory: true,
        profile_network: true
      ])

      # Profile all rules in an engine
      RuleProfiler.start_engine_profiling(:payment_engine, [
        sampling_rate: 0.1,
        max_duration: 300_000
      ])

      # Get detailed rule profile
      {:ok, profile} = RuleProfiler.get_rule_profile(:customer_engine, :discount_rule)

      # Generate optimization recommendations
      {:ok, recommendations} = RuleProfiler.analyze_rule_performance(:customer_engine)
  """

  use GenServer
  require Logger

  @type profiling_mode :: :execution | :network | :interaction | :comprehensive
  @type profiling_scope :: :single_rule | :engine | :cross_engine

  @type rule_profile :: %{
          rule_id: atom(),
          engine_name: atom(),
          execution_stats: execution_profile(),
          network_stats: network_profile(),
          memory_stats: memory_profile(),
          interaction_stats: interaction_profile(),
          optimization_score: float(),
          profiling_period: {DateTime.t(), DateTime.t()}
        }

  @type execution_profile :: %{
          total_executions: non_neg_integer(),
          avg_execution_time: float(),
          min_execution_time: float(),
          max_execution_time: float(),
          condition_evaluation_time: float(),
          action_execution_time: float(),
          success_rate: float(),
          failure_patterns: [String.t()]
        }

  @type network_profile :: %{
          alpha_activations: non_neg_integer(),
          beta_activations: non_neg_integer(),
          join_efficiency: float(),
          pattern_match_time: float(),
          working_memory_scans: non_neg_integer(),
          index_hits: non_neg_integer(),
          index_misses: non_neg_integer()
        }

  @type memory_profile :: %{
          total_allocations: non_neg_integer(),
          peak_memory_usage: non_neg_integer(),
          avg_memory_usage: non_neg_integer(),
          gc_pressure: float(),
          fact_creation_count: non_neg_integer(),
          fact_retraction_count: non_neg_integer()
        }

  @type interaction_profile :: %{
          conflicts_generated: non_neg_integer(),
          conflicts_resolved: non_neg_integer(),
          rule_dependencies: [atom()],
          fact_sharing_patterns: map(),
          cascading_executions: non_neg_integer(),
          priority_impacts: map()
        }

  ## Client API

  @doc """
  Starts the rule profiler.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts profiling a specific rule.
  """
  @spec start_rule_profiling(atom(), atom(), keyword()) :: :ok | {:error, term()}
  def start_rule_profiling(engine_name, rule_id, opts \\ []) do
    GenServer.call(__MODULE__, {:start_rule_profiling, engine_name, rule_id, opts})
  end

  @doc """
  Starts profiling all rules in an engine.
  """
  @spec start_engine_profiling(atom(), keyword()) :: :ok | {:error, term()}
  def start_engine_profiling(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:start_engine_profiling, engine_name, opts})
  end

  @doc """
  Stops profiling for a specific rule.
  """
  @spec stop_rule_profiling(atom(), atom()) :: :ok
  def stop_rule_profiling(engine_name, rule_id) do
    GenServer.call(__MODULE__, {:stop_rule_profiling, engine_name, rule_id})
  end

  @doc """
  Stops all profiling for an engine.
  """
  @spec stop_engine_profiling(atom()) :: :ok
  def stop_engine_profiling(engine_name) do
    GenServer.call(__MODULE__, {:stop_engine_profiling, engine_name})
  end

  @doc """
  Gets the profile for a specific rule.
  """
  @spec get_rule_profile(atom(), atom()) :: {:ok, rule_profile()} | {:error, term()}
  def get_rule_profile(engine_name, rule_id) do
    GenServer.call(__MODULE__, {:get_rule_profile, engine_name, rule_id})
  end

  @doc """
  Gets profiles for all rules in an engine.
  """
  @spec get_engine_profiles(atom()) :: {:ok, [rule_profile()]} | {:error, term()}
  def get_engine_profiles(engine_name) do
    GenServer.call(__MODULE__, {:get_engine_profiles, engine_name})
  end

  @doc """
  Analyzes rule performance and generates recommendations.
  """
  @spec analyze_rule_performance(atom()) :: {:ok, map()} | {:error, term()}
  def analyze_rule_performance(engine_name) do
    GenServer.call(__MODULE__, {:analyze_performance, engine_name}, 30_000)
  end

  @doc """
  Records a rule execution event for profiling.
  """
  @spec record_rule_execution(atom(), atom(), map()) :: :ok
  def record_rule_execution(engine_name, rule_id, execution_data) do
    GenServer.cast(__MODULE__, {:record_execution, engine_name, rule_id, execution_data})
  end

  @doc """
  Records network profiling data.
  """
  @spec record_network_activity(atom(), atom(), map()) :: :ok
  def record_network_activity(engine_name, rule_id, network_data) do
    GenServer.cast(__MODULE__, {:record_network, engine_name, rule_id, network_data})
  end

  @doc """
  Gets profiler statistics.
  """
  @spec get_profiler_stats() :: map()
  def get_profiler_stats do
    GenServer.call(__MODULE__, :get_profiler_stats)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Rule Profiler")

    state = %{
      # Configuration
      default_sampling_rate: Keyword.get(opts, :default_sampling_rate, 1.0),
      max_concurrent_profiles: Keyword.get(opts, :max_concurrent_profiles, 100),
      # 1 hour
      profile_retention_time: Keyword.get(opts, :profile_retention_time, 3600),
      min_execution_threshold: Keyword.get(opts, :min_execution_threshold, 10),

      # Active profiling sessions
      active_profiles: %{},
      profile_configs: %{},

      # Profile data storage
      execution_data: %{},
      network_data: %{},
      memory_data: %{},
      interaction_data: %{},

      # Analysis cache
      analysis_cache: %{},

      # Statistics
      stats: %{
        total_profiles_created: 0,
        active_profile_count: 0,
        total_executions_recorded: 0,
        total_analyses_performed: 0,
        cache_hits: 0,
        cache_misses: 0
      }
    }

    # Schedule periodic cleanup
    # 5 minutes
    Process.send_after(self(), :cleanup_profiles, 300_000)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:start_rule_profiling, engine_name, rule_id, opts}, _from, state) do
    if map_size(state.active_profiles) >= state.max_concurrent_profiles do
      {:reply, {:error, :max_profiles_exceeded}, state}
    else
      case start_profiling_session(engine_name, rule_id, opts, state) do
        {:ok, new_state} ->
          Logger.info("Started rule profiling",
            engine: engine_name,
            rule: rule_id
          )

          {:reply, :ok, new_state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  @impl GenServer
  def handle_call({:start_engine_profiling, engine_name, opts}, _from, state) do
    case start_engine_profiling_session(engine_name, opts, state) do
      {:ok, new_state} ->
        Logger.info("Started engine profiling", engine: engine_name)
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:stop_rule_profiling, engine_name, rule_id}, _from, state) do
    new_state = stop_profiling_session(engine_name, rule_id, state)

    Logger.info("Stopped rule profiling",
      engine: engine_name,
      rule: rule_id
    )

    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_call({:stop_engine_profiling, engine_name}, _from, state) do
    new_state = stop_engine_profiling_session(engine_name, state)
    Logger.info("Stopped engine profiling", engine: engine_name)
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_call({:get_rule_profile, engine_name, rule_id}, _from, state) do
    case generate_rule_profile(engine_name, rule_id, state) do
      {:ok, profile} ->
        {:reply, {:ok, profile}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:get_engine_profiles, engine_name}, _from, state) do
    {:ok, profiles} = generate_engine_profiles(engine_name, state)
    {:reply, {:ok, profiles}, state}
  end

  @impl GenServer
  def handle_call({:analyze_performance, engine_name}, _from, state) do
    {:ok, analysis, new_state} = perform_performance_analysis(engine_name, state)

    updated_stats = %{
      new_state.stats
      | total_analyses_performed: new_state.stats.total_analyses_performed + 1
    }

    {:reply, {:ok, analysis}, %{new_state | stats: updated_stats}}
  end

  @impl GenServer
  def handle_call(:get_profiler_stats, _from, state) do
    stats =
      Map.merge(state.stats, %{
        active_profile_count: map_size(state.active_profiles),
        cached_analyses: map_size(state.analysis_cache),
        total_execution_records: calculate_total_execution_records(state),
        memory_usage_mb: calculate_profiler_memory_usage(state)
      })

    {:reply, stats, state}
  end

  @impl GenServer
  def handle_cast({:record_execution, engine_name, rule_id, execution_data}, state) do
    profile_key = {engine_name, rule_id}

    case Map.get(state.active_profiles, profile_key) do
      nil ->
        # No active profile for this rule
        {:noreply, state}

      profile_config ->
        if should_sample_execution?(profile_config) do
          new_state = record_execution_data(engine_name, rule_id, execution_data, state)

          updated_stats = %{
            new_state.stats
            | total_executions_recorded: new_state.stats.total_executions_recorded + 1
          }

          {:noreply, %{new_state | stats: updated_stats}}
        else
          {:noreply, state}
        end
    end
  end

  @impl GenServer
  def handle_cast({:record_network, engine_name, rule_id, network_data}, state) do
    profile_key = {engine_name, rule_id}

    case Map.get(state.active_profiles, profile_key) do
      nil ->
        {:noreply, state}

      profile_config ->
        if profile_config.profile_network do
          new_state = record_network_data(engine_name, rule_id, network_data, state)
          {:noreply, new_state}
        else
          {:noreply, state}
        end
    end
  end

  @impl GenServer
  def handle_info(:cleanup_profiles, state) do
    new_state = cleanup_old_profiles(state)

    # Schedule next cleanup
    Process.send_after(self(), :cleanup_profiles, 300_000)

    {:noreply, new_state}
  end

  ## Private functions

  defp start_profiling_session(engine_name, rule_id, opts, state) do
    profile_config = %{
      engine_name: engine_name,
      rule_id: rule_id,
      profile_execution: Keyword.get(opts, :profile_execution, true),
      profile_memory: Keyword.get(opts, :profile_memory, true),
      profile_network: Keyword.get(opts, :profile_network, true),
      profile_interaction: Keyword.get(opts, :profile_interaction, false),
      sampling_rate: Keyword.get(opts, :sampling_rate, state.default_sampling_rate),
      # 1 hour
      max_duration: Keyword.get(opts, :max_duration, 3_600_000),
      started_at: DateTime.utc_now()
    }

    profile_key = {engine_name, rule_id}

    # Check if profile already exists
    if Map.has_key?(state.active_profiles, profile_key) do
      {:error, :profile_already_active}
    else
      new_active = Map.put(state.active_profiles, profile_key, profile_config)

      # Initialize data structures for this profile
      new_execution_data = Map.put(state.execution_data, profile_key, [])
      new_network_data = Map.put(state.network_data, profile_key, [])
      new_memory_data = Map.put(state.memory_data, profile_key, [])
      new_interaction_data = Map.put(state.interaction_data, profile_key, [])

      new_stats = %{state.stats | total_profiles_created: state.stats.total_profiles_created + 1}

      new_state = %{
        state
        | active_profiles: new_active,
          execution_data: new_execution_data,
          network_data: new_network_data,
          memory_data: new_memory_data,
          interaction_data: new_interaction_data,
          stats: new_stats
      }

      # Schedule automatic profile termination
      Process.send_after(self(), {:stop_profile, profile_key}, profile_config.max_duration)

      {:ok, new_state}
    end
  end

  defp start_engine_profiling_session(engine_name, opts, state) do
    # Get list of rules for the engine
    {:ok, rule_ids} = get_engine_rules(engine_name)
    # Start profiling for each rule
    Enum.reduce_while(rule_ids, {:ok, state}, fn rule_id, {:ok, acc_state} ->
      case start_profiling_session(engine_name, rule_id, opts, acc_state) do
        {:ok, new_state} -> {:cont, {:ok, new_state}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp stop_profiling_session(engine_name, rule_id, state) do
    profile_key = {engine_name, rule_id}

    # Remove from active profiles
    new_active = Map.delete(state.active_profiles, profile_key)

    # Keep data for analysis (cleanup will remove it later)
    %{state | active_profiles: new_active}
  end

  defp stop_engine_profiling_session(engine_name, state) do
    # Stop all profiles for the engine
    profiles_to_stop =
      Enum.filter(state.active_profiles, fn {{eng_name, _rule_id}, _config} ->
        eng_name == engine_name
      end)

    Enum.reduce(profiles_to_stop, state, fn {{eng_name, rule_id}, _config}, acc_state ->
      stop_profiling_session(eng_name, rule_id, acc_state)
    end)
  end

  defp should_sample_execution?(profile_config) do
    :rand.uniform() <= profile_config.sampling_rate
  end

  defp record_execution_data(engine_name, rule_id, execution_data, state) do
    profile_key = {engine_name, rule_id}

    timestamped_data =
      Map.merge(execution_data, %{
        timestamp: DateTime.utc_now(),
        engine_name: engine_name,
        rule_id: rule_id
      })

    current_data = Map.get(state.execution_data, profile_key, [])
    updated_data = [timestamped_data | current_data]

    # Limit data size to prevent memory issues
    trimmed_data = Enum.take(updated_data, 1000)

    new_execution_data = Map.put(state.execution_data, profile_key, trimmed_data)

    %{state | execution_data: new_execution_data}
  end

  defp record_network_data(engine_name, rule_id, network_data, state) do
    profile_key = {engine_name, rule_id}

    timestamped_data =
      Map.merge(network_data, %{
        timestamp: DateTime.utc_now(),
        engine_name: engine_name,
        rule_id: rule_id
      })

    current_data = Map.get(state.network_data, profile_key, [])
    updated_data = [timestamped_data | current_data]
    trimmed_data = Enum.take(updated_data, 1000)

    new_network_data = Map.put(state.network_data, profile_key, trimmed_data)

    %{state | network_data: new_network_data}
  end

  defp generate_rule_profile(engine_name, rule_id, state) do
    profile_key = {engine_name, rule_id}

    # Check if we have data for this rule
    execution_data = Map.get(state.execution_data, profile_key, [])
    network_data = Map.get(state.network_data, profile_key, [])
    memory_data = Map.get(state.memory_data, profile_key, [])
    interaction_data = Map.get(state.interaction_data, profile_key, [])

    if length(execution_data) < state.min_execution_threshold do
      {:error, :insufficient_data}
    else
      # Generate profile from collected data
      profile = %{
        rule_id: rule_id,
        engine_name: engine_name,
        execution_stats: analyze_execution_data(execution_data),
        network_stats: analyze_network_data(network_data),
        memory_stats: analyze_memory_data(memory_data),
        interaction_stats: analyze_interaction_data(interaction_data),
        optimization_score: calculate_optimization_score(execution_data, network_data),
        profiling_period: determine_profiling_period(execution_data)
      }

      {:ok, profile}
    end
  end

  defp generate_engine_profiles(engine_name, state) do
    # Get all rules for the engine that have profile data
    engine_profiles =
      state.execution_data
      |> Enum.filter(fn {{eng_name, _rule_id}, _data} -> eng_name == engine_name end)
      |> Enum.map(fn {{^engine_name, rule_id}, _data} ->
        case generate_rule_profile(engine_name, rule_id, state) do
          {:ok, profile} -> profile
          {:error, _} -> nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    {:ok, engine_profiles}
  end

  defp analyze_execution_data(execution_data) do
    if Enum.empty?(execution_data) do
      %{
        total_executions: 0,
        avg_execution_time: 0.0,
        min_execution_time: 0.0,
        max_execution_time: 0.0,
        condition_evaluation_time: 0.0,
        action_execution_time: 0.0,
        success_rate: 0.0,
        failure_patterns: []
      }
    else
      execution_times = Enum.map(execution_data, &Map.get(&1, :execution_time, 0))
      successful_executions = Enum.count(execution_data, &Map.get(&1, :success, false))

      %{
        total_executions: length(execution_data),
        avg_execution_time: Enum.sum(execution_times) / length(execution_times),
        min_execution_time: Enum.min(execution_times),
        max_execution_time: Enum.max(execution_times),
        condition_evaluation_time: calculate_avg_condition_time(execution_data),
        action_execution_time: calculate_avg_action_time(execution_data),
        success_rate: successful_executions / length(execution_data),
        failure_patterns: identify_failure_patterns(execution_data)
      }
    end
  end

  defp analyze_network_data(network_data) do
    if Enum.empty?(network_data) do
      %{
        alpha_activations: 0,
        beta_activations: 0,
        join_efficiency: 0.0,
        pattern_match_time: 0.0,
        working_memory_scans: 0,
        index_hits: 0,
        index_misses: 0
      }
    else
      %{
        alpha_activations: sum_network_field(network_data, :alpha_activations),
        beta_activations: sum_network_field(network_data, :beta_activations),
        join_efficiency: calculate_join_efficiency(network_data),
        pattern_match_time: avg_network_field(network_data, :pattern_match_time),
        working_memory_scans: sum_network_field(network_data, :working_memory_scans),
        index_hits: sum_network_field(network_data, :index_hits),
        index_misses: sum_network_field(network_data, :index_misses)
      }
    end
  end

  defp analyze_memory_data(memory_data) do
    if Enum.empty?(memory_data) do
      %{
        total_allocations: 0,
        peak_memory_usage: 0,
        avg_memory_usage: 0,
        gc_pressure: 0.0,
        fact_creation_count: 0,
        fact_retraction_count: 0
      }
    else
      memory_values = Enum.map(memory_data, &Map.get(&1, :memory_usage, 0))

      %{
        total_allocations: sum_memory_field(memory_data, :allocations),
        peak_memory_usage: Enum.max(memory_values),
        avg_memory_usage: Enum.sum(memory_values) / length(memory_values),
        gc_pressure: calculate_gc_pressure(memory_data),
        fact_creation_count: sum_memory_field(memory_data, :fact_creations),
        fact_retraction_count: sum_memory_field(memory_data, :fact_retractions)
      }
    end
  end

  defp analyze_interaction_data(interaction_data) do
    if Enum.empty?(interaction_data) do
      %{
        conflicts_generated: 0,
        conflicts_resolved: 0,
        rule_dependencies: [],
        fact_sharing_patterns: %{},
        cascading_executions: 0,
        priority_impacts: %{}
      }
    else
      %{
        conflicts_generated: sum_interaction_field(interaction_data, :conflicts_generated),
        conflicts_resolved: sum_interaction_field(interaction_data, :conflicts_resolved),
        rule_dependencies: extract_rule_dependencies(interaction_data),
        fact_sharing_patterns: analyze_fact_sharing(interaction_data),
        cascading_executions: sum_interaction_field(interaction_data, :cascading_executions),
        priority_impacts: analyze_priority_impacts(interaction_data)
      }
    end
  end

  defp calculate_optimization_score(execution_data, network_data) do
    # Simple optimization scoring algorithm
    base_score = 100.0

    # Execution performance factor
    base_score =
      if length(execution_data) > 0 do
        avg_time =
          Enum.sum(Enum.map(execution_data, &Map.get(&1, :execution_time, 0))) /
            length(execution_data)

        time_penalty = min(avg_time / 10, 30)
        base_score - time_penalty
      else
        base_score
      end

    # Network efficiency factor
    base_score =
      if length(network_data) > 0 do
        join_efficiency = calculate_join_efficiency(network_data)
        efficiency_bonus = join_efficiency * 10
        base_score + efficiency_bonus
      else
        base_score
      end

    max(base_score, 0.0)
  end

  defp perform_performance_analysis(engine_name, state) do
    cache_key = {engine_name, :performance_analysis}

    case check_analysis_cache(cache_key, state) do
      {:hit, analysis} ->
        updated_stats = %{state.stats | cache_hits: state.stats.cache_hits + 1}
        {:ok, analysis, %{state | stats: updated_stats}}

      :miss ->
        {:ok, analysis} = conduct_performance_analysis(engine_name, state)
        # Cache the analysis
        new_cache = Map.put(state.analysis_cache, cache_key, {analysis, DateTime.utc_now()})
        updated_stats = %{state.stats | cache_misses: state.stats.cache_misses + 1}

        new_state = %{state | analysis_cache: new_cache, stats: updated_stats}

        {:ok, analysis, new_state}
    end
  end

  defp conduct_performance_analysis(engine_name, state) do
    {:ok, profiles} = generate_engine_profiles(engine_name, state)

    analysis = %{
      engine_name: engine_name,
      total_rules_analyzed: length(profiles),
      overall_optimization_score: calculate_overall_optimization_score(profiles),
      performance_summary: generate_performance_summary(profiles),
      bottleneck_analysis: identify_performance_bottlenecks(profiles),
      optimization_recommendations: generate_rule_optimization_recommendations(profiles),
      rule_rankings: rank_rules_by_performance(profiles),
      generated_at: DateTime.utc_now()
    }

    {:ok, analysis}
  end

  defp check_analysis_cache(cache_key, state) do
    case Map.get(state.analysis_cache, cache_key) do
      nil ->
        :miss

      {analysis, timestamp} ->
        # Cache valid for 10 minutes
        if DateTime.diff(DateTime.utc_now(), timestamp, :second) < 600 do
          {:hit, analysis}
        else
          :miss
        end
    end
  end

  # Helper functions for data analysis

  defp calculate_avg_condition_time(execution_data) do
    condition_times = Enum.map(execution_data, &Map.get(&1, :condition_time, 0))

    if length(condition_times) > 0 do
      Enum.sum(condition_times) / length(condition_times)
    else
      0.0
    end
  end

  defp calculate_avg_action_time(execution_data) do
    action_times = Enum.map(execution_data, &Map.get(&1, :action_time, 0))

    if length(action_times) > 0 do
      Enum.sum(action_times) / length(action_times)
    else
      0.0
    end
  end

  defp identify_failure_patterns(execution_data) do
    failed_executions =
      Enum.filter(execution_data, fn data ->
        not Map.get(data, :success, false)
      end)

    # Group by error type/pattern
    failed_executions
    |> Enum.group_by(fn data -> Map.get(data, :error_type, "unknown") end)
    |> Enum.map(fn {error_type, errors} ->
      "#{error_type}: #{length(errors)} occurrences"
    end)
  end

  defp sum_network_field(network_data, field) do
    network_data
    |> Enum.map(&Map.get(&1, field, 0))
    |> Enum.sum()
  end

  defp avg_network_field(network_data, field) do
    values = Enum.map(network_data, &Map.get(&1, field, 0))

    if length(values) > 0 do
      Enum.sum(values) / length(values)
    else
      0.0
    end
  end

  defp calculate_join_efficiency(network_data) do
    total_joins = sum_network_field(network_data, :join_attempts)
    successful_joins = sum_network_field(network_data, :successful_joins)

    if total_joins > 0 do
      successful_joins / total_joins
    else
      0.0
    end
  end

  defp sum_memory_field(memory_data, field) do
    memory_data
    |> Enum.map(&Map.get(&1, field, 0))
    |> Enum.sum()
  end

  defp calculate_gc_pressure(memory_data) do
    gc_events = sum_memory_field(memory_data, :gc_events)
    total_time = length(memory_data)

    if total_time > 0 do
      gc_events / total_time
    else
      0.0
    end
  end

  defp sum_interaction_field(interaction_data, field) do
    interaction_data
    |> Enum.map(&Map.get(&1, field, 0))
    |> Enum.sum()
  end

  defp extract_rule_dependencies(interaction_data) do
    interaction_data
    |> Enum.flat_map(&Map.get(&1, :dependencies, []))
    |> Enum.uniq()
  end

  defp analyze_fact_sharing(_interaction_data) do
    # Analyze patterns of fact sharing between rules
    %{
      # Placeholder
      shared_fact_types: ["customer", "order", "product"],
      sharing_frequency: 0.7
    }
  end

  defp analyze_priority_impacts(_interaction_data) do
    # Analyze how rule priorities affect execution
    %{
      priority_conflicts: 0,
      priority_effectiveness: 0.8
    }
  end

  defp calculate_overall_optimization_score(profiles) do
    if length(profiles) > 0 do
      scores = Enum.map(profiles, & &1.optimization_score)
      Enum.sum(scores) / length(scores)
    else
      0.0
    end
  end

  defp generate_performance_summary(profiles) do
    %{
      total_rules: length(profiles),
      avg_execution_time: calculate_avg_execution_time_across_rules(profiles),
      high_performing_rules: count_high_performing_rules(profiles),
      problematic_rules: count_problematic_rules(profiles),
      memory_intensive_rules: count_memory_intensive_rules(profiles)
    }
  end

  defp identify_performance_bottlenecks(profiles) do
    bottlenecks = []

    # Identify slow rules
    slow_rules =
      Enum.filter(profiles, fn profile ->
        profile.execution_stats.avg_execution_time > 100
      end)

    bottlenecks =
      if length(slow_rules) > 0 do
        [
          %{
            type: :slow_execution,
            description: "#{length(slow_rules)} rules have slow execution times",
            affected_rules: Enum.map(slow_rules, & &1.rule_id),
            severity: :high
          }
          | bottlenecks
        ]
      else
        bottlenecks
      end

    # Identify memory-intensive rules
    memory_intensive =
      Enum.filter(profiles, fn profile ->
        # 10MB
        profile.memory_stats.peak_memory_usage > 10_000_000
      end)

    bottlenecks =
      if length(memory_intensive) > 0 do
        [
          %{
            type: :high_memory_usage,
            description: "#{length(memory_intensive)} rules use excessive memory",
            affected_rules: Enum.map(memory_intensive, & &1.rule_id),
            severity: :medium
          }
          | bottlenecks
        ]
      else
        bottlenecks
      end

    bottlenecks
  end

  defp generate_rule_optimization_recommendations(profiles) do
    recommendations = []

    # Recommendations for slow rules
    slow_rules =
      Enum.filter(profiles, fn profile ->
        profile.execution_stats.avg_execution_time > 100
      end)

    recommendations =
      if length(slow_rules) > 0 do
        [
          %{
            type: :execution_optimization,
            priority: :high,
            description: "Optimize slow-executing rules",
            rules: Enum.map(slow_rules, & &1.rule_id),
            suggestions: [
              "Review rule conditions for efficiency",
              "Consider adding more specific patterns",
              "Optimize fact indexing strategies"
            ]
          }
          | recommendations
        ]
      else
        recommendations
      end

    # Memory optimization recommendations
    memory_intensive =
      Enum.filter(profiles, fn profile ->
        profile.memory_stats.peak_memory_usage > 5_000_000
      end)

    recommendations =
      if length(memory_intensive) > 0 do
        [
          %{
            type: :memory_optimization,
            priority: :medium,
            description: "Optimize memory usage in rules",
            rules: Enum.map(memory_intensive, & &1.rule_id),
            suggestions: [
              "Review fact creation patterns",
              "Implement fact cleanup in actions",
              "Consider fact pooling strategies"
            ]
          }
          | recommendations
        ]
      else
        recommendations
      end

    recommendations
  end

  defp rank_rules_by_performance(profiles) do
    profiles
    |> Enum.sort_by(& &1.optimization_score, :desc)
    |> Enum.with_index(1)
    |> Enum.map(fn {profile, rank} ->
      %{
        rank: rank,
        rule_id: profile.rule_id,
        optimization_score: profile.optimization_score,
        avg_execution_time: profile.execution_stats.avg_execution_time,
        total_executions: profile.execution_stats.total_executions
      }
    end)
  end

  defp determine_profiling_period(execution_data) do
    if length(execution_data) > 0 do
      timestamps = Enum.map(execution_data, &Map.get(&1, :timestamp))
      start_time = Enum.min(timestamps)
      end_time = Enum.max(timestamps)
      {start_time, end_time}
    else
      now = DateTime.utc_now()
      {now, now}
    end
  end

  defp cleanup_old_profiles(state) do
    Logger.debug("Cleaning up old profile data")

    cutoff_time = DateTime.add(DateTime.utc_now(), -state.profile_retention_time, :second)

    # Clean up execution data
    new_execution_data =
      Map.filter(state.execution_data, fn {_key, data} ->
        case data do
          [] ->
            false

          [latest | _] ->
            DateTime.compare(Map.get(latest, :timestamp, DateTime.utc_now()), cutoff_time) == :gt
        end
      end)

    # Clean up other data similarly
    new_network_data = clean_data_by_timestamp(state.network_data, cutoff_time)
    new_memory_data = clean_data_by_timestamp(state.memory_data, cutoff_time)
    new_interaction_data = clean_data_by_timestamp(state.interaction_data, cutoff_time)

    # Clean up analysis cache
    new_analysis_cache =
      Map.filter(state.analysis_cache, fn {_key, {_analysis, timestamp}} ->
        DateTime.compare(timestamp, cutoff_time) == :gt
      end)

    %{
      state
      | execution_data: new_execution_data,
        network_data: new_network_data,
        memory_data: new_memory_data,
        interaction_data: new_interaction_data,
        analysis_cache: new_analysis_cache
    }
  end

  defp clean_data_by_timestamp(data_map, cutoff_time) do
    Map.filter(data_map, fn {_key, data} ->
      case data do
        [] ->
          false

        [latest | _] ->
          DateTime.compare(Map.get(latest, :timestamp, DateTime.utc_now()), cutoff_time) == :gt
      end
    end)
  end

  defp get_engine_rules(engine_name) do
    try do
      rules = Presto.RuleEngine.get_rules(engine_name)
      rule_ids = Map.keys(rules)
      {:ok, rule_ids}
    rescue
      _ ->
        # Fallback for non-existent engines
        {:ok, []}
    end
  end

  defp calculate_total_execution_records(state) do
    state.execution_data
    |> Map.values()
    |> Enum.map(&length/1)
    |> Enum.sum()
  end

  defp calculate_profiler_memory_usage(state) do
    # Rough estimate of memory usage in MB
    total_size =
      :erlang.external_size(state.execution_data) +
        :erlang.external_size(state.network_data) +
        :erlang.external_size(state.memory_data) +
        :erlang.external_size(state.interaction_data) +
        :erlang.external_size(state.analysis_cache)

    total_size / (1024 * 1024)
  end

  # Helper functions for performance analysis

  defp calculate_avg_execution_time_across_rules(profiles) do
    if length(profiles) > 0 do
      times = Enum.map(profiles, & &1.execution_stats.avg_execution_time)
      Enum.sum(times) / length(times)
    else
      0.0
    end
  end

  defp count_high_performing_rules(profiles) do
    Enum.count(profiles, &(&1.optimization_score > 80))
  end

  defp count_problematic_rules(profiles) do
    Enum.count(profiles, &(&1.optimization_score < 50))
  end

  defp count_memory_intensive_rules(profiles) do
    Enum.count(profiles, &(&1.memory_stats.peak_memory_usage > 5_000_000))
  end
end
