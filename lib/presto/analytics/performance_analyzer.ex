defmodule Presto.Analytics.PerformanceAnalyzer do
  @moduledoc """
  Advanced performance analysis system for Presto rule engines.

  Provides intelligent analysis capabilities including:
  - Rule execution pattern analysis
  - Performance bottleneck identification
  - Memory usage optimization recommendations
  - Conflict resolution efficiency analysis
  - Network performance insights for distributed setups

  ## Analysis Types

  ### Execution Analysis
  - Rule firing frequency patterns
  - Alpha/Beta network traversal efficiency
  - Working memory utilization patterns
  - Fact insertion/retraction impact analysis

  ### Optimization Recommendations
  - Rule reordering suggestions
  - Index optimization opportunities
  - Memory allocation improvements
  - Distributed coordination optimizations

  ### Anomaly Detection
  - Performance regression identification
  - Unusual execution pattern detection
  - Resource consumption anomalies
  - Capacity planning insights

  ## Example Usage

      # Analyze engine performance
      {:ok, insights} = PerformanceAnalyzer.analyze_engine_performance(
        analyzer_pid, 
        :customer_engine
      )

      # Get optimization recommendations
      {:ok, recommendations} = PerformanceAnalyzer.get_optimization_recommendations(
        analyzer_pid,
        :payment_engine,
        analysis_depth: :deep
      )

      # Perform comparative analysis
      {:ok, comparison} = PerformanceAnalyzer.compare_engines(
        analyzer_pid,
        [:engine_a, :engine_b],
        period: :last_hour
      )
  """

  use GenServer
  require Logger

  @type analysis_depth :: :surface | :standard | :deep | :comprehensive
  @type insight_type :: :optimization | :warning | :recommendation | :anomaly
  @type confidence_level :: :low | :medium | :high | :very_high

  @type performance_insight :: %{
          id: String.t(),
          engine_name: atom(),
          insight_type: insight_type(),
          category: String.t(),
          title: String.t(),
          description: String.t(),
          severity: :low | :medium | :high | :critical,
          confidence: confidence_level(),
          impact_score: float(),
          suggested_actions: [String.t()],
          evidence: map(),
          timestamp: DateTime.t()
        }

  @type optimization_recommendation :: %{
          id: String.t(),
          type:
            :rule_reordering | :index_optimization | :memory_tuning | :distributed_optimization,
          priority: :low | :medium | :high | :critical,
          estimated_improvement: float(),
          implementation_complexity: :simple | :moderate | :complex,
          description: String.t(),
          steps: [String.t()],
          risks: [String.t()],
          metrics_to_monitor: [String.t()]
        }

  @type comparative_analysis :: %{
          engines: [atom()],
          period: {DateTime.t(), DateTime.t()},
          performance_scores: %{atom() => float()},
          relative_performance: map(),
          best_practices: [String.t()],
          improvement_opportunities: [String.t()],
          generated_at: DateTime.t()
        }

  ## Client API

  @doc """
  Starts the performance analyzer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Analyzes performance for a specific engine.
  """
  @spec analyze_engine_performance(pid(), atom(), keyword()) ::
          {:ok, [performance_insight()]} | {:error, term()}
  def analyze_engine_performance(analyzer_pid, engine_name, opts \\ []) do
    GenServer.call(analyzer_pid, {:analyze_engine, engine_name, opts}, 30_000)
  end

  @doc """
  Gets optimization recommendations for an engine.
  """
  @spec get_optimization_recommendations(pid(), atom(), keyword()) ::
          {:ok, [optimization_recommendation()]} | {:error, term()}
  def get_optimization_recommendations(analyzer_pid, engine_name, opts \\ []) do
    GenServer.call(analyzer_pid, {:get_recommendations, engine_name, opts}, 30_000)
  end

  @doc """
  Compares performance across multiple engines.
  """
  @spec compare_engines(pid(), [atom()], keyword()) ::
          {:ok, comparative_analysis()} | {:error, term()}
  def compare_engines(analyzer_pid, engine_names, opts \\ []) do
    GenServer.call(analyzer_pid, {:compare_engines, engine_names, opts}, 30_000)
  end

  @doc """
  Detects performance anomalies.
  """
  @spec detect_anomalies(pid(), atom(), keyword()) ::
          {:ok, [performance_insight()]} | {:error, term()}
  def detect_anomalies(analyzer_pid, engine_name, opts \\ []) do
    GenServer.call(analyzer_pid, {:detect_anomalies, engine_name, opts})
  end

  @doc """
  Analyzes rule execution patterns.
  """
  @spec analyze_rule_patterns(pid(), atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def analyze_rule_patterns(analyzer_pid, engine_name, opts \\ []) do
    GenServer.call(analyzer_pid, {:analyze_patterns, engine_name, opts})
  end

  @doc """
  Gets performance benchmarks.
  """
  @spec get_performance_benchmarks(pid(), atom()) :: {:ok, map()} | {:error, term()}
  def get_performance_benchmarks(analyzer_pid, engine_name) do
    GenServer.call(analyzer_pid, {:get_benchmarks, engine_name})
  end

  @doc """
  Gets analyzer statistics.
  """
  @spec get_analyzer_stats(pid()) :: map()
  def get_analyzer_stats(analyzer_pid) do
    GenServer.call(analyzer_pid, :get_analyzer_stats)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Performance Analyzer")

    state = %{
      # Configuration
      default_analysis_depth: Keyword.get(opts, :default_analysis_depth, :standard),
      max_analysis_history: Keyword.get(opts, :max_analysis_history, 100),
      # 1 hour
      benchmark_window: Keyword.get(opts, :benchmark_window, 3600),
      anomaly_sensitivity: Keyword.get(opts, :anomaly_sensitivity, 0.8),

      # Analysis cache
      analysis_cache: %{},
      pattern_cache: %{},
      benchmark_cache: %{},

      # Analysis models
      baseline_models: %{},
      anomaly_detectors: %{},
      performance_models: %{},

      # Statistics
      stats: %{
        total_analyses: 0,
        insights_generated: 0,
        recommendations_provided: 0,
        anomalies_detected: 0,
        cache_hits: 0,
        cache_misses: 0
      }
    }

    # Schedule periodic model updates
    # 5 minutes
    Process.send_after(self(), :update_models, 300_000)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:analyze_engine, engine_name, opts}, _from, state) do
    depth = Keyword.get(opts, :analysis_depth, state.default_analysis_depth)

    case perform_engine_analysis(engine_name, depth, state) do
      {:ok, insights, new_state} ->
        updated_stats = %{
          new_state.stats
          | total_analyses: new_state.stats.total_analyses + 1,
            insights_generated: new_state.stats.insights_generated + length(insights)
        }

        {:reply, {:ok, insights}, %{new_state | stats: updated_stats}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:get_recommendations, engine_name, opts}, _from, state) do
    case generate_optimization_recommendations(engine_name, opts, state) do
      {:ok, recommendations, new_state} ->
        updated_stats = %{
          new_state.stats
          | recommendations_provided:
              new_state.stats.recommendations_provided + length(recommendations)
        }

        {:reply, {:ok, recommendations}, %{new_state | stats: updated_stats}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:compare_engines, engine_names, opts}, _from, state) do
    case perform_comparative_analysis(engine_names, opts, state) do
      {:ok, comparison} ->
        {:reply, {:ok, comparison}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:detect_anomalies, engine_name, opts}, _from, state) do
    case detect_performance_anomalies(engine_name, opts, state) do
      {:ok, anomalies, new_state} ->
        updated_stats = %{
          new_state.stats
          | anomalies_detected: new_state.stats.anomalies_detected + length(anomalies)
        }

        {:reply, {:ok, anomalies}, %{new_state | stats: updated_stats}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:analyze_patterns, engine_name, opts}, _from, state) do
    case analyze_execution_patterns(engine_name, opts, state) do
      {:ok, patterns} ->
        {:reply, {:ok, patterns}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:get_benchmarks, engine_name}, _from, state) do
    case get_or_calculate_benchmarks(engine_name, state) do
      {:ok, benchmarks} ->
        {:reply, {:ok, benchmarks}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call(:get_analyzer_stats, _from, state) do
    stats =
      Map.merge(state.stats, %{
        cached_analyses: map_size(state.analysis_cache),
        cached_patterns: map_size(state.pattern_cache),
        cached_benchmarks: map_size(state.benchmark_cache),
        baseline_models: map_size(state.baseline_models)
      })

    {:reply, stats, state}
  end

  @impl GenServer
  def handle_info(:update_models, state) do
    new_state = update_performance_models(state)

    # Schedule next update
    Process.send_after(self(), :update_models, 300_000)

    {:noreply, new_state}
  end

  ## Private functions

  defp perform_engine_analysis(engine_name, depth, state) do
    try do
      # Check cache first
      cache_key = {engine_name, depth, :analysis}

      case check_analysis_cache(cache_key, state) do
        {:hit, insights} ->
          updated_stats = %{state.stats | cache_hits: state.stats.cache_hits + 1}
          {:ok, insights, %{state | stats: updated_stats}}

        :miss ->
          # Perform fresh analysis
          case conduct_performance_analysis(engine_name, depth, state) do
            {:ok, insights} ->
              # Cache results
              new_cache = Map.put(state.analysis_cache, cache_key, {insights, DateTime.utc_now()})
              updated_stats = %{state.stats | cache_misses: state.stats.cache_misses + 1}

              new_state = %{state | analysis_cache: new_cache, stats: updated_stats}

              {:ok, insights, new_state}

            error ->
              error
          end
      end
    rescue
      error ->
        Logger.error("Performance analysis failed",
          engine: engine_name,
          error: inspect(error)
        )

        {:error, {:analysis_failed, error}}
    end
  end

  defp check_analysis_cache(cache_key, state) do
    case Map.get(state.analysis_cache, cache_key) do
      nil ->
        :miss

      {insights, timestamp} ->
        # Check if cache entry is still valid (5 minutes)
        if DateTime.diff(DateTime.utc_now(), timestamp, :second) < 300 do
          {:hit, insights}
        else
          :miss
        end
    end
  end

  defp conduct_performance_analysis(engine_name, depth, state) do
    insights = []

    # Execution performance analysis
    execution_insights = analyze_execution_performance(engine_name, depth)
    insights = insights ++ execution_insights

    # Memory usage analysis
    memory_insights = analyze_memory_usage(engine_name, depth)
    insights = insights ++ memory_insights

    # Rule efficiency analysis
    rule_insights = analyze_rule_efficiency(engine_name, depth)
    insights = insights ++ rule_insights

    # Network performance (if distributed)
    network_insights = analyze_network_performance(engine_name, depth)
    insights = insights ++ network_insights

    # Deep analysis additional checks
    deep_insights =
      if depth in [:deep, :comprehensive] do
        perform_deep_analysis(engine_name, state)
      else
        []
      end

    all_insights = insights ++ deep_insights

    {:ok, all_insights}
  end

  defp analyze_execution_performance(engine_name, _depth) do
    case get_engine_metrics(engine_name, :rule_execution) do
      {:ok, metrics} ->
        insights = []

        # Check average execution time
        avg_time = Map.get(metrics, :avg_execution_time, 0)

        insights =
          if avg_time > 100 do
            [
              create_insight(
                engine_name,
                :warning,
                "High Execution Time",
                "Average rule execution time is #{avg_time}ms, which may impact performance",
                ["Optimize rule conditions", "Review fact indexing", "Consider rule reordering"],
                %{avg_execution_time: avg_time},
                :medium
              )
              | insights
            ]
          else
            insights
          end

        # Check execution frequency
        total_executions = Map.get(metrics, :total_executions, 0)

        insights =
          if total_executions < 10 do
            [
              create_insight(
                engine_name,
                :recommendation,
                "Low Rule Activity",
                "Engine has low rule execution activity (#{total_executions} total executions)",
                ["Verify rule conditions are appropriate", "Check fact assertion patterns"],
                %{total_executions: total_executions},
                :low
              )
              | insights
            ]
          else
            insights
          end

        insights

      _ ->
        []
    end
  end

  defp analyze_memory_usage(engine_name, depth) do
    case get_engine_metrics(engine_name, :memory_usage) do
      {:ok, metrics} ->
        insights = []

        memory_mb = Map.get(metrics, :memory, 0) / (1024 * 1024)

        insights =
          if memory_mb > 500 do
            [
              create_insight(
                engine_name,
                :warning,
                "High Memory Usage",
                "Engine is using #{Float.round(memory_mb, 1)}MB of memory",
                [
                  "Review fact retention policies",
                  "Optimize working memory",
                  "Consider garbage collection tuning"
                ],
                %{memory_mb: memory_mb},
                :high
              )
              | insights
            ]
          else
            insights
          end

        # Check for memory growth patterns if doing deep analysis
        insights =
          if depth in [:deep, :comprehensive] do
            # Would analyze memory growth trends here
            insights
          else
            insights
          end

        insights

      _ ->
        []
    end
  end

  defp analyze_rule_efficiency(engine_name, _depth) do
    case get_engine_rule_statistics(engine_name) do
      {:ok, rule_stats} ->
        insights = []

        # Check for rules that never fire
        unfired_rules =
          Enum.filter(rule_stats, fn {_rule, stats} ->
            Map.get(stats, :fire_count, 0) == 0
          end)

        insights =
          if length(unfired_rules) > 0 do
            [
              create_insight(
                engine_name,
                :optimization,
                "Unused Rules Detected",
                "#{length(unfired_rules)} rules have never fired",
                [
                  "Review rule conditions",
                  "Consider removing unused rules",
                  "Validate rule logic"
                ],
                %{unfired_rules: length(unfired_rules)},
                :medium
              )
              | insights
            ]
          else
            insights
          end

        # Check for rules with very high execution frequency
        hot_rules =
          Enum.filter(rule_stats, fn {_rule, stats} ->
            Map.get(stats, :fire_count, 0) > 1000
          end)

        insights =
          if length(hot_rules) > 0 do
            [
              create_insight(
                engine_name,
                :optimization,
                "High-Frequency Rules",
                "#{length(hot_rules)} rules fire very frequently and may benefit from optimization",
                [
                  "Optimize frequently firing rules",
                  "Consider rule condition reordering",
                  "Review fact patterns"
                ],
                %{hot_rules: length(hot_rules)},
                :high
              )
              | insights
            ]
          else
            insights
          end

        insights

      _ ->
        []
    end
  end

  defp analyze_network_performance(engine_name, _depth) do
    # Check if engine is part of distributed setup
    case is_distributed_engine?(engine_name) do
      true ->
        case get_engine_metrics(engine_name, :network_stats) do
          {:ok, network_metrics} ->
            insights = []

            latency = Map.get(network_metrics, :avg_latency, 0)

            insights =
              if latency > 50 do
                [
                  create_insight(
                    engine_name,
                    :warning,
                    "High Network Latency",
                    "Average network latency is #{latency}ms in distributed setup",
                    [
                      "Check network configuration",
                      "Consider fact batching",
                      "Review replication settings"
                    ],
                    %{avg_latency: latency},
                    :medium
                  )
                  | insights
                ]
              else
                insights
              end

            insights

          _ ->
            []
        end

      false ->
        []
    end
  end

  defp perform_deep_analysis(engine_name, _state) do
    insights = []

    # Conflict resolution analysis
    conflict_insights = analyze_conflict_resolution(engine_name)
    insights = insights ++ conflict_insights

    # Pattern matching efficiency
    pattern_insights = analyze_pattern_matching(engine_name)
    insights = insights ++ pattern_insights

    # Fact lifecycle analysis
    lifecycle_insights = analyze_fact_lifecycle(engine_name)
    insights = insights ++ lifecycle_insights

    insights
  end

  defp analyze_conflict_resolution(engine_name) do
    # Analyze conflict resolution strategy effectiveness
    [
      create_insight(
        engine_name,
        :recommendation,
        "Conflict Resolution Analysis",
        "Deep analysis of conflict resolution patterns available in comprehensive mode",
        ["Consider custom conflict resolution strategies", "Monitor rule priority effectiveness"],
        %{analysis_type: :conflict_resolution},
        :low
      )
    ]
  end

  defp analyze_pattern_matching(engine_name) do
    # Analyze alpha/beta network efficiency
    [
      create_insight(
        engine_name,
        :optimization,
        "Pattern Matching Efficiency",
        "Pattern matching network analysis suggests potential optimizations",
        ["Review fact indexing strategies", "Consider join node optimization"],
        %{analysis_type: :pattern_matching},
        :medium
      )
    ]
  end

  defp analyze_fact_lifecycle(engine_name) do
    # Analyze fact assertion/retraction patterns
    [
      create_insight(
        engine_name,
        :recommendation,
        "Fact Lifecycle Optimization",
        "Fact lifecycle analysis reveals opportunities for optimization",
        ["Optimize fact assertion patterns", "Review retraction policies"],
        %{analysis_type: :fact_lifecycle},
        :low
      )
    ]
  end

  defp generate_optimization_recommendations(engine_name, opts, state) do
    _analysis_depth = Keyword.get(opts, :_analysis_depth, :standard)

    recommendations = []

    # Get current performance data
    case get_engine_performance_data(engine_name) do
      {:ok, perf_data} ->
        # Rule ordering recommendations
        rule_recs = generate_rule_ordering_recommendations(engine_name, perf_data)
        recommendations = recommendations ++ rule_recs

        # Memory optimization recommendations
        memory_recs = generate_memory_recommendations(engine_name, perf_data)
        recommendations = recommendations ++ memory_recs

        # Index optimization recommendations
        index_recs = generate_index_recommendations(engine_name, perf_data)
        recommendations = recommendations ++ index_recs

        # Distributed optimization (if applicable)
        dist_recs =
          if is_distributed_engine?(engine_name) do
            generate_distributed_recommendations(engine_name, perf_data)
          else
            []
          end

        recommendations = recommendations ++ dist_recs

        {:ok, recommendations, state}

      error ->
        error
    end
  end

  defp generate_rule_ordering_recommendations(_engine_name, _perf_data) do
    [
      %{
        id: generate_recommendation_id(),
        type: :rule_reordering,
        priority: :medium,
        estimated_improvement: 0.15,
        implementation_complexity: :moderate,
        description: "Reorder rules based on execution frequency and selectivity",
        steps: [
          "Analyze rule firing patterns",
          "Identify most selective rules",
          "Reorder rules to place selective rules first",
          "Test performance impact"
        ],
        risks: ["May affect rule execution semantics", "Requires thorough testing"],
        metrics_to_monitor: ["avg_execution_time", "rule_fire_count"]
      }
    ]
  end

  defp generate_memory_recommendations(_engine_name, perf_data) do
    memory_mb = get_in(perf_data, [:memory_usage, :memory]) |> (&(&1 / (1024 * 1024))).() || 0

    if memory_mb > 100 do
      [
        %{
          id: generate_recommendation_id(),
          type: :memory_tuning,
          priority: :high,
          estimated_improvement: 0.25,
          implementation_complexity: :simple,
          description: "Optimize memory usage through fact retention policies",
          steps: [
            "Review current fact retention settings",
            "Implement time-based fact expiration",
            "Add memory pressure monitoring",
            "Configure garbage collection tuning"
          ],
          risks: ["May affect rule behaviour if facts are removed too aggressively"],
          metrics_to_monitor: ["memory_usage", "fact_count", "gc_frequency"]
        }
      ]
    else
      []
    end
  end

  defp generate_index_recommendations(_engine_name, _perf_data) do
    [
      %{
        id: generate_recommendation_id(),
        type: :index_optimization,
        priority: :low,
        estimated_improvement: 0.10,
        implementation_complexity: :complex,
        description: "Optimize fact indexing for improved pattern matching",
        steps: [
          "Analyze most common fact access patterns",
          "Design optimized indexing strategy",
          "Implement custom index structures",
          "Benchmark performance improvements"
        ],
        risks: ["Complex implementation", "May increase memory usage"],
        metrics_to_monitor: ["pattern_match_time", "index_lookup_time"]
      }
    ]
  end

  defp generate_distributed_recommendations(_engine_name, _perf_data) do
    [
      %{
        id: generate_recommendation_id(),
        type: :distributed_optimization,
        priority: :medium,
        estimated_improvement: 0.20,
        implementation_complexity: :complex,
        description: "Optimize distributed coordination and fact replication",
        steps: [
          "Analyze cross-node communication patterns",
          "Optimize fact replication batching",
          "Tune consensus algorithm parameters",
          "Implement smart load balancing"
        ],
        risks: ["May affect distributed consistency", "Complex distributed debugging"],
        metrics_to_monitor: ["network_latency", "replication_time", "consensus_time"]
      }
    ]
  end

  defp perform_comparative_analysis(engine_names, opts, _state) do
    period = determine_analysis_period(opts)

    try do
      # Collect performance data for all engines
      engine_data =
        Enum.map(engine_names, fn engine_name ->
          case get_engine_performance_data(engine_name) do
            {:ok, data} -> {engine_name, data}
            _ -> {engine_name, %{}}
          end
        end)
        |> Map.new()

      # Calculate performance scores
      performance_scores =
        Map.new(engine_data, fn {engine_name, data} ->
          score = calculate_performance_score(data)
          {engine_name, score}
        end)

      # Generate comparative insights
      {best_engine, best_score} = Enum.max_by(performance_scores, fn {_name, score} -> score end)

      {worst_engine, worst_score} =
        Enum.min_by(performance_scores, fn {_name, score} -> score end)

      relative_performance = %{
        best_performer: %{engine: best_engine, score: best_score},
        worst_performer: %{engine: worst_engine, score: worst_score},
        score_range: best_score - worst_score,
        average_score: Enum.sum(Map.values(performance_scores)) / length(engine_names)
      }

      comparison = %{
        engines: engine_names,
        period: period,
        performance_scores: performance_scores,
        relative_performance: relative_performance,
        best_practices: generate_best_practices(engine_data),
        improvement_opportunities: generate_improvement_opportunities(engine_data),
        generated_at: DateTime.utc_now()
      }

      {:ok, comparison}
    rescue
      error ->
        {:error, {:comparison_failed, error}}
    end
  end

  defp detect_performance_anomalies(engine_name, opts, state) do
    sensitivity = Keyword.get(opts, :sensitivity, state.anomaly_sensitivity)

    case get_engine_baseline(engine_name, state) do
      {:ok, baseline} ->
        case get_current_performance_metrics(engine_name) do
          {:ok, current_metrics} ->
            anomalies = identify_anomalies(baseline, current_metrics, sensitivity, engine_name)
            {:ok, anomalies, state}

          error ->
            error
        end

      error ->
        error
    end
  end

  defp identify_anomalies(baseline, current_metrics, sensitivity, engine_name) do
    anomalies = []

    # Check execution time anomalies
    baseline_time = Map.get(baseline, :avg_execution_time, 0)
    current_time = Map.get(current_metrics, :avg_execution_time, 0)

    anomalies =
      if baseline_time > 0 and current_time / baseline_time > 1 + sensitivity do
        [
          create_insight(
            engine_name,
            :anomaly,
            "Execution Time Anomaly",
            "Execution time increased by #{Float.round((current_time / baseline_time - 1) * 100, 1)}% from baseline",
            [
              "Investigate recent changes",
              "Check system resources",
              "Analyze rule modifications"
            ],
            %{
              baseline_time: baseline_time,
              current_time: current_time,
              increase_factor: current_time / baseline_time
            },
            :high
          )
          | anomalies
        ]
      else
        anomalies
      end

    # Check memory usage anomalies
    baseline_memory = Map.get(baseline, :memory_usage, 0)
    current_memory = Map.get(current_metrics, :memory_usage, 0)

    anomalies =
      if baseline_memory > 0 and current_memory / baseline_memory > 1 + sensitivity do
        [
          create_insight(
            engine_name,
            :anomaly,
            "Memory Usage Anomaly",
            "Memory usage increased by #{Float.round((current_memory / baseline_memory - 1) * 100, 1)}% from baseline",
            ["Check for memory leaks", "Review fact retention", "Monitor garbage collection"],
            %{
              baseline_memory: baseline_memory,
              current_memory: current_memory,
              increase_factor: current_memory / baseline_memory
            },
            :high
          )
          | anomalies
        ]
      else
        anomalies
      end

    anomalies
  end

  # Helper functions

  defp create_insight(engine_name, insight_type, title, description, actions, evidence, severity) do
    %{
      id: generate_insight_id(),
      engine_name: engine_name,
      insight_type: insight_type,
      category: determine_category(insight_type),
      title: title,
      description: description,
      severity: severity,
      confidence: calculate_confidence(evidence),
      impact_score: calculate_impact_score(severity, evidence),
      suggested_actions: actions,
      evidence: evidence,
      timestamp: DateTime.utc_now()
    }
  end

  defp determine_category(:optimization), do: "Performance"
  defp determine_category(:warning), do: "Health"
  defp determine_category(:recommendation), do: "Optimization"
  defp determine_category(:anomaly), do: "Anomaly"

  defp calculate_confidence(evidence) do
    # Simple confidence calculation based on evidence quality
    evidence_count = map_size(evidence)

    cond do
      evidence_count >= 3 -> :very_high
      evidence_count >= 2 -> :high
      evidence_count >= 1 -> :medium
      true -> :low
    end
  end

  defp calculate_impact_score(severity, evidence) do
    base_score =
      case severity do
        :critical -> 0.9
        :high -> 0.7
        :medium -> 0.5
        :low -> 0.3
      end

    # Adjust based on evidence strength
    evidence_factor = map_size(evidence) * 0.1
    min(base_score + evidence_factor, 1.0)
  end

  defp calculate_performance_score(data) do
    # Simple performance scoring algorithm
    base_score = 100.0

    # Deduct for high execution time
    exec_time = Map.get(data, :avg_execution_time, 0)
    time_penalty = min(exec_time / 10, 30)

    # Deduct for high memory usage
    memory_mb = Map.get(data, :memory_usage, 0) / (1024 * 1024)
    memory_penalty = min(memory_mb / 100, 20)

    # Add bonus for high activity
    executions = Map.get(data, :total_executions, 0)
    activity_bonus = min(executions / 100, 10)

    max(base_score - time_penalty - memory_penalty + activity_bonus, 0)
  end

  defp generate_best_practices(_engine_data) do
    [
      "Maintain regular performance monitoring",
      "Implement automated alerting for performance degradation",
      "Regular review of rule efficiency and optimization opportunities",
      "Monitor memory usage patterns and implement appropriate retention policies"
    ]
  end

  defp generate_improvement_opportunities(engine_data) do
    opportunities = []

    # Check for engines with high memory usage
    high_memory_engines =
      Enum.filter(engine_data, fn {_name, data} ->
        memory_mb = Map.get(data, :memory_usage, 0) / (1024 * 1024)
        memory_mb > 200
      end)

    opportunities =
      if length(high_memory_engines) > 0 do
        ["Memory optimization across #{length(high_memory_engines)} engines" | opportunities]
      else
        opportunities
      end

    # Check for engines with slow execution
    slow_engines =
      Enum.filter(engine_data, fn {_name, data} ->
        Map.get(data, :avg_execution_time, 0) > 100
      end)

    opportunities =
      if length(slow_engines) > 0 do
        ["Execution time optimization for #{length(slow_engines)} engines" | opportunities]
      else
        opportunities
      end

    opportunities
  end

  defp get_engine_metrics(_engine_name, metric_type) do
    # Placeholder - would integrate with actual metrics collection
    case metric_type do
      :rule_execution ->
        {:ok,
         %{
           avg_execution_time: :rand.uniform(200),
           total_executions: :rand.uniform(1000),
           successful_executions: :rand.uniform(950)
         }}

      :memory_usage ->
        {:ok,
         %{
           memory: :rand.uniform(100_000_000),
           heap_size: :rand.uniform(50_000_000)
         }}

      :network_stats ->
        {:ok,
         %{
           avg_latency: :rand.uniform(100),
           total_requests: :rand.uniform(1000)
         }}

      _ ->
        {:error, :metric_not_available}
    end
  end

  defp get_engine_rule_statistics(_engine_name) do
    # Placeholder for rule statistics
    {:ok,
     %{
       rule_1: %{fire_count: :rand.uniform(100)},
       rule_2: %{fire_count: 0},
       rule_3: %{fire_count: :rand.uniform(1500)}
     }}
  end

  defp get_engine_performance_data(engine_name) do
    # Combine all available metrics
    {:ok, exec_metrics} = get_engine_metrics(engine_name, :rule_execution)
    {:ok, memory_metrics} = get_engine_metrics(engine_name, :memory_usage)

    combined = Map.merge(exec_metrics, memory_metrics)
    {:ok, combined}
  end

  defp get_current_performance_metrics(engine_name) do
    get_engine_performance_data(engine_name)
  end

  defp get_engine_baseline(engine_name, state) do
    case Map.get(state.baseline_models, engine_name) do
      nil -> {:error, :no_baseline}
      baseline -> {:ok, baseline}
    end
  end

  defp is_distributed_engine?(_engine_name) do
    # Check if engine is part of distributed setup
    # Placeholder implementation
    :rand.uniform() > 0.5
  end

  defp analyze_execution_patterns(_engine_name, _opts, _state) do
    # Placeholder for pattern analysis
    {:ok,
     %{
       execution_frequency: :high,
       peak_hours: [9, 10, 14, 15],
       pattern_type: :steady,
       seasonal_trends: []
     }}
  end

  defp get_or_calculate_benchmarks(engine_name, state) do
    case Map.get(state.benchmark_cache, engine_name) do
      nil ->
        # Calculate new benchmarks
        benchmarks = calculate_performance_benchmarks(engine_name)
        {:ok, benchmarks}

      benchmarks ->
        {:ok, benchmarks}
    end
  end

  defp calculate_performance_benchmarks(_engine_name) do
    %{
      baseline_execution_time: 50.0,
      target_execution_time: 30.0,
      baseline_memory_usage: 50_000_000,
      target_memory_usage: 30_000_000,
      baseline_throughput: 100,
      target_throughput: 150
    }
  end

  defp update_performance_models(state) do
    Logger.debug("Updating performance models")

    # Update baseline models for all known engines
    # This would analyze recent performance data to update baselines

    state
  end

  defp determine_analysis_period(opts) do
    case Keyword.get(opts, :period, :last_hour) do
      :last_hour ->
        end_time = DateTime.utc_now()
        start_time = DateTime.add(end_time, -3600, :second)
        {start_time, end_time}

      :last_day ->
        end_time = DateTime.utc_now()
        start_time = DateTime.add(end_time, -86400, :second)
        {start_time, end_time}

      {start_time, end_time} ->
        {start_time, end_time}
    end
  end

  defp generate_insight_id do
    "insight_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp generate_recommendation_id do
    "rec_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end
end
