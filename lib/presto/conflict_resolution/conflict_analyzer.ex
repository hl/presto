defmodule Presto.ConflictResolution.ConflictAnalyzer do
  @moduledoc """
  Advanced conflict analysis system for detailed rule interaction analysis.

  Provides comprehensive analysis capabilities including:
  - Deep rule dependency mapping
  - Conflict pattern recognition and classification
  - Performance impact modeling
  - Predictive conflict detection
  - Rule interaction visualization data

  ## Analysis Types

  ### Dependency Analysis
  - Direct rule dependencies
  - Transitive dependency chains
  - Circular dependency detection
  - Dependency strength measurement

  ### Pattern Analysis
  - Recurring conflict patterns
  - Temporal conflict trends
  - Rule firing sequence analysis
  - Resource contention patterns

  ### Impact Analysis
  - Performance impact modeling
  - Memory usage implications
  - Throughput effect estimation
  - System stability assessment

  ## Example Usage

      # Analyze rule dependencies
      {:ok, deps} = ConflictAnalyzer.analyze_rule_dependencies(:payment_engine)

      # Detect conflict patterns
      {:ok, patterns} = ConflictAnalyzer.detect_conflict_patterns(
        :customer_engine,
        time_window: 3600
      )

      # Model performance impact
      {:ok, impact} = ConflictAnalyzer.model_performance_impact(
        conflict_data,
        simulation_params: %{duration: 300, load: :high}
      )
  """

  require Logger

  @type dependency_type :: :direct | :transitive | :circular | :conditional
  @type pattern_type :: :recurring | :temporal | :cascading | :resource_contention

  @type rule_dependency :: %{
          from_rule: atom(),
          to_rule: atom(),
          dependency_type: dependency_type(),
          strength: float(),
          conditions: [String.t()],
          metadata: map()
        }

  @type conflict_pattern :: %{
          id: String.t(),
          pattern_type: pattern_type(),
          rules_involved: [atom()],
          frequency: float(),
          impact_score: float(),
          first_occurrence: DateTime.t(),
          last_occurrence: DateTime.t(),
          pattern_signature: String.t()
        }

  @type dependency_graph :: %{
          nodes: [atom()],
          edges: [rule_dependency()],
          strongly_connected_components: [[atom()]],
          topological_order: [atom()],
          metrics: map()
        }

  ## Client API

  @doc """
  Analyzes rule dependencies in an engine.
  """
  @spec analyze_rule_dependencies(atom(), keyword()) ::
          {:ok, dependency_graph()} | {:error, term()}
  def analyze_rule_dependencies(engine_name, opts \\ []) do
    case get_engine_rules(engine_name) do
      {:ok, rules} ->
        graph = build_comprehensive_dependency_graph(rules, opts)
        {:ok, graph}

      error ->
        error
    end
  end

  @doc """
  Detects recurring conflict patterns.
  """
  @spec detect_conflict_patterns(atom(), keyword()) ::
          {:ok, [conflict_pattern()]} | {:error, term()}
  def detect_conflict_patterns(engine_name, opts \\ []) do
    time_window = Keyword.get(opts, :time_window, 3600)
    pattern_threshold = Keyword.get(opts, :pattern_threshold, 0.1)

    case get_conflict_history(engine_name, time_window) do
      {:ok, conflicts} ->
        patterns = analyze_conflict_patterns(conflicts, pattern_threshold)
        {:ok, patterns}

      error ->
        error
    end
  end

  @doc """
  Models performance impact of conflicts.
  """
  @spec model_performance_impact(map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def model_performance_impact(conflict_data, opts \\ []) do
    simulation_params = Keyword.get(opts, :simulation_params, %{})

    impact_model = build_performance_impact_model(conflict_data, simulation_params)
    {:ok, impact_model}
  end

  @doc """
  Analyzes rule firing sequences for conflict prediction.
  """
  @spec analyze_firing_sequences(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def analyze_firing_sequences(engine_name, opts \\ []) do
    sequence_length = Keyword.get(opts, :sequence_length, 10)

    case get_firing_history(engine_name) do
      {:ok, firing_history} ->
        analysis = perform_sequence_analysis(firing_history, sequence_length)
        {:ok, analysis}

      error ->
        error
    end
  end

  @doc """
  Generates conflict prediction model.
  """
  @spec generate_prediction_model(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def generate_prediction_model(engine_name, opts \\ []) do
    # 24 hours
    training_window = Keyword.get(opts, :training_window, 86400)

    case gather_training_data(engine_name, training_window) do
      {:ok, training_data} ->
        model = build_prediction_model(training_data, opts)
        {:ok, model}

      error ->
        error
    end
  end

  @doc """
  Analyzes rule interaction strength.
  """
  @spec analyze_interaction_strength([atom()], keyword()) ::
          {:ok, map()} | {:error, term()}
  def analyze_interaction_strength(rules, opts \\ []) do
    case calculate_interaction_matrix(rules, opts) do
      {:ok, matrix} ->
        analysis = %{
          interaction_matrix: matrix,
          strongest_interactions: find_strongest_interactions(matrix),
          interaction_clusters: identify_interaction_clusters(matrix),
          interaction_metrics: calculate_interaction_metrics(matrix)
        }

        {:ok, analysis}

      error ->
        error
    end
  end

  @doc """
  Generates visualization data for conflict analysis.
  """
  @spec generate_visualization_data(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def generate_visualization_data(engine_name, opts \\ []) do
    viz_type = Keyword.get(opts, :type, :dependency_graph)

    case viz_type do
      :dependency_graph ->
        generate_dependency_graph_viz(engine_name, opts)

      :conflict_timeline ->
        generate_conflict_timeline_viz(engine_name, opts)

      :interaction_heatmap ->
        generate_interaction_heatmap_viz(engine_name, opts)

      :pattern_analysis ->
        generate_pattern_analysis_viz(engine_name, opts)

      _ ->
        {:error, {:unsupported_visualization_type, viz_type}}
    end
  end

  ## Private functions

  defp get_engine_rules(_engine_name) do
    # Placeholder - would integrate with actual engine
    {:ok,
     [
       %{id: :rule_1, conditions: ["customer.age > 18"], actions: ["add discount"], priority: 1},
       %{id: :rule_2, conditions: ["order.total > 100"], actions: ["free shipping"], priority: 2},
       %{
         id: :rule_3,
         conditions: ["customer.loyalty = gold"],
         actions: ["priority processing"],
         priority: 3
       }
     ]}
  end

  defp get_conflict_history(_engine_name, time_window) do
    # Placeholder for getting conflict history
    cutoff_time = DateTime.add(DateTime.utc_now(), -time_window, :second)

    conflicts = [
      %{
        id: "conflict_1",
        rules_involved: [:rule_1, :rule_2],
        timestamp: DateTime.utc_now(),
        type: :direct,
        severity: :medium
      },
      %{
        id: "conflict_2",
        rules_involved: [:rule_2, :rule_3],
        timestamp: DateTime.add(DateTime.utc_now(), -1800, :second),
        type: :indirect,
        severity: :low
      }
    ]

    recent_conflicts =
      Enum.filter(conflicts, fn conflict ->
        DateTime.compare(conflict.timestamp, cutoff_time) != :lt
      end)

    {:ok, recent_conflicts}
  end

  defp get_firing_history(_engine_name) do
    # Placeholder for getting rule firing history
    {:ok,
     [
       %{rule: :rule_1, timestamp: DateTime.utc_now(), duration: 10},
       %{rule: :rule_2, timestamp: DateTime.add(DateTime.utc_now(), -5, :second), duration: 15},
       %{rule: :rule_3, timestamp: DateTime.add(DateTime.utc_now(), -10, :second), duration: 8}
     ]}
  end

  defp build_comprehensive_dependency_graph(rules, _opts) do
    # Build dependency graph with various analysis techniques
    edges = build_dependency_edges(rules)
    nodes = Enum.map(rules, & &1.id)

    # Find strongly connected components (cycles)
    sccs = find_strongly_connected_components(nodes, edges)

    # Calculate topological order
    topo_order = calculate_topological_order(nodes, edges)

    # Calculate graph metrics
    metrics = calculate_graph_metrics(nodes, edges)

    %{
      nodes: nodes,
      edges: edges,
      strongly_connected_components: sccs,
      topological_order: topo_order,
      metrics: metrics
    }
  end

  defp build_dependency_edges(rules) do
    # Build edges representing dependencies between rules
    rule_pairs = for r1 <- rules, r2 <- rules, r1.id != r2.id, do: {r1, r2}

    Enum.flat_map(rule_pairs, fn {rule1, rule2} ->
      case analyze_rule_dependency(rule1, rule2) do
        {:dependency, type, strength, conditions} ->
          [
            %{
              from_rule: rule1.id,
              to_rule: rule2.id,
              dependency_type: type,
              strength: strength,
              conditions: conditions,
              metadata: %{analyzed_at: DateTime.utc_now()}
            }
          ]

        :no_dependency ->
          []
      end
    end)
  end

  defp analyze_rule_dependency(rule1, rule2) do
    # Analyze if rule1 depends on rule2
    # This is a simplified analysis - real implementation would be more sophisticated

    cond do
      has_fact_dependency?(rule1, rule2) ->
        {:dependency, :direct, 0.8, ["fact_modification"]}

      has_priority_dependency?(rule1, rule2) ->
        {:dependency, :conditional, 0.5, ["priority_ordering"]}

      has_resource_dependency?(rule1, rule2) ->
        {:dependency, :transitive, 0.3, ["resource_contention"]}

      true ->
        :no_dependency
    end
  end

  defp has_fact_dependency?(rule1, rule2) do
    # Check if rule1's conditions depend on facts modified by rule2
    rule1_conditions = extract_fact_references(rule1.conditions)
    rule2_modifications = extract_fact_modifications(rule2.actions)

    length(rule1_conditions -- (rule1_conditions -- rule2_modifications)) > 0
  end

  defp has_priority_dependency?(rule1, rule2) do
    # Check if rules have priority-based dependency
    rule1.priority > rule2.priority
  end

  defp has_resource_dependency?(rule1, rule2) do
    # Check if rules compete for same resources
    rule1_resources = extract_resource_usage(rule1)
    rule2_resources = extract_resource_usage(rule2)

    length(rule1_resources -- (rule1_resources -- rule2_resources)) > 0
  end

  defp extract_fact_references(conditions) do
    # Extract fact types referenced in conditions
    Enum.flat_map(conditions, fn condition ->
      # Simple pattern matching for fact references
      case Regex.scan(~r/(\w+)\./, condition) do
        [] -> []
        matches -> Enum.map(matches, fn [_, fact_type] -> fact_type end)
      end
    end)
    |> Enum.uniq()
  end

  defp extract_fact_modifications(actions) do
    # Extract fact types modified by actions
    Enum.flat_map(actions, fn action ->
      case Regex.scan(~r/modify (\w+)|add (\w+)|remove (\w+)/, action) do
        [] ->
          []

        matches ->
          Enum.flat_map(matches, fn match ->
            match |> Enum.drop(1) |> Enum.filter(&(&1 != ""))
          end)
      end
    end)
    |> Enum.uniq()
  end

  defp extract_resource_usage(_rule) do
    # Extract resources used by rule (memory, CPU, external services, etc.)
    # Placeholder implementation
    ["memory", "cpu"]
  end

  defp find_strongly_connected_components(nodes, edges) do
    # Find strongly connected components using Tarjan's algorithm
    # Simplified implementation
    adjacency_list = build_adjacency_list(nodes, edges)
    tarjan_scc(adjacency_list)
  end

  defp build_adjacency_list(nodes, edges) do
    base_list = Map.new(nodes, fn node -> {node, []} end)

    Enum.reduce(edges, base_list, fn edge, acc ->
      neighbors = Map.get(acc, edge.from_rule, [])
      Map.put(acc, edge.from_rule, [edge.to_rule | neighbors])
    end)
  end

  defp tarjan_scc(adjacency_list) do
    # Simplified Tarjan's strongly connected components algorithm
    # In a real implementation, this would be more sophisticated
    nodes = Map.keys(adjacency_list)

    # For now, just return each node as its own component
    Enum.map(nodes, fn node -> [node] end)
  end

  defp calculate_topological_order(nodes, edges) do
    # Calculate topological ordering of nodes
    # Simplified implementation using Kahn's algorithm
    in_degrees = calculate_in_degrees(nodes, edges)

    kahn_topological_sort(nodes, edges, in_degrees)
  end

  defp calculate_in_degrees(nodes, edges) do
    base_degrees = Map.new(nodes, fn node -> {node, 0} end)

    Enum.reduce(edges, base_degrees, fn edge, acc ->
      Map.update(acc, edge.to_rule, 1, &(&1 + 1))
    end)
  end

  defp kahn_topological_sort(nodes, edges, in_degrees) do
    # Simplified Kahn's algorithm
    zero_in_degree = Enum.filter(nodes, fn node -> Map.get(in_degrees, node) == 0 end)
    kahn_sort_helper(zero_in_degree, edges, in_degrees, [])
  end

  defp kahn_sort_helper([], _edges, _in_degrees, result) do
    Enum.reverse(result)
  end

  defp kahn_sort_helper([node | rest], edges, in_degrees, result) do
    # Add node to result
    new_result = [node | result]

    # Find edges from this node
    outgoing_edges = Enum.filter(edges, fn edge -> edge.from_rule == node end)

    # Update in-degrees for connected nodes
    new_in_degrees =
      Enum.reduce(outgoing_edges, in_degrees, fn edge, acc ->
        Map.update(acc, edge.to_rule, 0, &max(0, &1 - 1))
      end)

    # Find new zero in-degree nodes
    new_zero_nodes =
      Enum.filter(Map.keys(new_in_degrees), fn n ->
        Map.get(new_in_degrees, n) == 0 and n not in new_result and n not in rest
      end)

    kahn_sort_helper(rest ++ new_zero_nodes, edges, new_in_degrees, new_result)
  end

  defp calculate_graph_metrics(nodes, edges) do
    node_count = length(nodes)
    edge_count = length(edges)

    %{
      node_count: node_count,
      edge_count: edge_count,
      density: if(node_count > 1, do: edge_count / (node_count * (node_count - 1)), else: 0.0),
      avg_degree: if(node_count > 0, do: edge_count * 2 / node_count, else: 0.0),
      complexity_score: calculate_complexity_score(node_count, edge_count)
    }
  end

  defp calculate_complexity_score(node_count, edge_count) do
    # Calculate overall complexity score
    base_score = node_count * 0.1 + edge_count * 0.2
    # Cap at 10.0
    min(base_score, 10.0)
  end

  defp analyze_conflict_patterns(conflicts, pattern_threshold) do
    # Group conflicts by rule combinations
    rule_combinations = group_conflicts_by_rules(conflicts)

    # Find patterns that occur frequently enough
    Enum.flat_map(rule_combinations, fn {rules, rule_conflicts} ->
      frequency = length(rule_conflicts) / length(conflicts)

      if frequency >= pattern_threshold do
        [create_conflict_pattern(rules, rule_conflicts, frequency)]
      else
        []
      end
    end)
  end

  defp group_conflicts_by_rules(conflicts) do
    Enum.group_by(conflicts, fn conflict ->
      Enum.sort(conflict.rules_involved)
    end)
  end

  defp create_conflict_pattern(rules, conflicts, frequency) do
    first_occurrence = Enum.min_by(conflicts, & &1.timestamp).timestamp
    last_occurrence = Enum.max_by(conflicts, & &1.timestamp).timestamp
    impact_score = calculate_pattern_impact_score(conflicts)

    %{
      id: generate_pattern_id(rules),
      pattern_type: infer_pattern_type(conflicts),
      rules_involved: rules,
      frequency: frequency,
      impact_score: impact_score,
      first_occurrence: first_occurrence,
      last_occurrence: last_occurrence,
      pattern_signature: create_pattern_signature(rules, conflicts)
    }
  end

  defp infer_pattern_type(conflicts) do
    # Infer pattern type based on conflict characteristics
    time_intervals = calculate_time_intervals(conflicts)

    cond do
      is_regular_interval?(time_intervals) -> :temporal
      has_cascading_effect?(conflicts) -> :cascading
      involves_resource_contention?(conflicts) -> :resource_contention
      true -> :recurring
    end
  end

  defp calculate_time_intervals(conflicts) do
    conflicts
    |> Enum.sort_by(& &1.timestamp)
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [c1, c2] -> DateTime.diff(c2.timestamp, c1.timestamp, :second) end)
  end

  defp is_regular_interval?(intervals) do
    case intervals do
      [] ->
        false

      [_] ->
        false

      intervals ->
        avg_interval = Enum.sum(intervals) / length(intervals)

        variance =
          Enum.reduce(intervals, 0, fn interval, acc ->
            acc + :math.pow(interval - avg_interval, 2)
          end) / length(intervals)

        # Regular if variance is low relative to average
        variance < avg_interval * 0.1
    end
  end

  defp has_cascading_effect?(conflicts) do
    # Check if conflicts show cascading pattern
    length(conflicts) > 2
  end

  defp involves_resource_contention?(conflicts) do
    # Check if conflicts involve resource contention
    Enum.any?(conflicts, fn conflict -> conflict.type == :resource_contention end)
  end

  defp calculate_pattern_impact_score(conflicts) do
    # Calculate impact based on conflict severity and frequency
    severity_scores = %{critical: 1.0, high: 0.8, medium: 0.5, low: 0.2}

    total_impact =
      Enum.reduce(conflicts, 0, fn conflict, acc ->
        severity_score = Map.get(severity_scores, conflict.severity, 0.1)
        acc + severity_score
      end)

    total_impact / length(conflicts)
  end

  defp create_pattern_signature(rules, conflicts) do
    # Create unique signature for pattern
    rule_signature = Enum.join(Enum.sort(rules), "-")

    conflict_types =
      conflicts |> Enum.map(& &1.type) |> Enum.uniq() |> Enum.sort() |> Enum.join(",")

    "#{rule_signature}:#{conflict_types}"
  end

  defp build_performance_impact_model(conflict_data, simulation_params) do
    # Build performance impact model
    base_impact = calculate_base_impact(conflict_data)
    duration = Map.get(simulation_params, :duration, 300)
    load = Map.get(simulation_params, :load, :medium)

    load_multiplier =
      case load do
        :low -> 0.5
        :medium -> 1.0
        :high -> 2.0
        :extreme -> 4.0
      end

    %{
      base_impact: base_impact,
      projected_impact: base_impact * load_multiplier,
      simulation_duration: duration,
      load_level: load,
      impact_breakdown: %{
        latency_increase: base_impact.latency_ms * load_multiplier,
        throughput_decrease: base_impact.throughput_loss * load_multiplier,
        memory_overhead: base_impact.memory_mb * load_multiplier,
        cpu_overhead: base_impact.cpu_percent * load_multiplier
      },
      confidence_level: calculate_model_confidence(conflict_data)
    }
  end

  defp calculate_base_impact(conflict_data) do
    # Calculate base performance impact
    severity_impact =
      case conflict_data.severity do
        :critical -> %{latency_ms: 100, throughput_loss: 0.3, memory_mb: 50, cpu_percent: 20}
        :high -> %{latency_ms: 50, throughput_loss: 0.15, memory_mb: 25, cpu_percent: 10}
        :medium -> %{latency_ms: 20, throughput_loss: 0.05, memory_mb: 10, cpu_percent: 5}
        :low -> %{latency_ms: 5, throughput_loss: 0.01, memory_mb: 2, cpu_percent: 1}
      end

    # Adjust based on number of rules involved
    rule_factor = max(1.0, length(conflict_data.rules_involved) * 0.2)

    %{
      latency_ms: severity_impact.latency_ms * rule_factor,
      throughput_loss: severity_impact.throughput_loss * rule_factor,
      memory_mb: severity_impact.memory_mb * rule_factor,
      cpu_percent: severity_impact.cpu_percent * rule_factor
    }
  end

  defp calculate_model_confidence(conflict_data) do
    # Calculate confidence in the performance model
    base_confidence = 0.7

    # Increase confidence if we have more data
    data_factor = min(0.2, map_size(conflict_data) * 0.02)

    # Adjust based on conflict complexity
    complexity_factor =
      case length(conflict_data.rules_involved) do
        n when n <= 2 -> 0.1
        n when n <= 5 -> 0.0
        _ -> -0.1
      end

    max(0.0, min(1.0, base_confidence + data_factor + complexity_factor))
  end

  defp perform_sequence_analysis(firing_history, sequence_length) do
    # Analyze rule firing sequences
    sequences = extract_firing_sequences(firing_history, sequence_length)

    %{
      total_sequences: length(sequences),
      unique_sequences: length(Enum.uniq(sequences)),
      most_common_sequences: find_most_common_sequences(sequences, 5),
      sequence_patterns: identify_sequence_patterns(sequences),
      predictive_patterns: build_predictive_patterns(sequences)
    }
  end

  defp extract_firing_sequences(firing_history, sequence_length) do
    firing_history
    |> Enum.sort_by(& &1.timestamp)
    |> Enum.map(& &1.rule)
    |> Enum.chunk_every(sequence_length, 1, :discard)
  end

  defp find_most_common_sequences(sequences, limit) do
    sequences
    |> Enum.frequencies()
    |> Enum.sort_by(fn {_seq, count} -> count end, :desc)
    |> Enum.take(limit)
  end

  defp identify_sequence_patterns(sequences) do
    # Identify patterns in sequences (e.g., cycles, alternations)
    %{
      cyclic_patterns: find_cyclic_patterns(sequences),
      alternating_patterns: find_alternating_patterns(sequences),
      escalation_patterns: find_escalation_patterns(sequences)
    }
  end

  defp find_cyclic_patterns(sequences) do
    # Find repeating cycles in sequences
    sequences
    |> Enum.filter(fn seq -> has_repeating_cycle?(seq) end)
    |> Enum.take(10)
  end

  defp has_repeating_cycle?(sequence) do
    # Check if sequence has repeating cycles
    seq_length = length(sequence)

    Enum.any?(1..div(seq_length, 2), fn cycle_length ->
      cycles = Enum.chunk_every(sequence, cycle_length)

      case cycles do
        [first_cycle | rest] -> Enum.all?(rest, &(&1 == first_cycle))
        _ -> false
      end
    end)
  end

  defp find_alternating_patterns(sequences) do
    # Find alternating patterns
    sequences
    |> Enum.filter(fn seq -> is_alternating_pattern?(seq) end)
    |> Enum.take(10)
  end

  defp is_alternating_pattern?(sequence) do
    case sequence do
      [a, b | _rest] ->
        expected = Stream.cycle([a, b]) |> Enum.take(length(sequence))
        sequence == expected

      _ ->
        false
    end
  end

  defp find_escalation_patterns(_sequences) do
    # Find escalation patterns (increasing rule priorities)
    # Placeholder
    []
  end

  defp build_predictive_patterns(sequences) do
    # Build patterns for predicting next rule firing
    transition_matrix = build_transition_matrix(sequences)

    %{
      transition_matrix: transition_matrix,
      prediction_accuracy: estimate_prediction_accuracy(transition_matrix),
      high_confidence_predictions: find_high_confidence_predictions(transition_matrix)
    }
  end

  defp build_transition_matrix(sequences) do
    # Build transition probability matrix
    transitions =
      Enum.flat_map(sequences, fn sequence ->
        Enum.chunk_every(sequence, 2, 1, :discard)
      end)

    transition_counts = Enum.frequencies(transitions)
    total_transitions = Enum.sum(Map.values(transition_counts))

    Map.new(transition_counts, fn {transition, count} ->
      {transition, count / total_transitions}
    end)
  end

  defp estimate_prediction_accuracy(transition_matrix) do
    # Estimate how accurate predictions would be
    if map_size(transition_matrix) == 0 do
      0.0
    else
      max_probabilities =
        transition_matrix
        |> Map.values()
        |> Enum.map(&Float.round(&1, 3))

      Enum.sum(max_probabilities) / length(max_probabilities)
    end
  end

  defp find_high_confidence_predictions(transition_matrix) do
    # Find transitions with high prediction confidence
    threshold = 0.7

    transition_matrix
    |> Enum.filter(fn {_transition, probability} -> probability >= threshold end)
    |> Enum.sort_by(fn {_transition, probability} -> probability end, :desc)
    |> Enum.take(10)
  end

  defp gather_training_data(_engine_name, training_window) do
    # Gather training data for prediction model
    {:ok,
     %{
       conflicts: [],
       rule_firings: [],
       system_metrics: [],
       time_window: training_window
     }}
  end

  defp build_prediction_model(training_data, opts) do
    # Build machine learning model for conflict prediction
    %{
      model_type: :simple_statistical,
      accuracy: 0.75,
      features_used: [:rule_frequency, :timing_patterns, :system_load],
      training_data_size: map_size(training_data),
      model_parameters: %{
        prediction_horizon: Keyword.get(opts, :prediction_horizon, 300),
        confidence_threshold: Keyword.get(opts, :confidence_threshold, 0.8)
      }
    }
  end

  defp calculate_interaction_matrix(rules, _opts) do
    # Calculate interaction strength matrix between rules
    matrix = Map.new()

    rule_pairs = for r1 <- rules, r2 <- rules, do: {r1, r2}

    interaction_matrix =
      Enum.reduce(rule_pairs, matrix, fn {rule1, rule2}, acc ->
        strength = calculate_pairwise_interaction_strength(rule1, rule2)
        Map.put(acc, {rule1, rule2}, strength)
      end)

    {:ok, interaction_matrix}
  end

  defp calculate_pairwise_interaction_strength(rule1, rule2) do
    # Calculate interaction strength between two rules
    cond do
      rule1 == rule2 -> 1.0
      have_shared_facts?(rule1, rule2) -> 0.8
      have_priority_interaction?(rule1, rule2) -> 0.6
      have_resource_interaction?(rule1, rule2) -> 0.4
      true -> 0.1
    end
  end

  defp have_shared_facts?(_rule1, _rule2) do
    # Check if rules share fact references
    # Placeholder
    false
  end

  defp have_priority_interaction?(_rule1, _rule2) do
    # Check if rules have priority-based interactions
    # Placeholder
    false
  end

  defp have_resource_interaction?(_rule1, _rule2) do
    # Check if rules compete for resources
    # Placeholder
    false
  end

  defp find_strongest_interactions(matrix) do
    # Find strongest rule interactions
    matrix
    |> Enum.filter(fn {{r1, r2}, strength} -> r1 != r2 and strength > 0.5 end)
    |> Enum.sort_by(fn {_rules, strength} -> strength end, :desc)
    |> Enum.take(10)
  end

  defp identify_interaction_clusters(_matrix) do
    # Identify clusters of strongly interacting rules
    # Placeholder implementation
    []
  end

  defp calculate_interaction_metrics(matrix) do
    # Calculate metrics about rule interactions
    non_self_interactions = Enum.filter(matrix, fn {{r1, r2}, _} -> r1 != r2 end)
    strengths = Enum.map(non_self_interactions, fn {_, strength} -> strength end)

    %{
      total_interactions: length(non_self_interactions),
      average_strength:
        if(length(strengths) > 0, do: Enum.sum(strengths) / length(strengths), else: 0.0),
      max_strength: Enum.max(strengths, fn -> 0.0 end),
      min_strength: Enum.min(strengths, fn -> 0.0 end),
      strong_interactions: Enum.count(strengths, &(&1 > 0.7))
    }
  end

  defp generate_dependency_graph_viz(engine_name, opts) do
    case analyze_rule_dependencies(engine_name, opts) do
      {:ok, graph} ->
        viz_data = %{
          type: :dependency_graph,
          nodes:
            Enum.map(graph.nodes, fn node ->
              %{id: node, label: to_string(node), type: :rule}
            end),
          edges:
            Enum.map(graph.edges, fn edge ->
              %{
                from: edge.from_rule,
                to: edge.to_rule,
                type: edge.dependency_type,
                weight: edge.strength,
                label: to_string(edge.dependency_type)
              }
            end),
          layout: %{
            algorithm: :force_directed,
            clustering: graph.strongly_connected_components
          }
        }

        {:ok, viz_data}

      error ->
        error
    end
  end

  defp generate_conflict_timeline_viz(engine_name, opts) do
    time_window = Keyword.get(opts, :time_window, 3600)

    case get_conflict_history(engine_name, time_window) do
      {:ok, conflicts} ->
        viz_data = %{
          type: :timeline,
          events:
            Enum.map(conflicts, fn conflict ->
              %{
                id: conflict.id,
                timestamp: conflict.timestamp,
                type: conflict.type,
                severity: conflict.severity,
                rules: conflict.rules_involved,
                description: "Conflict between #{Enum.join(conflict.rules_involved, ", ")}"
              }
            end),
          time_range: {
            DateTime.add(DateTime.utc_now(), -time_window, :second),
            DateTime.utc_now()
          }
        }

        {:ok, viz_data}

      error ->
        error
    end
  end

  defp generate_interaction_heatmap_viz(engine_name, opts) do
    case get_engine_rules(engine_name) do
      {:ok, rules} ->
        rule_ids = Enum.map(rules, & &1.id)

        case analyze_interaction_strength(rule_ids, opts) do
          {:ok, analysis} ->
            viz_data = %{
              type: :heatmap,
              rules: rule_ids,
              interaction_matrix: analysis.interaction_matrix,
              color_scale: %{
                min: 0.0,
                max: 1.0,
                colors: ["#ffffff", "#ff0000"]
              }
            }

            {:ok, viz_data}

          error ->
            error
        end

      error ->
        error
    end
  end

  defp generate_pattern_analysis_viz(engine_name, opts) do
    case detect_conflict_patterns(engine_name, opts) do
      {:ok, patterns} ->
        viz_data = %{
          type: :pattern_analysis,
          patterns:
            Enum.map(patterns, fn pattern ->
              %{
                id: pattern.id,
                type: pattern.pattern_type,
                rules: pattern.rules_involved,
                frequency: pattern.frequency,
                impact: pattern.impact_score,
                timespan: {pattern.first_occurrence, pattern.last_occurrence}
              }
            end),
          summary: %{
            total_patterns: length(patterns),
            pattern_types: Enum.frequencies(Enum.map(patterns, & &1.pattern_type))
          }
        }

        {:ok, viz_data}

      error ->
        error
    end
  end

  defp generate_pattern_id(rules) do
    rule_string = rules |> Enum.sort() |> Enum.join("-")

    "pattern_" <>
      (rule_string |> :crypto.hash(:md5) |> Base.encode16(case: :lower) |> String.slice(0, 8))
  end
end
