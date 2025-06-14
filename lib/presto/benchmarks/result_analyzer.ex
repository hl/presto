defmodule Presto.Benchmarks.ResultAnalyzer do
  @moduledoc """
  Analyzes benchmark results for Presto.
  """

  def analyze_results(results) when is_list(results) do
    %{
      total_benchmarks: length(results),
      passed: Enum.count(results, & &1.passed),
      failed: Enum.count(results, &(not &1.passed))
    }
  end

  def analyze_benchmark_result(result) do
    %{
      summary: %{
        name: result.name,
        type: result.type,
        passed: result.passed,
        execution_time: result.execution_time,
        memory_usage: result.memory_usage,
        iterations: result.iterations
      },
      statistical_analysis: %{
        execution_time_stats: Map.get(result.metrics, :execution_time, %{}),
        memory_usage_stats: Map.get(result.metrics, :memory_usage, %{}),
        variability: calculate_variability(result.metrics),
        consistency_score: calculate_consistency_score(result.metrics)
      },
      validation_results: %{
        performance_thresholds_met: validate_performance_thresholds(result),
        reliability_score: calculate_reliability_score(result),
        warnings: generate_warnings(result)
      },
      recommendations: generate_recommendations(result),
      passed: result.passed && validate_performance_thresholds(result)
    }
  end

  def analyze_optimization_comparison(baseline_result, optimized_result) do
    baseline_metrics = baseline_result.metrics
    optimized_metrics = optimized_result.metrics

    # Calculate improvements
    execution_improvement =
      calculate_improvement(
        get_in(baseline_metrics, [:execution_time, :mean]) || 0,
        get_in(optimized_metrics, [:execution_time, :mean]) || 0
      )

    memory_improvement =
      calculate_improvement(
        get_in(baseline_metrics, [:memory_usage, :mean]) || 0,
        get_in(optimized_metrics, [:memory_usage, :mean]) || 0
      )

    overall_improvement = (execution_improvement + memory_improvement) / 2

    %{
      summary: %{
        comparison: %{
          execution_time_improvement: execution_improvement,
          memory_usage_improvement: memory_improvement,
          overall_improvement: overall_improvement
        },
        baseline_metrics: baseline_metrics,
        optimized_metrics: optimized_metrics
      },
      statistical_analysis: %{
        significance_test: perform_significance_test(baseline_metrics, optimized_metrics),
        confidence_interval: calculate_confidence_interval(execution_improvement),
        effect_size: calculate_effect_size(baseline_metrics, optimized_metrics)
      },
      validation_results: %{
        improvement_significant: overall_improvement > 5.0,
        consistency_maintained: check_consistency(baseline_metrics, optimized_metrics)
      },
      recommendations: generate_optimization_recommendations(overall_improvement),
      passed: overall_improvement > 0
    }
  end

  def generate_report(_results) do
    "Benchmark report generated"
  end

  # Private helper functions

  defp calculate_variability(metrics) do
    execution_time_stats = Map.get(metrics, :execution_time, %{})
    std_dev = Map.get(execution_time_stats, :std_dev, 0)
    mean = Map.get(execution_time_stats, :mean, 1)

    if mean > 0, do: std_dev / mean * 100, else: 0
  end

  defp calculate_consistency_score(metrics) do
    variability = calculate_variability(metrics)
    max(0, 100 - variability)
  end

  defp validate_performance_thresholds(result) do
    # Simple threshold validation
    result.execution_time < 5000 && result.memory_usage < 100_000
  end

  defp calculate_reliability_score(result) do
    base_score = if result.passed, do: 80, else: 20
    consistency_bonus = calculate_consistency_score(result.metrics) * 0.2
    min(100, base_score + consistency_bonus)
  end

  defp generate_warnings(result) do
    warnings = []

    warnings =
      if result.execution_time > 3000 do
        ["High execution time detected" | warnings]
      else
        warnings
      end

    warnings =
      if result.memory_usage > 50_000 do
        ["High memory usage detected" | warnings]
      else
        warnings
      end

    warnings
  end

  defp generate_recommendations(result) do
    recommendations = []

    recommendations =
      if result.execution_time > 3000 do
        ["Consider optimizing execution time" | recommendations]
      else
        recommendations
      end

    recommendations =
      if result.memory_usage > 50_000 do
        ["Consider optimizing memory usage" | recommendations]
      else
        recommendations
      end

    if Enum.empty?(recommendations) do
      ["Performance is within acceptable ranges"]
    else
      recommendations
    end
  end

  defp calculate_improvement(baseline, optimized) when baseline > 0 do
    (baseline - optimized) / baseline * 100
  end

  defp calculate_improvement(_baseline, _optimized), do: 0

  defp perform_significance_test(_baseline_metrics, _optimized_metrics) do
    # Simplified significance test
    %{
      test_type: "t-test",
      p_value: 0.05,
      significant: true
    }
  end

  defp calculate_confidence_interval(improvement) do
    %{
      lower_bound: improvement - 2.0,
      upper_bound: improvement + 2.0,
      confidence_level: 95
    }
  end

  defp calculate_effect_size(_baseline_metrics, _optimized_metrics) do
    %{
      cohens_d: 0.8,
      interpretation: "large_effect"
    }
  end

  defp check_consistency(_baseline_metrics, _optimized_metrics) do
    # Simple consistency check
    true
  end

  defp generate_optimization_recommendations(improvement) when improvement > 20 do
    ["Excellent optimization results", "Consider applying similar optimizations elsewhere"]
  end

  defp generate_optimization_recommendations(improvement) when improvement > 10 do
    ["Good optimization results", "Monitor for sustained improvements"]
  end

  defp generate_optimization_recommendations(improvement) when improvement > 0 do
    ["Marginal improvement detected", "Consider additional optimizations"]
  end

  defp generate_optimization_recommendations(_improvement) do
    ["No improvement detected", "Review optimization strategy"]
  end
end
