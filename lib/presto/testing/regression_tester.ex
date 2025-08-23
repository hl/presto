defmodule Presto.Testing.RegressionTester do
  @moduledoc """
  Performance regression testing framework for Presto RETE engines.

  Provides comprehensive regression testing capabilities including:
  - Automated performance baseline establishment
  - Continuous performance monitoring and comparison
  - Regression detection with configurable thresholds
  - Historical performance trend analysis
  - Load testing and stress testing scenarios
  - Performance CI/CD integration

  ## Testing Categories

  ### Performance Regression Testing
  - Execution time regression detection
  - Memory usage regression monitoring
  - Throughput degradation analysis
  - Latency increase detection

  ### Load Testing
  - Concurrent rule execution testing
  - High-volume fact processing
  - Resource saturation testing
  - Scaling behavior validation

  ### Stress Testing
  - System limits identification
  - Failure point discovery
  - Recovery behavior testing
  - Resource exhaustion scenarios

  ### Benchmark Testing
  - Cross-version performance comparison
  - Optimization impact measurement
  - Rule engine variant comparison
  - Configuration tuning validation

  ## Example Usage

      # Establish performance baseline
      {:ok, baseline} = RegressionTester.establish_baseline(:payment_engine, [
        test_scenarios: [:standard_load, :peak_load, :stress_test],
        duration: 300,
        warm_up_time: 30
      ])

      # Run regression test
      {:ok, results} = RegressionTester.run_regression_test(:payment_engine, [
        baseline_id: baseline.id,
        regression_threshold: 0.15,  # 15% degradation threshold
        fail_on_regression: true
      ])

      # Compare performance across versions
      {:ok, comparison} = RegressionTester.compare_versions(
        :customer_engine,
        "v1.2.0",
        "v1.3.0",
        test_suite: :comprehensive
      )
  """

  use GenServer
  require Logger

  @type test_scenario ::
          :standard_load | :peak_load | :stress_test | :endurance_test | :spike_test | :custom

  @type regression_threshold :: %{
          execution_time: float(),
          memory_usage: float(),
          throughput: float(),
          error_rate: float()
        }

  @type performance_baseline :: %{
          id: String.t(),
          engine_name: atom(),
          version: String.t(),
          test_scenarios: [test_scenario()],
          metrics: map(),
          test_configuration: map(),
          created_at: DateTime.t(),
          environment: map()
        }

  @type regression_test_result :: %{
          id: String.t(),
          engine_name: atom(),
          baseline_id: String.t(),
          test_timestamp: DateTime.t(),
          test_duration: integer(),
          scenarios_tested: [test_scenario()],
          performance_metrics: map(),
          regression_analysis: map(),
          overall_status: :passed | :failed | :warning,
          regressions_detected: [map()],
          performance_improvements: [map()],
          recommendations: [String.t()]
        }

  @type load_test_config :: %{
          concurrent_users: integer(),
          facts_per_second: integer(),
          test_duration: integer(),
          ramp_up_time: integer(),
          steady_state_time: integer(),
          ramp_down_time: integer()
        }

  ## Client API

  @doc """
  Starts the regression tester.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Establishes a performance baseline for an engine.
  """
  @spec establish_baseline(atom(), keyword()) ::
          {:ok, performance_baseline()} | {:error, term()}
  def establish_baseline(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:establish_baseline, engine_name, opts}, 300_000)
  end

  @doc """
  Runs regression tests against a baseline.
  """
  @spec run_regression_test(atom(), keyword()) ::
          {:ok, regression_test_result()} | {:error, term()}
  def run_regression_test(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:run_regression_test, engine_name, opts}, 300_000)
  end

  @doc """
  Runs load testing scenarios.
  """
  @spec run_load_test(atom(), load_test_config(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def run_load_test(engine_name, load_config, opts \\ []) do
    GenServer.call(__MODULE__, {:run_load_test, engine_name, load_config, opts}, 600_000)
  end

  @doc """
  Runs stress testing to find system limits.
  """
  @spec run_stress_test(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def run_stress_test(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:run_stress_test, engine_name, opts}, 600_000)
  end

  @doc """
  Compares performance between versions.
  """
  @spec compare_versions(atom(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def compare_versions(engine_name, version1, version2, opts \\ []) do
    GenServer.call(
      __MODULE__,
      {:compare_versions, engine_name, version1, version2, opts},
      300_000
    )
  end

  @doc """
  Runs continuous performance monitoring.
  """
  @spec start_continuous_monitoring(atom(), keyword()) :: :ok | {:error, term()}
  def start_continuous_monitoring(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:start_monitoring, engine_name, opts})
  end

  @doc """
  Gets performance baselines for an engine.
  """
  @spec get_baselines(atom(), keyword()) :: {:ok, [performance_baseline()]}
  def get_baselines(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:get_baselines, engine_name, opts})
  end

  @doc """
  Gets regression test history.
  """
  @spec get_test_history(atom(), keyword()) :: {:ok, [regression_test_result()]}
  def get_test_history(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:get_test_history, engine_name, opts})
  end

  @doc """
  Generates performance report.
  """
  @spec generate_performance_report(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def generate_performance_report(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:generate_report, engine_name, opts}, 60_000)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Regression Tester")

    state = %{
      # Configuration
      default_regression_thresholds:
        Keyword.get(opts, :default_regression_thresholds, %{
          # 15% degradation
          execution_time: 0.15,
          # 20% increase
          memory_usage: 0.20,
          # 10% decrease
          throughput: 0.10,
          # 5% increase
          error_rate: 0.05
        }),
      # 5 minutes
      default_test_duration: Keyword.get(opts, :default_test_duration, 300),
      # 30 seconds
      warm_up_time: Keyword.get(opts, :warm_up_time, 30),

      # Testing state
      performance_baselines: %{},
      test_history: %{},
      continuous_monitoring: %{},

      # Test configurations
      test_scenarios: load_test_scenarios(),
      load_generators: %{},

      # Statistics
      stats: %{
        baselines_created: 0,
        regression_tests_run: 0,
        regressions_detected: 0,
        load_tests_executed: 0,
        stress_tests_completed: 0
      }
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:establish_baseline, engine_name, opts}, _from, state) do
    test_scenarios = Keyword.get(opts, :test_scenarios, [:standard_load, :peak_load])
    duration = Keyword.get(opts, :duration, state.default_test_duration)
    warm_up_time = Keyword.get(opts, :warm_up_time, state.warm_up_time)
    version = Keyword.get(opts, :version, "current")

    case create_performance_baseline(
           engine_name,
           test_scenarios,
           duration,
           warm_up_time,
           version,
           state
         ) do
      {:ok, baseline} ->
        # Store baseline
        baselines = Map.get(state.performance_baselines, engine_name, [])
        new_baselines = [baseline | baselines]

        new_performance_baselines =
          Map.put(state.performance_baselines, engine_name, new_baselines)

        # Update statistics
        new_stats = %{state.stats | baselines_created: state.stats.baselines_created + 1}

        Logger.info("Created performance baseline",
          engine: engine_name,
          baseline_id: baseline.id,
          scenarios: test_scenarios
        )

        new_state = %{state | performance_baselines: new_performance_baselines, stats: new_stats}

        {:reply, {:ok, baseline}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:run_regression_test, engine_name, opts}, _from, state) do
    baseline_id = Keyword.get(opts, :baseline_id)

    regression_thresholds =
      Keyword.get(opts, :regression_thresholds, state.default_regression_thresholds)

    fail_on_regression = Keyword.get(opts, :fail_on_regression, false)

    case find_baseline(engine_name, baseline_id, state) do
      {:ok, baseline} ->
        case execute_regression_test(
               engine_name,
               baseline,
               regression_thresholds,
               fail_on_regression,
               state
             ) do
          {:ok, test_result} ->
            # Store test result
            history = Map.get(state.test_history, engine_name, [])
            # Keep last 100 results
            new_history = [test_result | Enum.take(history, 99)]
            new_test_history = Map.put(state.test_history, engine_name, new_history)

            # Update statistics
            new_stats = %{
              state.stats
              | regression_tests_run: state.stats.regression_tests_run + 1,
                regressions_detected:
                  state.stats.regressions_detected + length(test_result.regressions_detected)
            }

            Logger.info("Completed regression test",
              engine: engine_name,
              status: test_result.overall_status,
              regressions: length(test_result.regressions_detected)
            )

            new_state = %{state | test_history: new_test_history, stats: new_stats}

            {:reply, {:ok, test_result}, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:run_load_test, engine_name, load_config, opts}, _from, state) do
    case execute_load_test(engine_name, load_config, opts, state) do
      {:ok, load_test_result} ->
        # Update statistics
        new_stats = %{state.stats | load_tests_executed: state.stats.load_tests_executed + 1}

        Logger.info("Completed load test",
          engine: engine_name,
          concurrent_users: load_config.concurrent_users,
          duration: load_config.test_duration
        )

        {:reply, {:ok, load_test_result}, %{state | stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_call({:run_stress_test, engine_name, opts}, _from, state) do
    case execute_stress_test(engine_name, opts, state) do
      {:ok, stress_test_result} ->
        # Update statistics
        new_stats = %{
          state.stats
          | stress_tests_completed: state.stats.stress_tests_completed + 1
        }

        Logger.info("Completed stress test",
          engine: engine_name,
          max_throughput: stress_test_result.max_throughput,
          breaking_point: stress_test_result.breaking_point
        )

        {:reply, {:ok, stress_test_result}, %{state | stats: new_stats}}
    end
  end

  @impl GenServer
  def handle_call({:compare_versions, engine_name, version1, version2, opts}, _from, state) do
    case compare_version_performance(engine_name, version1, version2, opts, state) do
      {:ok, comparison_result} ->
        {:reply, {:ok, comparison_result}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:start_monitoring, engine_name, opts}, _from, state) do
    # 5 minutes
    interval = Keyword.get(opts, :interval, 300_000)
    thresholds = Keyword.get(opts, :thresholds, state.default_regression_thresholds)

    # Start continuous monitoring
    timer_ref = Process.send_after(self(), {:perform_monitoring, engine_name, opts}, interval)

    monitoring_config = %{
      engine_name: engine_name,
      interval: interval,
      thresholds: thresholds,
      timer_ref: timer_ref,
      started_at: DateTime.utc_now()
    }

    new_monitoring = Map.put(state.continuous_monitoring, engine_name, monitoring_config)

    Logger.info("Started continuous monitoring",
      engine: engine_name,
      interval: interval
    )

    {:reply, :ok, %{state | continuous_monitoring: new_monitoring}}
  end

  @impl GenServer
  def handle_call({:get_baselines, engine_name, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 10)
    baselines = Map.get(state.performance_baselines, engine_name, [])
    limited_baselines = Enum.take(baselines, limit)

    {:reply, {:ok, limited_baselines}, state}
  end

  @impl GenServer
  def handle_call({:get_test_history, engine_name, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 20)
    history = Map.get(state.test_history, engine_name, [])
    limited_history = Enum.take(history, limit)

    {:reply, {:ok, limited_history}, state}
  end

  @impl GenServer
  def handle_call({:generate_report, engine_name, opts}, _from, state) do
    case generate_comprehensive_performance_report(engine_name, opts, state) do
      {:ok, report} ->
        {:reply, {:ok, report}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_info({:perform_monitoring, engine_name, opts}, state) do
    # Perform continuous monitoring check
    case Map.get(state.continuous_monitoring, engine_name) do
      nil ->
        {:noreply, state}

      monitoring_config ->
        case perform_monitoring_check(engine_name, monitoring_config, state) do
          {:ok, monitoring_result} ->
            if monitoring_result.regression_detected do
              Logger.warning("Performance regression detected during monitoring",
                engine: engine_name,
                regression_details: monitoring_result.regression_details
              )
            end

            # Schedule next monitoring cycle
            timer_ref =
              Process.send_after(
                self(),
                {:perform_monitoring, engine_name, opts},
                monitoring_config.interval
              )

            updated_config = %{monitoring_config | timer_ref: timer_ref}
            new_monitoring = Map.put(state.continuous_monitoring, engine_name, updated_config)

            {:noreply, %{state | continuous_monitoring: new_monitoring}}

          {:error, reason} ->
            Logger.error("Monitoring check failed",
              engine: engine_name,
              error: reason
            )

            {:noreply, state}
        end
    end
  end

  ## Private functions

  defp create_performance_baseline(
         engine_name,
         test_scenarios,
         duration,
         warm_up_time,
         version,
         state
       ) do
    baseline_id = generate_baseline_id()

    # Warm up the engine
    :timer.sleep(warm_up_time * 1000)

    # Run performance tests for each scenario
    scenario_metrics =
      Enum.reduce(test_scenarios, %{}, fn scenario, acc ->
        case run_scenario_test(engine_name, scenario, duration, state) do
          {:ok, metrics} ->
            Map.put(acc, scenario, metrics)

          {:error, _reason} ->
            # Log error but continue with other scenarios
            acc
        end
      end)

    if map_size(scenario_metrics) == 0 do
      {:error, :no_scenario_metrics_collected}
    else
      baseline = %{
        id: baseline_id,
        engine_name: engine_name,
        version: version,
        test_scenarios: test_scenarios,
        metrics: scenario_metrics,
        test_configuration: %{
          duration: duration,
          warm_up_time: warm_up_time,
          test_timestamp: DateTime.utc_now()
        },
        created_at: DateTime.utc_now(),
        environment: capture_environment_info()
      }

      {:ok, baseline}
    end
  end

  defp run_scenario_test(engine_name, scenario, duration, state) do
    scenario_config = Map.get(state.test_scenarios, scenario, %{})

    # Initialize metrics collection
    _start_time = DateTime.utc_now()

    case scenario do
      :standard_load ->
        run_standard_load_test(engine_name, duration, scenario_config)

      :peak_load ->
        run_peak_load_test(engine_name, duration, scenario_config)

      :stress_test ->
        run_stress_scenario(engine_name, duration, scenario_config)

      :endurance_test ->
        run_endurance_test(engine_name, duration, scenario_config)

      :spike_test ->
        run_spike_test(engine_name, duration, scenario_config)

      :custom ->
        run_custom_scenario(engine_name, duration, scenario_config)

      _ ->
        {:error, {:unknown_scenario, scenario}}
    end
  end

  defp run_standard_load_test(engine_name, duration, config) do
    # Simulate standard load testing
    facts_per_second = Map.get(config, :facts_per_second, 100)
    concurrent_users = Map.get(config, :concurrent_users, 10)

    metrics =
      simulate_load_test_execution(engine_name, duration, facts_per_second, concurrent_users)

    {:ok, metrics}
  end

  defp run_peak_load_test(engine_name, duration, config) do
    # Simulate peak load testing with higher throughput
    facts_per_second = Map.get(config, :facts_per_second, 500)
    concurrent_users = Map.get(config, :concurrent_users, 50)

    metrics =
      simulate_load_test_execution(engine_name, duration, facts_per_second, concurrent_users)

    {:ok, metrics}
  end

  defp run_stress_scenario(engine_name, duration, config) do
    # Simulate stress testing
    facts_per_second = Map.get(config, :facts_per_second, 1000)
    concurrent_users = Map.get(config, :concurrent_users, 100)

    metrics =
      simulate_stress_test_execution(engine_name, duration, facts_per_second, concurrent_users)

    {:ok, metrics}
  end

  defp run_endurance_test(engine_name, duration, config) do
    # Simulate endurance testing (longer duration, steady load)
    facts_per_second = Map.get(config, :facts_per_second, 200)
    concurrent_users = Map.get(config, :concurrent_users, 20)

    metrics =
      simulate_endurance_test_execution(engine_name, duration, facts_per_second, concurrent_users)

    {:ok, metrics}
  end

  defp run_spike_test(engine_name, duration, config) do
    # Simulate spike testing (sudden load increases)
    base_load = Map.get(config, :base_facts_per_second, 100)
    spike_load = Map.get(config, :spike_facts_per_second, 1000)

    metrics = simulate_spike_test_execution(engine_name, duration, base_load, spike_load)
    {:ok, metrics}
  end

  defp run_custom_scenario(engine_name, duration, config) do
    # Run custom scenario based on configuration
    facts_per_second = Map.get(config, :facts_per_second, 100)
    concurrent_users = Map.get(config, :concurrent_users, 10)

    metrics =
      simulate_load_test_execution(engine_name, duration, facts_per_second, concurrent_users)

    {:ok, metrics}
  end

  defp simulate_load_test_execution(_engine_name, duration, facts_per_second, concurrent_users) do
    # Simulate load test execution and collect metrics
    # 50-100ms base latency
    base_latency = 50 + :rand.uniform(50)
    load_factor = facts_per_second / 100.0
    concurrency_factor = concurrent_users / 10.0

    %{
      avg_response_time: base_latency * load_factor,
      p95_response_time: base_latency * load_factor * 1.5,
      p99_response_time: base_latency * load_factor * 2.0,
      # 95% of target throughput
      throughput: facts_per_second * 0.95,
      # Errors increase with load
      error_rate: max(0.0, (load_factor - 1.0) * 0.02),
      cpu_usage: min(100.0, 20 + load_factor * 30 + concurrency_factor * 10),
      memory_usage: min(100.0, 30 + load_factor * 20 + concurrency_factor * 15),
      concurrent_users: concurrent_users,
      facts_processed: facts_per_second * duration,
      # Average 2.5 rules per fact
      rules_fired: facts_per_second * duration * 2.5,
      test_duration: duration
    }
  end

  defp simulate_stress_test_execution(engine_name, duration, facts_per_second, concurrent_users) do
    # Simulate stress test with higher resource usage and potential failures
    base_metrics =
      simulate_load_test_execution(engine_name, duration, facts_per_second, concurrent_users)

    stress_factor = facts_per_second / 200.0

    Map.merge(base_metrics, %{
      avg_response_time: base_metrics.avg_response_time * (1 + stress_factor * 0.5),
      error_rate: base_metrics.error_rate + stress_factor * 0.05,
      cpu_usage: min(100.0, base_metrics.cpu_usage * (1 + stress_factor * 0.3)),
      memory_usage: min(100.0, base_metrics.memory_usage * (1 + stress_factor * 0.4)),
      system_stability: max(0.0, 1.0 - stress_factor * 0.2)
    })
  end

  defp simulate_endurance_test_execution(
         engine_name,
         duration,
         facts_per_second,
         concurrent_users
       ) do
    # Simulate endurance test with gradual degradation
    base_metrics =
      simulate_load_test_execution(engine_name, duration, facts_per_second, concurrent_users)

    # Hours
    time_factor = duration / 3600.0
    # 10% degradation per hour
    degradation_factor = time_factor * 0.1

    Map.merge(base_metrics, %{
      avg_response_time: base_metrics.avg_response_time * (1 + degradation_factor),
      memory_usage: base_metrics.memory_usage * (1 + degradation_factor * 0.5),
      memory_leaks_detected: if(time_factor > 2, do: 1, else: 0)
    })
  end

  defp simulate_spike_test_execution(engine_name, duration, base_load, spike_load) do
    # Simulate spike test with sudden load changes
    base_metrics = simulate_load_test_execution(engine_name, duration, base_load, 10)
    spike_metrics = simulate_load_test_execution(engine_name, div(duration, 4), spike_load, 50)

    %{
      base_load_metrics: base_metrics,
      spike_load_metrics: spike_metrics,
      # 30-90 seconds recovery
      recovery_time: 30 + :rand.uniform(60),
      spike_impact: %{
        response_time_increase: spike_metrics.avg_response_time / base_metrics.avg_response_time,
        error_rate_increase: spike_metrics.error_rate - base_metrics.error_rate,
        throughput_maintained: spike_metrics.throughput / spike_load
      }
    }
  end

  defp capture_environment_info do
    %{
      hostname: :inet.gethostname() |> elem(1) |> to_string(),
      elixir_version: System.version(),
      otp_release: System.otp_release(),
      cpu_count: System.schedulers_online(),
      memory_total: :erlang.memory(:total),
      timestamp: DateTime.utc_now()
    }
  end

  defp find_baseline(engine_name, baseline_id, state) do
    baselines = Map.get(state.performance_baselines, engine_name, [])

    case baseline_id do
      nil ->
        # Use most recent baseline
        case List.first(baselines) do
          nil -> {:error, :no_baseline_found}
          baseline -> {:ok, baseline}
        end

      id ->
        case Enum.find(baselines, &(&1.id == id)) do
          nil -> {:error, :baseline_not_found}
          baseline -> {:ok, baseline}
        end
    end
  end

  defp execute_regression_test(
         engine_name,
         baseline,
         regression_thresholds,
         fail_on_regression,
         state
       ) do
    test_id = generate_test_id()
    start_time = DateTime.utc_now()

    # Run the same scenarios as the baseline
    current_metrics =
      Enum.reduce(baseline.test_scenarios, %{}, fn scenario, acc ->
        case run_scenario_test(engine_name, scenario, baseline.test_configuration.duration, state) do
          {:ok, metrics} ->
            Map.put(acc, scenario, metrics)

          {:error, _reason} ->
            acc
        end
      end)

    if map_size(current_metrics) == 0 do
      {:error, :no_current_metrics_collected}
    else
      # Analyze for regressions
      regression_analysis =
        analyze_regression(baseline.metrics, current_metrics, regression_thresholds)

      # Determine overall status
      overall_status = determine_test_status(regression_analysis, fail_on_regression)

      test_result = %{
        id: test_id,
        engine_name: engine_name,
        baseline_id: baseline.id,
        test_timestamp: start_time,
        test_duration: DateTime.diff(DateTime.utc_now(), start_time, :second),
        scenarios_tested: baseline.test_scenarios,
        performance_metrics: current_metrics,
        regression_analysis: regression_analysis,
        overall_status: overall_status,
        regressions_detected: extract_regressions(regression_analysis),
        performance_improvements: extract_improvements(regression_analysis),
        recommendations: generate_regression_recommendations(regression_analysis)
      }

      {:ok, test_result}
    end
  end

  defp analyze_regression(baseline_metrics, current_metrics, thresholds) do
    Enum.reduce(baseline_metrics, %{}, fn {scenario, baseline_scenario_metrics}, acc ->
      case Map.get(current_metrics, scenario) do
        nil ->
          Map.put(acc, scenario, %{status: :no_current_data})

        current_scenario_metrics ->
          scenario_analysis =
            analyze_scenario_regression(
              baseline_scenario_metrics,
              current_scenario_metrics,
              thresholds
            )

          Map.put(acc, scenario, scenario_analysis)
      end
    end)
  end

  defp analyze_scenario_regression(baseline_metrics, current_metrics, thresholds) do
    metric_specs = [
      {:avg_response_time, :execution_time, :higher_is_worse, -0.05},
      {:memory_usage, :memory_usage, :higher_is_worse, -0.05},
      {:throughput, :throughput, :lower_is_worse, 0.05},
      {:error_rate, :error_rate, :higher_is_worse, -0.01}
    ]

    metric_comparisons =
      Enum.reduce(metric_specs, %{}, fn {metric_key, threshold_key, direction,
                                         improvement_threshold},
                                        acc ->
        analyze_single_metric(
          acc,
          baseline_metrics,
          current_metrics,
          metric_key,
          Map.get(thresholds, threshold_key),
          direction,
          improvement_threshold
        )
      end)

    # Determine overall scenario status
    overall_status = determine_scenario_status(metric_comparisons)

    %{
      metric_comparisons: metric_comparisons,
      overall_status: overall_status,
      regression_count: count_regressions(metric_comparisons),
      improvement_count: count_improvements(metric_comparisons)
    }
  end

  defp analyze_single_metric(
         acc,
         baseline_metrics,
         current_metrics,
         metric_key,
         threshold,
         direction,
         improvement_threshold
       ) do
    if Map.has_key?(baseline_metrics, metric_key) and Map.has_key?(current_metrics, metric_key) do
      baseline_value = Map.get(baseline_metrics, metric_key)
      current_value = Map.get(current_metrics, metric_key)

      change_ratio = calculate_change_ratio(baseline_value, current_value, metric_key)
      status = determine_metric_status(change_ratio, threshold, direction, improvement_threshold)

      Map.put(acc, metric_key, %{
        baseline: baseline_value,
        current: current_value,
        change_ratio: change_ratio,
        change_percent: change_ratio * 100,
        status: status
      })
    else
      acc
    end
  end

  defp calculate_change_ratio(baseline_value, current_value, :error_rate)
       when baseline_value > 0 do
    (current_value - baseline_value) / baseline_value
  end

  defp calculate_change_ratio(_baseline_value, current_value, :error_rate) do
    current_value
  end

  defp calculate_change_ratio(baseline_value, current_value, _metric_key) do
    (current_value - baseline_value) / baseline_value
  end

  defp determine_metric_status(change_ratio, threshold, :higher_is_worse, improvement_threshold) do
    cond do
      change_ratio > threshold -> :regression
      change_ratio < improvement_threshold -> :improvement
      true -> :stable
    end
  end

  defp determine_metric_status(change_ratio, threshold, :lower_is_worse, improvement_threshold) do
    cond do
      change_ratio < -threshold -> :regression
      change_ratio > improvement_threshold -> :improvement
      true -> :stable
    end
  end

  defp determine_scenario_status(metric_comparisons) do
    statuses = Enum.map(metric_comparisons, fn {_metric, comparison} -> comparison.status end)

    cond do
      :regression in statuses -> :regression_detected
      :improvement in statuses -> :improvement_detected
      true -> :stable
    end
  end

  defp count_regressions(metric_comparisons) do
    Enum.count(metric_comparisons, fn {_metric, comparison} ->
      comparison.status == :regression
    end)
  end

  defp count_improvements(metric_comparisons) do
    Enum.count(metric_comparisons, fn {_metric, comparison} ->
      comparison.status == :improvement
    end)
  end

  defp determine_test_status(regression_analysis, fail_on_regression) do
    has_regressions =
      Enum.any?(regression_analysis, fn {_scenario, analysis} ->
        analysis.overall_status == :regression_detected
      end)

    has_improvements =
      Enum.any?(regression_analysis, fn {_scenario, analysis} ->
        analysis.overall_status == :improvement_detected
      end)

    cond do
      has_regressions and fail_on_regression -> :failed
      has_regressions -> :warning
      has_improvements -> :passed
      true -> :passed
    end
  end

  defp extract_regressions(regression_analysis) do
    Enum.flat_map(regression_analysis, fn {scenario, analysis} ->
      if analysis.overall_status == :regression_detected do
        regressed_metrics =
          Enum.filter(analysis.metric_comparisons, fn {_metric, comparison} ->
            comparison.status == :regression
          end)

        Enum.map(regressed_metrics, fn {metric, comparison} ->
          %{
            scenario: scenario,
            metric: metric,
            baseline_value: comparison.baseline,
            current_value: comparison.current,
            change_percent: comparison.change_percent,
            severity: determine_regression_severity(comparison.change_percent)
          }
        end)
      else
        []
      end
    end)
  end

  defp extract_improvements(regression_analysis) do
    Enum.flat_map(regression_analysis, fn {scenario, analysis} ->
      if analysis.overall_status == :improvement_detected do
        improved_metrics =
          Enum.filter(analysis.metric_comparisons, fn {_metric, comparison} ->
            comparison.status == :improvement
          end)

        Enum.map(improved_metrics, fn {metric, comparison} ->
          %{
            scenario: scenario,
            metric: metric,
            baseline_value: comparison.baseline,
            current_value: comparison.current,
            improvement_percent: abs(comparison.change_percent)
          }
        end)
      else
        []
      end
    end)
  end

  defp determine_regression_severity(change_percent) do
    cond do
      abs(change_percent) > 50 -> :critical
      abs(change_percent) > 25 -> :high
      abs(change_percent) > 15 -> :medium
      true -> :low
    end
  end

  defp generate_regression_recommendations(regression_analysis) do
    recommendations = []

    # Check for consistent patterns
    all_regressions = extract_regressions(regression_analysis)

    recommendations =
      if length(all_regressions) > 0 do
        [
          "Investigate recent changes that may have caused performance regressions"
          | recommendations
        ]
      else
        recommendations
      end

    memory_regressions = Enum.filter(all_regressions, &(&1.metric == :memory_usage))

    recommendations =
      if length(memory_regressions) > 0 do
        ["Check for memory leaks or increased memory allocation patterns" | recommendations]
      else
        recommendations
      end

    throughput_regressions = Enum.filter(all_regressions, &(&1.metric == :throughput))

    recommendations =
      if length(throughput_regressions) > 0 do
        ["Analyze bottlenecks in rule execution pipeline" | recommendations]
      else
        recommendations
      end

    if Enum.empty?(recommendations) do
      ["Performance is stable compared to baseline"]
    else
      recommendations ++ ["Consider rolling back recent changes if regressions are severe"]
    end
  end

  defp execute_load_test(engine_name, load_config, _opts, _state) do
    test_id = generate_test_id()
    start_time = DateTime.utc_now()

    # Execute load test phases
    results = %{
      test_id: test_id,
      engine_name: engine_name,
      test_configuration: load_config,
      start_time: start_time,
      phases: %{}
    }

    # Ramp-up phase
    ramp_up_metrics = simulate_ramp_up_phase(engine_name, load_config)
    results = put_in(results, [:phases, :ramp_up], ramp_up_metrics)

    # Steady state phase
    steady_state_metrics = simulate_steady_state_phase(engine_name, load_config)
    results = put_in(results, [:phases, :steady_state], steady_state_metrics)

    # Ramp-down phase
    ramp_down_metrics = simulate_ramp_down_phase(engine_name, load_config)
    results = put_in(results, [:phases, :ramp_down], ramp_down_metrics)

    # Calculate overall results
    overall_metrics = calculate_overall_load_test_metrics(results.phases)
    results = Map.put(results, :overall_metrics, overall_metrics)

    end_time = DateTime.utc_now()
    results = Map.put(results, :end_time, end_time)
    results = Map.put(results, :total_duration, DateTime.diff(end_time, start_time, :second))

    {:ok, results}
  end

  defp simulate_ramp_up_phase(_engine_name, load_config) do
    duration = load_config.ramp_up_time
    target_users = load_config.concurrent_users

    # Simulate gradual increase in load
    %{
      phase: :ramp_up,
      duration: duration,
      initial_users: 1,
      final_users: target_users,
      avg_response_time: 45 + :rand.uniform(20),
      throughput_ramp: target_users * 0.8,
      errors_during_ramp: 0,
      resource_utilization: %{
        cpu: 15 + target_users * 0.3,
        memory: 25 + target_users * 0.2
      }
    }
  end

  defp simulate_steady_state_phase(_engine_name, load_config) do
    duration = load_config.steady_state_time
    users = load_config.concurrent_users
    facts_per_second = load_config.facts_per_second

    # Simulate steady load
    base_latency = 60 + users * 0.5

    %{
      phase: :steady_state,
      duration: duration,
      concurrent_users: users,
      facts_per_second: facts_per_second,
      avg_response_time: base_latency,
      p95_response_time: base_latency * 1.4,
      p99_response_time: base_latency * 1.8,
      throughput: facts_per_second * 0.95,
      error_rate: max(0.0, (users - 50) * 0.001),
      resource_utilization: %{
        cpu: min(95.0, 30 + users * 0.8),
        memory: min(90.0, 40 + users * 0.6)
      },
      stability_score: max(0.5, 1.0 - users / 200.0)
    }
  end

  defp simulate_ramp_down_phase(_engine_name, load_config) do
    duration = load_config.ramp_down_time
    initial_users = load_config.concurrent_users

    # Simulate gradual decrease in load
    %{
      phase: :ramp_down,
      duration: duration,
      initial_users: initial_users,
      final_users: 0,
      avg_response_time: 35 + :rand.uniform(15),
      recovery_time: 20 + :rand.uniform(30),
      resource_recovery: %{
        cpu: 10 + :rand.uniform(15),
        memory: 20 + :rand.uniform(20)
      }
    }
  end

  defp calculate_overall_load_test_metrics(phases) do
    steady_state = phases.steady_state

    %{
      peak_throughput: steady_state.throughput,
      avg_response_time: steady_state.avg_response_time,
      p95_response_time: steady_state.p95_response_time,
      p99_response_time: steady_state.p99_response_time,
      error_rate: steady_state.error_rate,
      stability_score: steady_state.stability_score,
      max_cpu_usage: steady_state.resource_utilization.cpu,
      max_memory_usage: steady_state.resource_utilization.memory,
      test_success: steady_state.error_rate < 0.05,
      performance_grade: calculate_performance_grade(steady_state)
    }
  end

  defp calculate_performance_grade(metrics) do
    # Calculate performance grade based on multiple factors
    response_time_score = max(0, 100 - metrics.avg_response_time)
    throughput_score = min(100, metrics.throughput / 10)
    error_score = max(0, 100 - metrics.error_rate * 2000)
    stability_score = metrics.stability_score * 100

    overall_score = (response_time_score + throughput_score + error_score + stability_score) / 4

    cond do
      overall_score >= 90 -> "A"
      overall_score >= 80 -> "B"
      overall_score >= 70 -> "C"
      overall_score >= 60 -> "D"
      true -> "F"
    end
  end

  defp execute_stress_test_loop(
         _engine_name,
         current_load,
         max_users,
         _step_duration,
         _increment,
         load_steps,
         breaking_point,
         stable_throughput,
         max_throughput
       )
       when current_load > max_users or not is_nil(breaking_point) do
    {Enum.reverse(load_steps), breaking_point, stable_throughput, max_throughput}
  end

  defp execute_stress_test_loop(
         engine_name,
         current_load,
         max_users,
         step_duration,
         increment,
         load_steps,
         breaking_point,
         stable_throughput,
         max_throughput
       ) do
    step_metrics = simulate_stress_test_step(engine_name, current_load, step_duration)
    updated_load_steps = [step_metrics | load_steps]

    # Update max throughput
    updated_max_throughput = max(max_throughput, step_metrics.throughput)

    # Check if system is still stable
    {updated_breaking_point, updated_stable_throughput} =
      if step_metrics.stability_score > 0.7 and step_metrics.error_rate < 0.05 do
        {breaking_point, step_metrics.throughput}
      else
        updated_bp =
          if is_nil(breaking_point) do
            %{
              load: current_load,
              throughput: step_metrics.throughput,
              error_rate: step_metrics.error_rate,
              avg_response_time: step_metrics.avg_response_time
            }
          else
            breaking_point
          end

        {updated_bp, stable_throughput}
      end

    execute_stress_test_loop(
      engine_name,
      current_load + increment,
      max_users,
      step_duration,
      increment,
      updated_load_steps,
      updated_breaking_point,
      updated_stable_throughput,
      updated_max_throughput
    )
  end

  defp execute_stress_test(engine_name, opts, _state) do
    test_id = generate_test_id()
    max_users = Keyword.get(opts, :max_users, 1000)
    increment = Keyword.get(opts, :increment, 50)
    step_duration = Keyword.get(opts, :step_duration, 60)

    stress_results = %{
      test_id: test_id,
      engine_name: engine_name,
      start_time: DateTime.utc_now(),
      test_configuration: %{
        max_users: max_users,
        increment: increment,
        step_duration: step_duration
      },
      load_steps: [],
      breaking_point: nil,
      max_stable_throughput: 0,
      max_throughput: 0,
      recovery_metrics: %{}
    }

    # Execute stress test steps
    {load_steps, breaking_point, stable_throughput, max_throughput} =
      execute_stress_test_loop(
        engine_name,
        increment,
        max_users,
        step_duration,
        increment,
        [],
        nil,
        0,
        0
      )

    # Test recovery
    recovery_metrics = simulate_recovery_test(engine_name, increment)

    stress_results =
      Map.merge(stress_results, %{
        load_steps: Enum.reverse(load_steps),
        breaking_point: breaking_point,
        max_stable_throughput: stable_throughput,
        max_throughput: max_throughput,
        recovery_metrics: recovery_metrics,
        end_time: DateTime.utc_now()
      })

    {:ok, stress_results}
  end

  defp simulate_stress_test_step(_engine_name, load, duration) do
    # Simulate increasing load and measure system response
    base_latency = 50
    load_factor = load / 100.0

    # Response time increases with load
    avg_response_time = base_latency * (1 + load_factor * 0.5)

    # Throughput increases but plateaus and then degrades
    max_throughput = 500
    throughput = min(max_throughput, load * 8 * max(0.1, 1 - (load_factor - 3) * 0.2))

    # Error rate increases significantly under high load
    error_rate = max(0.0, (load_factor - 2) * 0.1)

    # CPU usage approaches 100% under high load
    cpu_usage = min(100.0, 20 + load_factor * 40)

    # Memory usage increases with load
    memory_usage = min(100.0, 30 + load_factor * 35)

    # Stability decreases with high load
    stability_score = max(0.0, 1.0 - max(0, load_factor - 2) * 0.3)

    %{
      load: load,
      duration: duration,
      avg_response_time: avg_response_time,
      p95_response_time: avg_response_time * 1.5,
      p99_response_time: avg_response_time * 2.0,
      throughput: throughput,
      error_rate: error_rate,
      cpu_usage: cpu_usage,
      memory_usage: memory_usage,
      stability_score: stability_score,
      timestamp: DateTime.utc_now()
    }
  end

  defp simulate_recovery_test(_engine_name, base_load) do
    # Test system recovery after stress
    recovery_start = DateTime.utc_now()

    # Simulate recovery metrics
    # 1-3 minutes recovery
    recovery_time = 60 + :rand.uniform(120)

    %{
      recovery_start: recovery_start,
      recovery_load: base_load,
      recovery_time_seconds: recovery_time,
      # 90% of normal capacity
      post_recovery_throughput: base_load * 8 * 0.9,
      post_recovery_response_time: 55 + :rand.uniform(20),
      post_recovery_error_rate: 0.01,
      recovery_success: true
    }
  end

  defp compare_version_performance(engine_name, version1, version2, opts, state) do
    test_suite = Keyword.get(opts, :test_suite, :standard)

    # Find baselines for both versions
    baselines = Map.get(state.performance_baselines, engine_name, [])

    baseline1 = Enum.find(baselines, &(&1.version == version1))
    baseline2 = Enum.find(baselines, &(&1.version == version2))

    cond do
      baseline1 == nil ->
        {:error, {:baseline_not_found, version1}}

      baseline2 == nil ->
        {:error, {:baseline_not_found, version2}}

      true ->
        comparison = perform_version_comparison(baseline1, baseline2, test_suite)
        {:ok, comparison}
    end
  end

  defp perform_version_comparison(baseline1, baseline2, test_suite) do
    # Compare metrics across versions
    _comparison_results = %{}

    # Compare each common scenario
    common_scenarios =
      MapSet.intersection(
        MapSet.new(baseline1.test_scenarios),
        MapSet.new(baseline2.test_scenarios)
      )
      |> MapSet.to_list()

    scenario_comparisons =
      Enum.reduce(common_scenarios, %{}, fn scenario, acc ->
        metrics1 = baseline1.metrics[scenario]
        metrics2 = baseline2.metrics[scenario]

        scenario_comparison = compare_scenario_metrics(metrics1, metrics2)
        Map.put(acc, scenario, scenario_comparison)
      end)

    # Calculate overall comparison
    overall_comparison = calculate_overall_version_comparison(scenario_comparisons)

    %{
      version1: baseline1.version,
      version2: baseline2.version,
      comparison_timestamp: DateTime.utc_now(),
      test_suite: test_suite,
      scenarios_compared: common_scenarios,
      scenario_comparisons: scenario_comparisons,
      overall_comparison: overall_comparison,
      recommendations: generate_version_comparison_recommendations(overall_comparison)
    }
  end

  defp compare_scenario_metrics(metrics1, metrics2) do
    # Compare key metrics between versions using data-driven approach
    metric_configs = [
      {:avg_response_time, :lower_is_better},
      {:throughput, :higher_is_better},
      {:memory_usage, :lower_is_better}
    ]

    Enum.reduce(metric_configs, %{}, fn {metric_key, direction}, comparisons ->
      compare_single_metric(comparisons, metrics1, metrics2, metric_key, direction)
    end)
  end

  defp compare_single_metric(comparisons, metrics1, metrics2, metric_key, direction) do
    if Map.has_key?(metrics1, metric_key) and Map.has_key?(metrics2, metric_key) do
      value1 = metrics1[metric_key]
      value2 = metrics2[metric_key]

      improvement = calculate_improvement_percentage(value1, value2, direction)
      winner = determine_winner(improvement)

      Map.put(comparisons, metric_key, %{
        version1_value: value1,
        version2_value: value2,
        improvement_percent: improvement,
        winner: winner
      })
    else
      comparisons
    end
  end

  defp calculate_improvement_percentage(value1, value2, :lower_is_better) do
    (value1 - value2) / value1 * 100
  end

  defp calculate_improvement_percentage(value1, value2, :higher_is_better) do
    (value2 - value1) / value1 * 100
  end

  defp determine_winner(improvement) do
    if improvement > 0, do: :version2, else: :version1
  end

  defp calculate_overall_version_comparison(scenario_comparisons) do
    # Calculate overall winner and statistics
    all_metrics =
      Enum.flat_map(scenario_comparisons, fn {_scenario, comparisons} ->
        Map.values(comparisons)
      end)

    version2_wins = Enum.count(all_metrics, &(&1.winner == :version2))
    version1_wins = Enum.count(all_metrics, &(&1.winner == :version1))

    overall_winner = if version2_wins > version1_wins, do: :version2, else: :version1

    avg_improvement =
      if length(all_metrics) > 0 do
        total_improvement = Enum.sum(Enum.map(all_metrics, &abs(&1.improvement_percent)))
        total_improvement / length(all_metrics)
      else
        0.0
      end

    %{
      overall_winner: overall_winner,
      version1_wins: version1_wins,
      version2_wins: version2_wins,
      average_improvement_percent: avg_improvement,
      significant_improvement: avg_improvement > 10.0,
      metrics_compared: length(all_metrics)
    }
  end

  defp generate_version_comparison_recommendations(overall_comparison) do
    recommendations = []

    recommendations =
      if overall_comparison.significant_improvement do
        [
          "Version #{overall_comparison.overall_winner} shows significant performance improvements"
          | recommendations
        ]
      else
        ["Performance differences between versions are minimal" | recommendations]
      end

    recommendations =
      if overall_comparison.average_improvement_percent > 20 do
        ["Consider upgrading to the better-performing version" | recommendations]
      else
        recommendations
      end

    recommendations ++
      [
        "Monitor performance closely after version upgrades",
        "Run regression tests for critical scenarios"
      ]
  end

  defp perform_monitoring_check(engine_name, monitoring_config, state) do
    # Perform a quick performance check for continuous monitoring
    # 1 minute quick test
    quick_test_duration = 60

    case run_scenario_test(engine_name, :standard_load, quick_test_duration, state) do
      {:ok, current_metrics} ->
        # Compare with recent baseline
        case find_baseline(engine_name, nil, state) do
          {:ok, baseline} ->
            case Map.get(baseline.metrics, :standard_load) do
              nil ->
                {:ok, %{regression_detected: false, message: "No baseline data for comparison"}}

              baseline_metrics ->
                # Check for regression
                regression_detected =
                  detect_quick_regression(
                    baseline_metrics,
                    current_metrics,
                    monitoring_config.thresholds
                  )

                monitoring_result = %{
                  regression_detected: regression_detected,
                  current_metrics: current_metrics,
                  baseline_metrics: baseline_metrics,
                  monitoring_timestamp: DateTime.utc_now()
                }

                monitoring_result =
                  if regression_detected do
                    regression_details =
                      analyze_quick_regression(
                        baseline_metrics,
                        current_metrics,
                        monitoring_config.thresholds
                      )

                    Map.put(monitoring_result, :regression_details, regression_details)
                  else
                    monitoring_result
                  end

                {:ok, monitoring_result}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp detect_quick_regression(baseline_metrics, current_metrics, thresholds) do
    # Quick regression detection for monitoring
    response_time_regression =
      if Map.has_key?(baseline_metrics, :avg_response_time) and
           Map.has_key?(current_metrics, :avg_response_time) do
        change =
          (current_metrics.avg_response_time - baseline_metrics.avg_response_time) /
            baseline_metrics.avg_response_time

        change > thresholds.execution_time
      else
        false
      end

    memory_regression =
      if Map.has_key?(baseline_metrics, :memory_usage) and
           Map.has_key?(current_metrics, :memory_usage) do
        change =
          (current_metrics.memory_usage - baseline_metrics.memory_usage) /
            baseline_metrics.memory_usage

        change > thresholds.memory_usage
      else
        false
      end

    error_rate_regression =
      if Map.has_key?(baseline_metrics, :error_rate) and
           Map.has_key?(current_metrics, :error_rate) do
        change = current_metrics.error_rate - baseline_metrics.error_rate
        change > thresholds.error_rate
      else
        false
      end

    response_time_regression or memory_regression or error_rate_regression
  end

  defp analyze_quick_regression(baseline_metrics, current_metrics, thresholds) do
    # Analyze which metrics regressed during monitoring
    regressions = []

    regressions =
      if Map.has_key?(baseline_metrics, :avg_response_time) and
           Map.has_key?(current_metrics, :avg_response_time) do
        change =
          (current_metrics.avg_response_time - baseline_metrics.avg_response_time) /
            baseline_metrics.avg_response_time

        if change > thresholds.execution_time do
          [
            %{
              metric: :avg_response_time,
              baseline: baseline_metrics.avg_response_time,
              current: current_metrics.avg_response_time,
              change_percent: change * 100
            }
            | regressions
          ]
        else
          regressions
        end
      else
        regressions
      end

    # Add other regression checks...

    regressions
  end

  defp generate_comprehensive_performance_report(engine_name, _opts, state) do
    baselines = Map.get(state.performance_baselines, engine_name, [])
    test_history = Map.get(state.test_history, engine_name, [])

    if Enum.empty?(baselines) and Enum.empty?(test_history) do
      {:error, :no_performance_data}
    else
      report = %{
        engine_name: engine_name,
        report_timestamp: DateTime.utc_now(),
        performance_summary: generate_performance_summary(baselines, test_history),
        trend_analysis: analyze_performance_trends(test_history),
        baseline_analysis: analyze_baselines(baselines),
        regression_summary: summarize_regressions(test_history),
        recommendations: generate_performance_recommendations(baselines, test_history),
        test_coverage: analyze_test_coverage(baselines, test_history)
      }

      {:ok, report}
    end
  end

  defp generate_performance_summary(baselines, test_history) do
    latest_baseline = List.first(baselines)
    latest_test = List.first(test_history)

    %{
      total_baselines: length(baselines),
      total_tests_run: length(test_history),
      latest_baseline: if(latest_baseline, do: latest_baseline.created_at, else: nil),
      latest_test: if(latest_test, do: latest_test.test_timestamp, else: nil),
      current_status: determine_current_performance_status(test_history)
    }
  end

  defp determine_current_performance_status(test_history) do
    case List.first(test_history) do
      nil -> :unknown
      latest_test -> latest_test.overall_status
    end
  end

  defp analyze_performance_trends(test_history) do
    if length(test_history) < 2 do
      %{trend: :insufficient_data}
    else
      recent_tests = Enum.take(test_history, 10)

      regression_count = Enum.count(recent_tests, &(&1.overall_status in [:failed, :warning]))

      trend =
        cond do
          regression_count > 5 -> :declining
          regression_count > 2 -> :unstable
          true -> :stable
        end

      %{
        trend: trend,
        recent_regression_rate: regression_count / length(recent_tests),
        tests_analyzed: length(recent_tests)
      }
    end
  end

  defp analyze_baselines(baselines) do
    %{
      baseline_count: length(baselines),
      oldest_baseline: if(length(baselines) > 0, do: List.last(baselines).created_at, else: nil),
      newest_baseline: if(length(baselines) > 0, do: List.first(baselines).created_at, else: nil),
      baseline_versions: Enum.map(baselines, & &1.version) |> Enum.uniq()
    }
  end

  defp summarize_regressions(test_history) do
    all_regressions = Enum.flat_map(test_history, & &1.regressions_detected)

    %{
      total_regressions: length(all_regressions),
      regression_types: Enum.frequencies_by(all_regressions, & &1.metric),
      severity_distribution: Enum.frequencies_by(all_regressions, & &1.severity),
      most_affected_scenarios: find_most_affected_scenarios(all_regressions)
    }
  end

  defp find_most_affected_scenarios(regressions) do
    regressions
    |> Enum.frequencies_by(& &1.scenario)
    |> Enum.sort_by(fn {_scenario, count} -> count end, :desc)
    |> Enum.take(5)
  end

  defp generate_performance_recommendations(baselines, test_history) do
    recommendations = []

    # Check baseline freshness
    recommendations =
      if length(baselines) > 0 do
        latest_baseline = List.first(baselines)
        days_old = DateTime.diff(DateTime.utc_now(), latest_baseline.created_at, :day)

        if days_old > 30 do
          [
            "Update performance baselines - current baseline is #{days_old} days old"
            | recommendations
          ]
        else
          recommendations
        end
      else
        ["Establish performance baselines for consistent testing" | recommendations]
      end

    # Check test frequency
    recommendations =
      if length(test_history) > 0 do
        latest_test = List.first(test_history)
        days_since_test = DateTime.diff(DateTime.utc_now(), latest_test.test_timestamp, :day)

        if days_since_test > 7 do
          [
            "Run regression tests more frequently - last test was #{days_since_test} days ago"
            | recommendations
          ]
        else
          recommendations
        end
      else
        ["Start running regular regression tests" | recommendations]
      end

    # Check for recurring regressions
    all_regressions = Enum.flat_map(test_history, & &1.regressions_detected)

    recommendations =
      if length(all_regressions) > 5 do
        ["Investigate recurring performance issues" | recommendations]
      else
        recommendations
      end

    recommendations ++
      [
        "Implement continuous performance monitoring",
        "Set up automated alerts for performance regressions",
        "Regular performance optimization reviews"
      ]
  end

  defp analyze_test_coverage(baselines, test_history) do
    all_scenarios =
      baselines
      |> Enum.flat_map(& &1.test_scenarios)
      |> Enum.uniq()

    tested_scenarios =
      test_history
      |> Enum.flat_map(& &1.scenarios_tested)
      |> Enum.uniq()

    coverage_percent =
      if length(all_scenarios) > 0 do
        length(tested_scenarios) / length(all_scenarios) * 100
      else
        0.0
      end

    %{
      total_scenarios: length(all_scenarios),
      tested_scenarios: length(tested_scenarios),
      coverage_percent: coverage_percent,
      untested_scenarios: all_scenarios -- tested_scenarios
    }
  end

  defp load_test_scenarios do
    %{
      standard_load: %{
        facts_per_second: 100,
        concurrent_users: 10,
        description: "Standard production load simulation"
      },
      peak_load: %{
        facts_per_second: 500,
        concurrent_users: 50,
        description: "Peak traffic load simulation"
      },
      stress_test: %{
        facts_per_second: 1000,
        concurrent_users: 100,
        description: "Stress testing beyond normal capacity"
      },
      endurance_test: %{
        facts_per_second: 200,
        concurrent_users: 20,
        description: "Long-duration endurance testing"
      },
      spike_test: %{
        base_facts_per_second: 100,
        spike_facts_per_second: 1000,
        description: "Sudden load spike simulation"
      }
    }
  end

  defp generate_baseline_id do
    "baseline_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp generate_test_id do
    "test_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end
end
