defmodule Presto.LoggerTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Presto.Logger, as: PrestoLogger

  setup do
    # Enable Presto logging for tests
    original_config = Application.get_env(:presto, :logging, [])
    Application.put_env(:presto, :logging, enabled: true, level: :debug)

    on_exit(fn ->
      Application.put_env(:presto, :logging, original_config)
    end)
  end

  describe "log_rule_execution/4" do
    test "logs rule execution with structured metadata" do
      log =
        capture_log(fn ->
          PrestoLogger.log_rule_execution(:info, :test_rule, "rule_fired", %{facts_matched: 3})
        end)

      assert log =~ "Rule execution: rule_fired"
    end
  end

  describe "log_fact_processing/4" do
    test "logs fact processing events" do
      log =
        capture_log(fn ->
          PrestoLogger.log_fact_processing(:debug, :person, "fact_added", %{fact_count: 10})
        end)

      assert log =~ "Fact processing: fact_added"
    end
  end

  describe "log_network_operation/4" do
    test "logs network operations" do
      log =
        capture_log(fn ->
          PrestoLogger.log_network_operation(:info, "alpha", "node_created", %{node_id: "alpha_1"})
        end)

      assert log =~ "Network operation: node_created on alpha"
    end
  end

  describe "log_performance/4" do
    test "logs performance metrics" do
      log =
        capture_log(fn ->
          PrestoLogger.log_performance(:info, "rule_compilation", 150, %{rule_count: 5})
        end)

      assert log =~ "Performance: rule_compilation completed in 150ms"
    end
  end

  describe "log_engine_lifecycle/4" do
    test "logs engine lifecycle events" do
      log =
        capture_log(fn ->
          PrestoLogger.log_engine_lifecycle(:info, "engine_123", "started", %{pid: self()})
        end)

      assert log =~ "Engine lifecycle: started"
    end
  end

  describe "log_configuration/4" do
    test "logs configuration events" do
      log =
        capture_log(fn ->
          PrestoLogger.log_configuration(:warning, "rule_timeout", "value_changed", %{
            old: 5000,
            new: 10_000
          })
        end)

      assert log =~ "Configuration: value_changed for rule_timeout"
    end
  end

  describe "log_error/2" do
    test "logs errors with structured context" do
      error = %RuntimeError{message: "Something went wrong"}
      context = %{operation: "rule_execution", rule_id: :test_rule}

      log =
        capture_log(fn ->
          PrestoLogger.log_error(error, context)
        end)

      assert log =~ "Error occurred: Something went wrong"
    end
  end

  describe "log_debug/3" do
    test "logs debug information with trace ID" do
      trace_id = "abc123"

      log =
        capture_log(fn ->
          PrestoLogger.log_debug("Debug message", trace_id, %{step: 1})
        end)

      assert log =~ "[abc123] Debug message"
    end
  end

  describe "generate_trace_id/0" do
    test "generates unique trace IDs" do
      id1 = PrestoLogger.generate_trace_id()
      id2 = PrestoLogger.generate_trace_id()

      assert is_binary(id1)
      assert is_binary(id2)
      assert id1 != id2
      # 8 bytes * 2 hex chars per byte
      assert String.length(id1) == 16
    end
  end

  describe "log_with_timing/4" do
    test "logs successful operation with timing" do
      log =
        capture_log(fn ->
          result =
            PrestoLogger.log_with_timing(
              :info,
              "test_operation",
              fn ->
                Process.sleep(10)
                :success
              end,
              %{test: true}
            )

          assert result == :success
        end)

      assert log =~ "Performance: test_operation completed in"
      assert log =~ "ms"
    end

    test "logs failed operation with timing and re-raises error" do
      assert_raise RuntimeError, "test error", fn ->
        capture_log(fn ->
          PrestoLogger.log_with_timing(:info, "failing_operation", fn ->
            raise "test error"
          end)
        end)
      end
    end
  end

  describe "log_rule_compilation/4" do
    test "logs rule compilation stages" do
      log =
        capture_log(fn ->
          PrestoLogger.log_rule_compilation(:info, :my_rule, "parsing", %{line_count: 10})
        end)

      assert log =~ "Rule compilation: parsing for rule my_rule"
    end
  end

  describe "log_memory_stats/3" do
    test "logs working memory statistics" do
      stats = %{fact_count: 100, memory_usage_bytes: 1024}

      log =
        capture_log(fn ->
          PrestoLogger.log_memory_stats(:info, stats, %{engine_id: "test_engine"})
        end)

      assert log =~ "Working memory stats: 100 facts, 1024 bytes"
    end
  end

  describe "logging levels" do
    test "respects different log levels" do
      # Test that different levels work
      for level <- [:debug, :info, :warning, :error] do
        log =
          capture_log(fn ->
            PrestoLogger.log_rule_execution(level, :test_rule, "test_event")
          end)

        # Should contain the message regardless of level in test environment
        assert log =~ "Rule execution: test_event"
      end
    end
  end
end
