defmodule Presto.ExceptionsTest do
  use ExUnit.Case, async: true

  alias Presto.Exceptions.{
    ConfigurationError,
    EngineStateError,
    FactValidationError,
    NetworkError,
    RuleCompilationError,
    RuleExecutionError
  }

  describe "RuleCompilationError" do
    test "creates exception with default message" do
      exception =
        RuleCompilationError.exception(rule_id: :test_rule, compilation_stage: "validation")

      assert exception.rule_id == :test_rule
      assert exception.compilation_stage == "validation"

      assert exception.message =~
               "Rule compilation failed for rule 'test_rule' at stage 'validation'"
    end

    test "creates exception with custom message" do
      exception =
        RuleCompilationError.exception(
          rule_id: :test_rule,
          compilation_stage: "parsing",
          message: "Custom error message",
          details: %{line: 42}
        )

      assert exception.message == "Custom error message"
      assert exception.details == %{line: 42}
    end
  end

  describe "FactValidationError" do
    test "creates exception with validation errors" do
      fact = {:person, "john", %{age: "invalid"}}
      validation_errors = ["age must be a number"]

      exception =
        FactValidationError.exception(
          fact: fact,
          validation_errors: validation_errors,
          fact_type: "person"
        )

      assert exception.fact == fact
      assert exception.validation_errors == validation_errors
      assert exception.fact_type == "person"
      assert exception.message =~ "Fact validation failed for person"
    end
  end

  describe "RuleExecutionError" do
    test "creates exception with execution context" do
      original_error = %RuntimeError{message: "Division by zero"}
      context = %{rule_conditions: 3, matched_facts: 5}

      exception =
        RuleExecutionError.exception(
          rule_id: :calculation_rule,
          execution_context: context,
          original_error: original_error
        )

      assert exception.rule_id == :calculation_rule
      assert exception.execution_context == context
      assert exception.original_error == original_error
      assert exception.message =~ "Rule execution failed for rule 'calculation_rule'"
    end
  end

  describe "EngineStateError" do
    test "creates exception with state information" do
      exception =
        EngineStateError.exception(
          current_state: "stopped",
          requested_operation: "fire_rules",
          engine_id: "engine_123"
        )

      assert exception.current_state == "stopped"
      assert exception.requested_operation == "fire_rules"
      assert exception.engine_id == "engine_123"

      assert exception.message =~
               "Engine engine_123 in state 'stopped' cannot perform 'fire_rules'"
    end
  end

  describe "ConfigurationError" do
    test "creates exception with configuration details" do
      validation_errors = ["timeout must be positive", "max_rules is required"]

      exception =
        ConfigurationError.exception(
          config_key: "rule_engine.timeout",
          config_value: -1,
          validation_errors: validation_errors
        )

      assert exception.config_key == "rule_engine.timeout"
      assert exception.config_value == -1
      assert exception.validation_errors == validation_errors
      assert exception.message =~ "Configuration error for 'rule_engine.timeout'"
    end
  end

  describe "NetworkError" do
    test "creates exception with network details" do
      details = %{node_count: 5, memory_usage: "high"}

      exception =
        NetworkError.exception(
          network_type: "alpha",
          node_id: "alpha_node_42",
          operation: "pattern_matching",
          details: details
        )

      assert exception.network_type == "alpha"
      assert exception.node_id == "alpha_node_42"
      assert exception.operation == "pattern_matching"
      assert exception.details == details

      assert exception.message =~
               "Network error in alpha network, node 'alpha_node_42' during 'pattern_matching'"
    end
  end

  describe "exception raising and catching" do
    test "can raise and catch RuleCompilationError" do
      assert_raise RuleCompilationError, ~r/compilation failed/, fn ->
        raise RuleCompilationError, rule_id: :bad_rule, compilation_stage: "syntax_check"
      end
    end

    test "can pattern match on exception types" do
      try do
        raise FactValidationError, fact: {:invalid}, validation_errors: ["bad fact"]
      rescue
        error in FactValidationError ->
          assert error.fact == {:invalid}
          assert error.validation_errors == ["bad fact"]
      end
    end
  end
end
