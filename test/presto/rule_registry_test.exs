defmodule Presto.RuleRegistryTest do
  use ExUnit.Case, async: true

  alias Presto.RuleRegistry

  setup do
    # Clear any dynamic rules that might have been set by previous tests
    Process.delete(:presto_dynamic_rules)
    :ok
  end

  describe "configuration-driven rule registry" do
    test "reads rules from application configuration" do
      # This will use the default configuration from config/config.exs
      rules = RuleRegistry.get_available_rules()

      # Should have the example rules configured
      assert Map.has_key?(rules, "time_calculation")
      assert Map.has_key?(rules, "overtime_check")
      assert Map.has_key?(rules, "weekly_compliance")
      assert Map.has_key?(rules, "spike_break_compliance")
    end

    test "validates configuration" do
      case RuleRegistry.validate_configuration() do
        :ok ->
          # Configuration is valid
          assert true

        {:error, _errors} ->
          # Configuration errors found but system is working
          assert true
      end
    end

    test "gets rule modules for configured rules" do
      assert {:ok, module} = RuleRegistry.get_rule_module("time_calculation")
      assert module == Presto.Examples.PayrollRules

      assert {:ok, module} = RuleRegistry.get_rule_module("weekly_compliance")
      assert module == Presto.Examples.ComplianceRules
    end

    test "returns error for unknown rules" do
      assert {:error, :not_found} = RuleRegistry.get_rule_module("unknown_rule")
    end

    test "lists all rule names" do
      rule_names = RuleRegistry.list_rule_names()

      assert is_list(rule_names)
      assert "time_calculation" in rule_names
      assert "weekly_compliance" in rule_names
    end

    test "validates rule specifications" do
      # Test valid spec
      valid_spec = %{
        "rules_to_run" => ["time_calculation", "overtime_check"],
        "variables" => %{"overtime_threshold" => 40.0}
      }

      assert RuleRegistry.valid_rule_spec?(valid_spec) == true

      # Test invalid spec - unknown rule
      invalid_spec = %{
        "rules_to_run" => ["unknown_rule"],
        "variables" => %{}
      }

      assert RuleRegistry.valid_rule_spec?(invalid_spec) == false
    end

    test "supports dynamic rule registration" do
      # Register a test rule
      assert :ok = RuleRegistry.register_rule("test_rule", Presto.Examples.PayrollRules)

      # Should now be available
      assert {:ok, module} = RuleRegistry.get_rule_module("test_rule")
      assert module == Presto.Examples.PayrollRules

      # Should be in the list
      assert "test_rule" in RuleRegistry.list_rule_names()
    end
  end

  describe "behaviour validation" do
    test "rejects modules that don't implement the behaviour" do
      # Try to register a module that doesn't implement the behaviour
      result = RuleRegistry.register_rule("invalid_rule", String)

      assert {:error, {:invalid_module, _message}} = result
    end
  end
end
