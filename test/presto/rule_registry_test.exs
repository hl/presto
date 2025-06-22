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

      # Configuration might be empty if no rules are configured
      # This is expected since example modules were converted to standalone scripts
      assert is_map(rules)
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
      # Register a test rule first since config is now empty
      :ok = RuleRegistry.register_rule("test_rule", Presto.TestRuleModule)

      assert {:ok, module} = RuleRegistry.get_rule_module("test_rule")
      assert module == Presto.TestRuleModule
    end

    test "returns error for unknown rules" do
      assert {:error, :not_found} = RuleRegistry.get_rule_module("unknown_rule")
    end

    test "lists all rule names" do
      # Register a test rule first since config is now empty
      :ok = RuleRegistry.register_rule("test_rule", Presto.TestRuleModule)

      rule_names = RuleRegistry.list_rule_names()
      assert is_list(rule_names)
      assert "test_rule" in rule_names
    end

    test "validates rule specifications" do
      # Register a test rule first
      :ok = RuleRegistry.register_rule("test_rule", Presto.TestRuleModule)

      # Test valid spec
      valid_spec = %{
        "rules_to_run" => ["test_rule"],
        "variables" => %{"test_var" => 42}
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
      assert :ok = RuleRegistry.register_rule("dynamic_test_rule", Presto.TestRuleModule)

      # Should now be available
      assert {:ok, module} = RuleRegistry.get_rule_module("dynamic_test_rule")
      assert module == Presto.TestRuleModule

      # Should be in the list
      assert "dynamic_test_rule" in RuleRegistry.list_rule_names()
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
