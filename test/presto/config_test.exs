defmodule Presto.ConfigTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Presto.Config
  alias Presto.Exceptions.ConfigurationError

  describe "validate_engine_config/1" do
    test "validates valid engine configuration" do
      config = %{
        max_rules: 500,
        rule_timeout_ms: 3000,
        enable_concurrent_execution: true,
        max_concurrent_rules: 5,
        working_memory_limit: 50_000
      }

      assert Config.validate_engine_config(config) == :ok
    end

    test "rejects invalid max_rules" do
      config = %{max_rules: -1}
      
      assert {:error, errors} = Config.validate_engine_config(config)
      assert Enum.any?(errors, &String.contains?(&1, "max_rules failed validation"))
    end

    test "rejects invalid rule_timeout_ms" do
      config = %{rule_timeout_ms: 70_000}  # Too high
      
      assert {:error, errors} = Config.validate_engine_config(config)
      assert Enum.any?(errors, &String.contains?(&1, "rule_timeout_ms failed validation"))
    end

    test "accepts configuration with defaults" do
      config = %{}  # Empty config should use defaults
      
      assert Config.validate_engine_config(config) == :ok
    end

    test "rejects wrong types" do
      config = %{enable_concurrent_execution: "true"}  # Should be boolean
      
      assert {:error, errors} = Config.validate_engine_config(config)
      assert Enum.any?(errors, &String.contains?(&1, "enable_concurrent_execution must be of type boolean"))
    end
  end

  describe "validate_rule_registry_config/1" do
    test "validates valid rule registry configuration" do
      config = %{
        default_rules: [Presto.Examples.PayrollRules],
        rule_cache_size: 2000,
        enable_rule_hot_reload: true
      }

      assert Config.validate_rule_registry_config(config) == :ok
    end

    test "rejects invalid rule_cache_size" do
      config = %{rule_cache_size: 0}
      
      assert {:error, errors} = Config.validate_rule_registry_config(config)
      assert Enum.any?(errors, &String.contains?(&1, "rule_cache_size failed validation"))
    end

    test "validates default_rules as list" do
      config = %{default_rules: "not a list"}
      
      assert {:error, errors} = Config.validate_rule_registry_config(config)
      assert Enum.any?(errors, &String.contains?(&1, "default_rules must be of type list"))
    end
  end

  describe "validate_performance_config/1" do
    test "validates valid performance configuration" do
      config = %{
        enable_metrics: true,
        metrics_interval_ms: 30_000,
        enable_profiling: false
      }

      assert Config.validate_performance_config(config) == :ok
    end

    test "rejects invalid metrics_interval_ms" do
      config = %{metrics_interval_ms: 500}  # Too low
      
      assert {:error, errors} = Config.validate_performance_config(config)
      assert Enum.any?(errors, &String.contains?(&1, "metrics_interval_ms failed validation"))
    end
  end

  describe "validate_presto_config/1" do
    test "validates complete configuration" do
      config = %{
        engine: %{max_rules: 100},
        rule_registry: %{rule_cache_size: 500},
        performance: %{enable_metrics: true}
      }

      assert Config.validate_presto_config(config) == :ok
    end

    test "validates with missing sections using defaults" do
      config = %{engine: %{max_rules: 100}}
      
      assert Config.validate_presto_config(config) == :ok
    end

    test "fails when any section is invalid" do
      config = %{
        engine: %{max_rules: -1},  # Invalid
        rule_registry: %{rule_cache_size: 500}
      }

      assert {:error, _errors} = Config.validate_presto_config(config)
    end
  end

  describe "get_engine_config/0" do
    test "returns validated configuration with defaults" do
      # Mock the application environment
      Application.put_env(:presto, :engine, %{max_rules: 200})
      
      config = Config.get_engine_config()
      
      assert config.max_rules == 200
      assert config.rule_timeout_ms == 5000  # Default value
      assert is_boolean(config.enable_concurrent_execution)
      
      # Cleanup
      Application.delete_env(:presto, :engine)
    end

    test "raises ConfigurationError for invalid configuration" do
      # Set invalid configuration
      Application.put_env(:presto, :engine, %{max_rules: -1})
      
      assert_raise ConfigurationError, fn ->
        Config.get_engine_config()
      end
      
      # Cleanup
      Application.delete_env(:presto, :engine)
    end

    test "logs configuration loading" do
      Application.put_env(:presto, :engine, %{})
      
      log = capture_log(fn ->
        Config.get_engine_config()
      end)
      
      assert log =~ "Configuration: config_loaded for engine"
      
      # Cleanup
      Application.delete_env(:presto, :engine)
    end
  end

  describe "get_rule_registry_config/0" do
    test "returns validated configuration with defaults" do
      Application.put_env(:presto, :rule_registry, %{rule_cache_size: 1500})
      
      config = Config.get_rule_registry_config()
      
      assert config.rule_cache_size == 1500
      assert config.default_rules == []  # Default value
      assert is_boolean(config.enable_rule_hot_reload)
      
      # Cleanup
      Application.delete_env(:presto, :rule_registry)
    end
  end

  describe "get_performance_config/0" do
    test "returns validated configuration with defaults" do
      Application.put_env(:presto, :performance, %{enable_profiling: true})
      
      config = Config.get_performance_config()
      
      assert config.enable_profiling == true
      assert config.enable_metrics == true  # Default value
      assert config.metrics_interval_ms == 60_000  # Default value
      
      # Cleanup
      Application.delete_env(:presto, :performance)
    end
  end

  describe "validate_rule_spec/1" do
    test "validates valid rule specification" do
      rule_spec = %{
        "rules_to_run" => ["rule1", "rule2"],
        "variables" => %{"threshold" => 100},
        "rule_execution_order" => ["rule1", "rule2"]
      }

      assert Config.validate_rule_spec(rule_spec) == :ok
    end

    test "validates minimal rule specification" do
      rule_spec = %{}
      
      assert Config.validate_rule_spec(rule_spec) == :ok
    end

    test "rejects invalid rules_to_run" do
      rule_spec = %{"rules_to_run" => ["rule1", 123]}  # Mixed types
      
      assert {:error, errors} = Config.validate_rule_spec(rule_spec)
      assert Enum.any?(errors, &String.contains?(&1, "rules_to_run must contain only strings"))
    end

    test "rejects invalid variables" do
      rule_spec = %{"variables" => "not a map"}
      
      assert {:error, errors} = Config.validate_rule_spec(rule_spec)
      assert Enum.any?(errors, &String.contains?(&1, "variables must be a map"))
    end

    test "rejects invalid rule_execution_order" do
      rule_spec = %{"rule_execution_order" => [1, 2, 3]}  # Should be strings
      
      assert {:error, errors} = Config.validate_rule_spec(rule_spec)
      assert Enum.any?(errors, &String.contains?(&1, "rule_execution_order must contain only strings"))
    end

    test "rejects non-map rule specification" do
      assert {:error, errors} = Config.validate_rule_spec("not a map")
      assert Enum.any?(errors, &String.contains?(&1, "rule_spec must be a map"))
    end
  end

  describe "validate_environment_config/1" do
    test "validates production environment" do
      # Set production-friendly configuration
      Application.put_env(:presto, :engine, %{enable_concurrent_execution: true})
      Application.put_env(:presto, :performance, %{enable_metrics: true})
      
      assert Config.validate_environment_config(:prod) == :ok
      
      # Cleanup
      Application.delete_env(:presto, :engine)
      Application.delete_env(:presto, :performance)
    end

    test "fails production validation with poor configuration" do
      # Set non-production configuration
      Application.put_env(:presto, :engine, %{enable_concurrent_execution: false})
      
      assert {:error, errors} = Config.validate_environment_config(:prod)
      assert Enum.any?(errors, &String.contains?(&1, "Production should enable concurrent execution"))
      
      # Cleanup
      Application.delete_env(:presto, :engine)
    end

    test "validates test environment" do
      # Set test-friendly configuration
      Application.put_env(:presto, :engine, %{rule_timeout_ms: 500})
      
      assert Config.validate_environment_config(:test) == :ok
      
      # Cleanup
      Application.delete_env(:presto, :engine)
    end

    test "validates development environment" do
      # Development is permissive
      assert Config.validate_environment_config(:dev) == :ok
    end

    test "rejects invalid environment" do
      assert {:error, errors} = Config.validate_environment_config(:invalid)
      assert Enum.any?(errors, &String.contains?(&1, "Invalid environment"))
    end
  end

  describe "get_config_schema/0" do
    test "returns complete configuration schema" do
      schema = Config.get_config_schema()
      
      assert Map.has_key?(schema, :engine)
      assert Map.has_key?(schema, :rule_registry)
      assert Map.has_key?(schema, :performance)
      
      # Check engine schema structure
      engine_schema = schema.engine
      assert Map.has_key?(engine_schema, :max_rules)
      assert Map.has_key?(engine_schema, :rule_timeout_ms)
      
      # Check field specifications
      max_rules_spec = engine_schema.max_rules
      assert max_rules_spec.type == :integer
      assert is_boolean(max_rules_spec.required)
      assert is_function(max_rules_spec.validator)
      assert is_binary(max_rules_spec.description)
    end
  end

  describe "generate_config_docs/0" do
    test "generates configuration documentation" do
      docs = Config.generate_config_docs()
      
      assert is_binary(docs)
      assert String.contains?(docs, "ENGINE Configuration")
      assert String.contains?(docs, "RULE_REGISTRY Configuration")
      assert String.contains?(docs, "PERFORMANCE Configuration")
      assert String.contains?(docs, "max_rules")
      assert String.contains?(docs, "rule_timeout_ms")
      assert String.contains?(docs, "default:")
      assert String.contains?(docs, "required")
      assert String.contains?(docs, "optional")
    end
  end

  describe "type validation" do
    test "validates integer types correctly" do
      config = %{max_rules: 100}
      assert Config.validate_engine_config(config) == :ok
      
      config = %{max_rules: "100"}
      assert {:error, _} = Config.validate_engine_config(config)
    end

    test "validates boolean types correctly" do
      config = %{enable_concurrent_execution: true}
      assert Config.validate_engine_config(config) == :ok
      
      config = %{enable_concurrent_execution: "true"}
      assert {:error, _} = Config.validate_engine_config(config)
    end

    test "validates list types correctly" do
      config = %{default_rules: []}
      assert Config.validate_rule_registry_config(config) == :ok
      
      config = %{default_rules: "not a list"}
      assert {:error, _} = Config.validate_rule_registry_config(config)
    end
  end

  describe "custom validators" do
    test "validates positive integers" do
      config = %{max_rules: 1}
      assert Config.validate_engine_config(config) == :ok
      
      config = %{max_rules: 0}
      assert {:error, _} = Config.validate_engine_config(config)
      
      config = %{max_rules: -1}
      assert {:error, _} = Config.validate_engine_config(config)
    end

    test "validates timeout ranges" do
      config = %{rule_timeout_ms: 1000}
      assert Config.validate_engine_config(config) == :ok
      
      config = %{rule_timeout_ms: 60_000}
      assert Config.validate_engine_config(config) == :ok
      
      config = %{rule_timeout_ms: 60_001}
      assert {:error, _} = Config.validate_engine_config(config)
    end

    test "validates concurrent rule limits" do
      config = %{max_concurrent_rules: 1}
      assert Config.validate_engine_config(config) == :ok
      
      config = %{max_concurrent_rules: 100}
      assert Config.validate_engine_config(config) == :ok
      
      config = %{max_concurrent_rules: 101}
      assert {:error, _} = Config.validate_engine_config(config)
    end
  end
end