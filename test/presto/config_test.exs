defmodule Presto.ConfigTest do
  use ExUnit.Case, async: true

  alias Presto.Config

  describe "get_engine_config/0" do
    test "returns default engine configuration when no config is set" do
      config = Config.get_engine_config()

      assert config.max_rules == 1000
      assert config.rule_timeout_ms == 5000
      assert config.enable_concurrent_execution == false
      assert config.max_concurrent_rules == 10
      assert config.working_memory_limit == 100_000
    end

    test "merges custom configuration with defaults" do
      # Save original config
      original_config = Application.get_env(:presto, :engine)

      Application.put_env(:presto, :engine, %{max_rules: 500, enable_concurrent_execution: true})

      config = Config.get_engine_config()

      assert config.max_rules == 500
      assert config.enable_concurrent_execution == true
      # Defaults should still be present
      assert config.rule_timeout_ms == 5000
      assert config.max_concurrent_rules == 10

      # Restore original config
      if original_config do
        Application.put_env(:presto, :engine, original_config)
      else
        Application.delete_env(:presto, :engine)
      end
    end
  end

  describe "get_rule_registry_config/0" do
    test "returns default rule registry configuration when no config is set" do
      config = Config.get_rule_registry_config()

      assert config.default_rules == []
      assert config.rule_cache_size == 1000
      assert config.enable_rule_hot_reload == false
    end

    test "merges custom configuration with defaults" do
      # Save original config
      original_config = Application.get_env(:presto, :rule_registry)

      Application.put_env(:presto, :rule_registry, %{
        rule_cache_size: 2000,
        enable_rule_hot_reload: true
      })

      config = Config.get_rule_registry_config()

      assert config.rule_cache_size == 2000
      assert config.enable_rule_hot_reload == true
      # Defaults should still be present
      assert config.default_rules == []

      # Restore original config
      if original_config do
        Application.put_env(:presto, :rule_registry, original_config)
      else
        Application.delete_env(:presto, :rule_registry)
      end
    end
  end

  describe "get_performance_config/0" do
    test "returns default performance configuration when no config is set" do
      config = Config.get_performance_config()

      assert config.enable_metrics == true
      assert config.metrics_interval_ms == 60_000
      assert config.enable_profiling == false
    end

    test "merges custom configuration with defaults" do
      # Save original config
      original_config = Application.get_env(:presto, :performance)

      Application.put_env(:presto, :performance, %{
        metrics_interval_ms: 30_000,
        enable_profiling: true
      })

      config = Config.get_performance_config()

      assert config.metrics_interval_ms == 30_000
      assert config.enable_profiling == true
      # Defaults should still be present
      assert config.enable_metrics == true

      # Restore original config
      if original_config do
        Application.put_env(:presto, :performance, original_config)
      else
        Application.delete_env(:presto, :performance)
      end
    end
  end
end
