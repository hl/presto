defmodule Presto.Config.MigrationTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Presto.Config.Migration

  describe "detect_config_version/1" do
    test "detects version 0.1.0 configuration" do
      config = %{
        engine: %{max_rules: 100, rule_timeout_ms: 5000},
        rule_registry: %{default_rules: []}
      }

      assert Migration.detect_config_version(config) == "0.1.0"
    end

    test "detects version 0.2.0 configuration" do
      config = %{
        engine: %{
          max_rules: 100,
          rule_timeout_ms: 5000,
          enable_concurrent_execution: true,
          max_concurrent_rules: 5
        },
        rule_registry: %{
          default_rules: [],
          rule_cache_size: 1000
        }
      }

      assert Migration.detect_config_version(config) == "0.2.0"
    end

    test "detects version 0.3.0 configuration" do
      config = %{
        engine: %{
          max_rules: 100,
          rule_timeout_ms: 5000,
          enable_concurrent_execution: true,
          max_concurrent_rules: 5,
          working_memory_limit: 50_000
        },
        rule_registry: %{
          default_rules: [],
          rule_cache_size: 1000,
          enable_rule_hot_reload: false
        },
        performance: %{
          enable_metrics: true,
          metrics_interval_ms: 30_000,
          enable_profiling: false
        }
      }

      assert Migration.detect_config_version(config) == "0.3.0"
    end
  end

  describe "can_migrate?/2" do
    test "can migrate from older to newer version" do
      config = %{engine: %{max_rules: 100}}

      assert Migration.can_migrate?(config, "0.1.0") == true
      assert Migration.can_migrate?(config, "0.2.0") == true
    end

    test "cannot migrate from newer to older version" do
      config = %{engine: %{max_rules: 100}}

      # This would be a downgrade, which is not supported
      assert Migration.can_migrate?(config, "0.4.0") == false
    end

    test "cannot migrate from unknown version" do
      config = %{engine: %{max_rules: 100}}

      assert Migration.can_migrate?(config, "unknown") == false
    end
  end

  describe "migrate_config/2" do
    test "migrates from 0.1.0 to current version" do
      config = %{
        engine: %{max_rules: 100, rule_timeout_ms: 3000},
        rule_registry: %{default_rules: []}
      }

      log =
        capture_log(fn ->
          assert {:ok, migrated_config} = Migration.migrate_config(config, "0.1.0")

          # Should have new fields from 0.2.0
          assert migrated_config.engine.enable_concurrent_execution == false
          assert migrated_config.engine.max_concurrent_rules == 10
          assert migrated_config.rule_registry.rule_cache_size == 1000

          # Should have new fields from 0.3.0
          assert migrated_config.engine.working_memory_limit == 100_000
          assert migrated_config.rule_registry.enable_rule_hot_reload == false
          assert migrated_config.performance.enable_metrics == true
          assert migrated_config.performance.metrics_interval_ms == 60_000
          assert migrated_config.performance.enable_profiling == false

          # Should preserve original values
          assert migrated_config.engine.max_rules == 100
          assert migrated_config.engine.rule_timeout_ms == 3000
          assert migrated_config.rule_registry.default_rules == []
        end)

      assert log =~ "Configuration: starting for migration"
      assert log =~ "Configuration: step_completed for migration"
    end

    test "migrates from 0.2.0 to current version" do
      config = %{
        engine: %{
          max_rules: 200,
          rule_timeout_ms: 4000,
          enable_concurrent_execution: true,
          max_concurrent_rules: 15
        },
        rule_registry: %{
          default_rules: [SomeModule],
          rule_cache_size: 2000
        }
      }

      assert {:ok, migrated_config} = Migration.migrate_config(config, "0.2.0")

      # Should have new fields from 0.3.0
      assert migrated_config.engine.working_memory_limit == 100_000
      assert migrated_config.rule_registry.enable_rule_hot_reload == false
      assert migrated_config.performance.enable_metrics == true

      # Should preserve existing values
      assert migrated_config.engine.max_rules == 200
      assert migrated_config.engine.enable_concurrent_execution == true
      assert migrated_config.engine.max_concurrent_rules == 15
      assert migrated_config.rule_registry.rule_cache_size == 2000
    end

    test "no migration needed for current version" do
      config = %{engine: %{max_rules: 100}}

      assert {:ok, ^config} = Migration.migrate_config(config, "0.3.0")
    end

    test "fails migration from unknown version" do
      config = %{engine: %{max_rules: 100}}

      assert {:error, reason} = Migration.migrate_config(config, "unknown")
      assert reason =~ "Unknown source version"
    end

    test "preserves existing values during migration" do
      config = %{
        engine: %{
          max_rules: 500,
          rule_timeout_ms: 2000,
          # This should be preserved
          enable_concurrent_execution: true
        }
      }

      assert {:ok, migrated_config} = Migration.migrate_config(config, "0.2.0")

      # Existing value should be preserved, not overwritten with default
      assert migrated_config.engine.enable_concurrent_execution == true
      assert migrated_config.engine.max_rules == 500
      assert migrated_config.engine.rule_timeout_ms == 2000
    end
  end

  describe "backup_config/1" do
    test "creates configuration backup" do
      config = %{
        engine: %{max_rules: 100},
        rule_registry: %{default_rules: []}
      }

      log =
        capture_log(fn ->
          assert {:ok, backup_path} = Migration.backup_config(config)

          assert File.exists?(backup_path)
          assert String.contains?(backup_path, "presto_config_backup_")
          assert String.ends_with?(backup_path, ".json")

          # Verify backup content
          backup_content = File.read!(backup_path)
          restored_config = Jason.decode!(backup_content)

          assert restored_config["engine"]["max_rules"] == 100
          assert restored_config["rule_registry"]["default_rules"] == []

          # Cleanup
          File.rm!(backup_path)
        end)

      assert log =~ "Configuration: created for backup"
    end

    test "handles backup creation errors gracefully" do
      # Create a config that will cause JSON encoding to fail
      config = %{invalid: fn -> :error end}

      log =
        capture_log(fn ->
          assert {:error, reason} = Migration.backup_config(config)
          assert reason =~ "Failed to create backup"
        end)

      assert log =~ "Configuration: failed for backup"
    end
  end

  describe "restore_config/1" do
    test "restores configuration from backup" do
      # Create a temporary backup file
      config = %{engine: %{max_rules: 150}}
      backup_content = Jason.encode!(config, pretty: true)
      backup_path = Path.join([System.tmp_dir!(), "test_backup.json"])
      File.write!(backup_path, backup_content)

      log =
        capture_log(fn ->
          assert {:ok, restored_config} = Migration.restore_config(backup_path)
          assert restored_config == config
        end)

      assert log =~ "Configuration: completed for restore"

      # Cleanup
      File.rm!(backup_path)
    end

    test "handles restore errors gracefully" do
      non_existent_path = "/non/existent/path.json"

      log =
        capture_log(fn ->
          assert {:error, reason} = Migration.restore_config(non_existent_path)
          assert reason =~ "Failed to restore backup"
        end)

      assert log =~ "Configuration: failed for restore"
    end
  end

  describe "generate_migration_report/2" do
    test "generates migration report for valid migration" do
      config = %{engine: %{max_rules: 100}}

      report = Migration.generate_migration_report(config, "0.1.0")

      assert String.contains?(report, "Configuration Migration Report")
      assert String.contains?(report, "From Version: 0.1.0")
      assert String.contains?(report, "To Version: 0.3.0")
      assert String.contains?(report, "Changes to be applied:")
      assert String.contains?(report, "concurrent execution")
      assert String.contains?(report, "performance monitoring")
      assert String.contains?(report, "backed up before migration")
    end

    test "generates error report for invalid migration" do
      config = %{engine: %{max_rules: 100}}

      report = Migration.generate_migration_report(config, "unknown")

      assert String.contains?(report, "Migration not possible")
      assert String.contains?(report, "Unknown source version")
    end
  end

  describe "migration step descriptions" do
    test "describes 0.2.0 migration step" do
      config = %{engine: %{max_rules: 100}}

      report = Migration.generate_migration_report(config, "0.1.0")

      assert String.contains?(report, "concurrent execution configuration")
      assert String.contains?(report, "enable_concurrent_execution")
      assert String.contains?(report, "max_concurrent_rules")
      assert String.contains?(report, "rule_cache_size")
    end

    test "describes 0.3.0 migration step" do
      config = %{engine: %{max_rules: 100}}

      report = Migration.generate_migration_report(config, "0.2.0")

      assert String.contains?(report, "performance monitoring")
      assert String.contains?(report, "working_memory_limit")
      assert String.contains?(report, "enable_metrics")
      assert String.contains?(report, "enable_profiling")
    end
  end
end
