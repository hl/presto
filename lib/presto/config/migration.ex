defmodule Presto.Config.Migration do
  @moduledoc """
  Configuration migration utilities for Presto.

  Handles upgrading configuration between versions, providing
  backward compatibility and smooth transitions.
  """

  alias Presto.Logger, as: PrestoLogger

  @type migration_result :: {:ok, map()} | {:error, String.t()}
  @type version :: String.t()

  # Configuration version history
  @config_versions %{
    "0.1.0" => %{
      # Initial version - basic configuration
      engine: [:max_rules, :rule_timeout_ms],
      rule_registry: [:default_rules]
    },
    "0.2.0" => %{
      # Added concurrent execution
      engine: [:max_rules, :rule_timeout_ms, :enable_concurrent_execution, :max_concurrent_rules],
      rule_registry: [:default_rules, :rule_cache_size]
    },
    "0.3.0" => %{
      # Added performance monitoring and working memory limits
      engine: [
        :max_rules,
        :rule_timeout_ms,
        :enable_concurrent_execution,
        :max_concurrent_rules,
        :working_memory_limit
      ],
      rule_registry: [:default_rules, :rule_cache_size, :enable_rule_hot_reload],
      performance: [:enable_metrics, :metrics_interval_ms, :enable_profiling]
    }
  }

  @current_version "0.3.0"

  @doc """
  Migrates configuration from an older version to the current version.
  """
  @spec migrate_config(map(), version()) :: migration_result()
  def migrate_config(config, from_version) do
    PrestoLogger.log_configuration(:info, "migration", "starting", %{
      from_version: from_version,
      to_version: @current_version
    })

    case get_migration_path(from_version, @current_version) do
      {:ok, migration_steps} ->
        apply_migrations(config, migration_steps)

      {:error, reason} ->
        PrestoLogger.log_configuration(:error, "migration", "path_not_found", %{
          from_version: from_version,
          to_version: @current_version,
          reason: reason
        })

        {:error, reason}
    end
  end

  @doc """
  Detects the configuration version based on available keys.
  """
  @spec detect_config_version(map()) :: version()
  def detect_config_version(config) do
    # Check which version the configuration most closely matches
    # Start from the highest version and work backwards
    versions = Map.keys(@config_versions) |> Enum.sort(:desc)

    Enum.find(versions, "0.1.0", fn version ->
      schema = @config_versions[version]
      config_matches_schema?(config, schema)
    end)
  end

  @doc """
  Validates that a configuration can be migrated to the current version.
  """
  @spec can_migrate?(map(), version()) :: boolean()
  def can_migrate?(_config, from_version) do
    case get_migration_path(from_version, @current_version) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  @doc """
  Creates a backup of the current configuration before migration.
  """
  @spec backup_config(map()) :: {:ok, String.t()} | {:error, String.t()}
  def backup_config(config) do
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601(:basic)
    backup_filename = "presto_config_backup_#{timestamp}.json"
    backup_path = Path.join([System.tmp_dir!(), backup_filename])

    try do
      json_config = Jason.encode!(config, pretty: true)
      File.write!(backup_path, json_config)

      PrestoLogger.log_configuration(:info, "backup", "created", %{
        backup_path: backup_path,
        config_size: byte_size(json_config)
      })

      {:ok, backup_path}
    rescue
      error ->
        PrestoLogger.log_configuration(:error, "backup", "failed", %{
          error: Exception.message(error)
        })

        {:error, "Failed to create backup: #{Exception.message(error)}"}
    end
  end

  @doc """
  Restores configuration from a backup file.
  """
  @spec restore_config(String.t()) :: {:ok, map()} | {:error, String.t()}
  def restore_config(backup_path) do
    try do
      json_content = File.read!(backup_path)
      config = Jason.decode!(json_content, keys: :atoms)

      PrestoLogger.log_configuration(:info, "restore", "completed", %{
        backup_path: backup_path
      })

      {:ok, config}
    rescue
      error ->
        PrestoLogger.log_configuration(:error, "restore", "failed", %{
          backup_path: backup_path,
          error: Exception.message(error)
        })

        {:error, "Failed to restore backup: #{Exception.message(error)}"}
    end
  end

  @doc """
  Generates a migration report showing what will change.
  """
  @spec generate_migration_report(map(), version()) :: String.t()
  def generate_migration_report(_config, from_version) do
    case get_migration_path(from_version, @current_version) do
      {:ok, migration_steps} ->
        changes = Enum.map(migration_steps, &describe_migration_step/1)

        """
        Configuration Migration Report
        =============================

        From Version: #{from_version}
        To Version: #{@current_version}

        Changes to be applied:
        #{Enum.join(changes, "\n")}

        Current configuration will be backed up before migration.
        """

      {:error, reason} ->
        "Migration not possible: #{reason}"
    end
  end

  # Private functions

  defp get_migration_path(from_version, to_version) when from_version == to_version do
    {:ok, []}
  end

  defp get_migration_path(from_version, to_version) do
    versions = Map.keys(@config_versions) |> Enum.sort()

    case {Enum.find_index(versions, &(&1 == from_version)),
          Enum.find_index(versions, &(&1 == to_version))} do
      {nil, _} ->
        {:error, "Unknown source version: #{from_version}"}

      {_, nil} ->
        {:error, "Unknown target version: #{to_version}"}

      {from_idx, to_idx} when from_idx > to_idx ->
        {:error, "Cannot downgrade configuration"}

      {from_idx, to_idx} ->
        migration_versions = Enum.slice(versions, (from_idx + 1)..to_idx)
        {:ok, migration_versions}
    end
  end

  defp apply_migrations(config, []), do: {:ok, config}

  defp apply_migrations(config, [version | remaining_versions]) do
    case apply_single_migration(config, version) do
      {:ok, migrated_config} ->
        PrestoLogger.log_configuration(:info, "migration", "step_completed", %{
          version: version
        })

        apply_migrations(migrated_config, remaining_versions)

      {:error, reason} ->
        PrestoLogger.log_configuration(:error, "migration", "step_failed", %{
          version: version,
          reason: reason
        })

        {:error, "Migration to #{version} failed: #{reason}"}
    end
  end

  defp apply_single_migration(config, "0.2.0") do
    # Migration from 0.1.0 to 0.2.0: Add concurrent execution defaults
    updated_config =
      config
      |> put_in_if_missing([:engine, :enable_concurrent_execution], false)
      |> put_in_if_missing([:engine, :max_concurrent_rules], 10)
      |> put_in_if_missing([:rule_registry, :rule_cache_size], 1000)

    {:ok, updated_config}
  end

  defp apply_single_migration(config, "0.3.0") do
    # Migration from 0.2.0 to 0.3.0: Add performance monitoring and memory limits
    updated_config =
      config
      |> put_in_if_missing([:engine, :working_memory_limit], 100_000)
      |> put_in_if_missing([:rule_registry, :enable_rule_hot_reload], false)
      |> put_in_if_missing([:performance, :enable_metrics], true)
      |> put_in_if_missing([:performance, :metrics_interval_ms], 60_000)
      |> put_in_if_missing([:performance, :enable_profiling], false)

    {:ok, updated_config}
  end

  defp apply_single_migration(_config, version) do
    {:error, "No migration defined for version #{version}"}
  end

  defp put_in_if_missing(config, path, default_value) do
    if get_in(config, path) == nil do
      # Ensure all intermediate keys exist
      ensure_path_exists(config, path, default_value)
    else
      config
    end
  end

  defp ensure_path_exists(config, [key], value) do
    Map.put(config, key, value)
  end

  defp ensure_path_exists(config, [key | rest], value) do
    current_value = Map.get(config, key, %{})
    updated_value = ensure_path_exists(current_value, rest, value)
    Map.put(config, key, updated_value)
  end

  defp config_matches_schema?(config, schema) do
    Enum.all?(schema, fn {section, expected_keys} ->
      section_config = Map.get(config, section, %{})
      section_keys = Map.keys(section_config)

      # Check if all expected keys are present and no extra keys
      Enum.all?(expected_keys, &(&1 in section_keys)) and
        length(section_keys) == length(expected_keys)
    end)
  end

  defp describe_migration_step("0.2.0") do
    """
    - Add concurrent execution configuration
      * engine.enable_concurrent_execution (default: false)
      * engine.max_concurrent_rules (default: 10)
      * rule_registry.rule_cache_size (default: 1000)
    """
  end

  defp describe_migration_step("0.3.0") do
    """
    - Add performance monitoring and memory management
      * engine.working_memory_limit (default: 100,000)
      * rule_registry.enable_rule_hot_reload (default: false)
      * performance.enable_metrics (default: true)
      * performance.metrics_interval_ms (default: 60,000)
      * performance.enable_profiling (default: false)
    """
  end

  defp describe_migration_step(version) do
    "- Unknown migration step for version #{version}"
  end
end
