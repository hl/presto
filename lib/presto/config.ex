defmodule Presto.Config do
  @moduledoc """
  Configuration validation and management for the Presto rules engine.

  Provides schema validation, environment-specific configuration handling,
  and runtime configuration validation to ensure system reliability.
  """

  alias Presto.Exceptions.ConfigurationError
  alias Presto.Logger, as: PrestoLogger

  @type config_schema :: %{
          required(atom()) => config_field_spec()
        }

  @type config_field_spec :: %{
          type: atom(),
          required: boolean(),
          default: any(),
          validator: function() | nil,
          description: String.t()
        }

  @type validation_result :: :ok | {:error, [String.t()]}

  # Configuration schemas for different components
  defp engine_schema do
    %{
      max_rules: %{
        type: :integer,
        required: false,
        default: 1000,
        validator: &(&1 > 0),
        description: "Maximum number of rules allowed in the engine"
      },
      rule_timeout_ms: %{
        type: :integer,
        required: false,
        default: 5000,
        validator: &(&1 > 0 and &1 <= 60_000),
        description: "Maximum time in milliseconds for rule execution"
      },
      enable_concurrent_execution: %{
        type: :boolean,
        required: false,
        default: false,
        validator: &is_boolean/1,
        description: "Whether to enable concurrent rule execution"
      },
      max_concurrent_rules: %{
        type: :integer,
        required: false,
        default: 10,
        validator: &(&1 > 0 and &1 <= 100),
        description: "Maximum number of rules to execute concurrently"
      },
      working_memory_limit: %{
        type: :integer,
        required: false,
        default: 100_000,
        validator: &(&1 > 0),
        description: "Maximum number of facts in working memory"
      }
    }
  end

  defp rule_registry_schema do
    %{
      default_rules: %{
        type: :list,
        required: false,
        default: [],
        validator: &is_list/1,
        description: "List of default rule modules to load"
      },
      rule_cache_size: %{
        type: :integer,
        required: false,
        default: 1000,
        validator: &(&1 > 0),
        description: "Maximum number of compiled rules to cache"
      },
      enable_rule_hot_reload: %{
        type: :boolean,
        required: false,
        default: false,
        validator: &is_boolean/1,
        description: "Whether to enable hot reloading of rule modules"
      }
    }
  end

  defp performance_schema do
    %{
      enable_metrics: %{
        type: :boolean,
        required: false,
        default: true,
        validator: &is_boolean/1,
        description: "Whether to collect performance metrics"
      },
      metrics_interval_ms: %{
        type: :integer,
        required: false,
        default: 60_000,
        validator: &(&1 >= 1000),
        description: "Interval for collecting performance metrics"
      },
      enable_profiling: %{
        type: :boolean,
        required: false,
        default: false,
        validator: &is_boolean/1,
        description: "Whether to enable detailed profiling"
      }
    }
  end

  @doc """
  Validates engine configuration against the schema.
  """
  @spec validate_engine_config(map()) :: validation_result()
  def validate_engine_config(config) do
    validate_config(config, engine_schema(), "engine")
  end

  @doc """
  Validates rule registry configuration against the schema.
  """
  @spec validate_rule_registry_config(map()) :: validation_result()
  def validate_rule_registry_config(config) do
    validate_config(config, rule_registry_schema(), "rule_registry")
  end

  @doc """
  Validates performance configuration against the schema.
  """
  @spec validate_performance_config(map()) :: validation_result()
  def validate_performance_config(config) do
    validate_config(config, performance_schema(), "performance")
  end

  @doc """
  Validates a complete Presto configuration.
  """
  @spec validate_presto_config(map()) :: validation_result()
  def validate_presto_config(config) do
    engine_config = Map.get(config, :engine, %{})
    registry_config = Map.get(config, :rule_registry, %{})
    performance_config = Map.get(config, :performance, %{})

    with :ok <- validate_engine_config(engine_config),
         :ok <- validate_rule_registry_config(registry_config),
         :ok <- validate_performance_config(performance_config) do
      :ok
    else
      {:error, errors} -> {:error, errors}
    end
  end

  @doc """
  Gets validated configuration for the engine with defaults applied.
  """
  @spec get_engine_config() :: map()
  def get_engine_config do
    config = Application.get_env(:presto, :engine, %{})

    case validate_and_apply_defaults(config, engine_schema()) do
      {:ok, validated_config} ->
        PrestoLogger.log_configuration(:info, "engine", "config_loaded", %{
          config: validated_config
        })

        validated_config

      {:error, errors} ->
        PrestoLogger.log_configuration(:error, "engine", "config_invalid", %{errors: errors})
        raise ConfigurationError, config_key: "engine", validation_errors: errors
    end
  end

  @doc """
  Gets validated configuration for the rule registry with defaults applied.
  """
  @spec get_rule_registry_config() :: map()
  def get_rule_registry_config do
    config = Application.get_env(:presto, :rule_registry, %{})

    case validate_and_apply_defaults(config, rule_registry_schema()) do
      {:ok, validated_config} ->
        PrestoLogger.log_configuration(:info, "rule_registry", "config_loaded", %{
          config: validated_config
        })

        validated_config

      {:error, errors} ->
        PrestoLogger.log_configuration(:error, "rule_registry", "config_invalid", %{
          errors: errors
        })

        raise ConfigurationError, config_key: "rule_registry", validation_errors: errors
    end
  end

  @doc """
  Gets validated configuration for performance monitoring with defaults applied.
  """
  @spec get_performance_config() :: map()
  def get_performance_config do
    config = Application.get_env(:presto, :performance, %{})

    case validate_and_apply_defaults(config, performance_schema()) do
      {:ok, validated_config} ->
        PrestoLogger.log_configuration(:info, "performance", "config_loaded", %{
          config: validated_config
        })

        validated_config

      {:error, errors} ->
        PrestoLogger.log_configuration(:error, "performance", "config_invalid", %{errors: errors})
        raise ConfigurationError, config_key: "performance", validation_errors: errors
    end
  end

  @doc """
  Validates a rule specification against expected structure.
  """
  @spec validate_rule_spec(map()) :: validation_result()
  def validate_rule_spec(rule_spec) when is_map(rule_spec) do
    errors = []

    errors =
      if Map.has_key?(rule_spec, "rules_to_run") do
        case Map.get(rule_spec, "rules_to_run") do
          rules when is_list(rules) ->
            if Enum.all?(rules, &is_binary/1) do
              errors
            else
              ["rules_to_run must contain only strings" | errors]
            end

          _ ->
            ["rules_to_run must be a list" | errors]
        end
      else
        errors
      end

    errors =
      if Map.has_key?(rule_spec, "variables") do
        case Map.get(rule_spec, "variables") do
          variables when is_map(variables) -> errors
          _ -> ["variables must be a map" | errors]
        end
      else
        errors
      end

    errors =
      if Map.has_key?(rule_spec, "rule_execution_order") do
        case Map.get(rule_spec, "rule_execution_order") do
          order when is_list(order) ->
            if Enum.all?(order, &is_binary/1) do
              errors
            else
              ["rule_execution_order must contain only strings" | errors]
            end

          _ ->
            ["rule_execution_order must be a list" | errors]
        end
      else
        errors
      end

    case errors do
      [] -> :ok
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  def validate_rule_spec(_), do: {:error, ["rule_spec must be a map"]}

  @doc """
  Validates environment-specific configuration.
  """
  @spec validate_environment_config(atom()) :: validation_result()
  def validate_environment_config(env) when env in [:dev, :test, :prod] do
    config = Application.get_all_env(:presto)

    case env do
      :prod -> validate_production_config(config)
      :test -> validate_test_config(config)
      :dev -> validate_development_config(config)
    end
  end

  def validate_environment_config(env) do
    {:error, ["Invalid environment: #{env}. Must be :dev, :test, or :prod"]}
  end

  @doc """
  Gets the current configuration schema for documentation purposes.
  """
  @spec get_config_schema() :: %{atom() => config_schema()}
  def get_config_schema do
    %{
      engine: engine_schema(),
      rule_registry: rule_registry_schema(),
      performance: performance_schema()
    }
  end

  @doc """
  Generates configuration documentation.
  """
  @spec generate_config_docs() :: String.t()
  def generate_config_docs do
    schema = get_config_schema()

    Enum.map_join(schema, "\n\n", fn {section, fields} ->
      "## #{String.upcase(to_string(section))} Configuration\n\n" <>
        Enum.map_join(fields, "\n", fn {key, spec} ->
          required_text = if spec.required, do: " (required)", else: " (optional)"
          default_text = if spec.default, do: " [default: #{inspect(spec.default)}]", else: ""
          "- **#{key}** (#{spec.type})#{required_text}#{default_text}: #{spec.description}"
        end)
    end)
  end

  # Private functions

  defp validate_config(config, schema, section_name) do
    case validate_and_apply_defaults(config, schema) do
      {:ok, _validated_config} ->
        :ok

      {:error, errors} ->
        PrestoLogger.log_configuration(:error, section_name, "validation_failed", %{
          errors: errors
        })

        {:error, errors}
    end
  end

  defp validate_and_apply_defaults(config, schema) do
    {validated_config, errors} =
      Enum.reduce(schema, {%{}, []}, fn {key, spec}, {acc_config, acc_errors} ->
        case validate_field(config, key, spec) do
          {:ok, value} -> {Map.put(acc_config, key, value), acc_errors}
          {:error, error} -> {acc_config, [error | acc_errors]}
        end
      end)

    case errors do
      [] -> {:ok, validated_config}
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  defp validate_field(config, key, spec) do
    value = Map.get(config, key, spec.default)

    cond do
      is_nil(value) and spec.required ->
        {:error, "#{key} is required but not provided"}

      is_nil(value) ->
        {:ok, spec.default}

      not validate_type(value, spec.type) ->
        {:error, "#{key} must be of type #{spec.type}, got #{inspect(value)}"}

      spec.validator && not spec.validator.(value) ->
        {:error, "#{key} failed validation: #{inspect(value)}"}

      true ->
        {:ok, value}
    end
  end

  defp validate_type(value, :integer), do: is_integer(value)
  defp validate_type(value, :float), do: is_float(value)
  defp validate_type(value, :number), do: is_number(value)
  defp validate_type(value, :boolean), do: is_boolean(value)
  defp validate_type(value, :string), do: is_binary(value)
  defp validate_type(value, :atom), do: is_atom(value)
  defp validate_type(value, :list), do: is_list(value)
  defp validate_type(value, :map), do: is_map(value)
  defp validate_type(_value, _type), do: false

  defp validate_production_config(config) do
    errors = []

    # Production-specific validations
    errors =
      case get_in(config, [:engine, :enable_concurrent_execution]) do
        true -> errors
        _ -> ["Production should enable concurrent execution for performance" | errors]
      end

    errors =
      case get_in(config, [:performance, :enable_metrics]) do
        true -> errors
        _ -> ["Production should enable metrics collection" | errors]
      end

    case errors do
      [] -> :ok
      _ -> {:error, errors}
    end
  end

  defp validate_test_config(config) do
    errors = []

    # Test-specific validations
    errors =
      case get_in(config, [:engine, :rule_timeout_ms]) do
        timeout when is_integer(timeout) and timeout <= 1000 -> errors
        _ -> ["Test environment should use short timeouts for faster tests" | errors]
      end

    case errors do
      [] -> :ok
      _ -> {:error, errors}
    end
  end

  defp validate_development_config(_config) do
    # Development environment is more permissive
    :ok
  end
end
