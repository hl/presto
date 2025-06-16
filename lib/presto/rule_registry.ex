defmodule Presto.RuleRegistry do
  @moduledoc """
  Configuration-driven registry system that maps rule names to their implementations.

  This registry reads rule mappings from the host application's configuration,
  allowing users to define their own custom rule modules while keeping the
  library flexible and extensible.

  ## Configuration

  Configure rules in your application's config:

      config :presto, :rule_registry,
        rules: %{
          "time_calculation" => MyApp.PayrollRules,
          "overtime_check" => MyApp.PayrollRules,
          "weekly_compliance" => MyApp.ComplianceRules,
          "custom_rule" => MyApp.CustomRules
        }

  Or using the advanced format:

      config :presto, :rule_registry,
        rules: [
          {MyApp.PayrollRules, ["time_calculation", "overtime_check"]},
          {MyApp.ComplianceRules, ["weekly_compliance"]}
        ]

  Rule modules must implement the `Presto.RuleBehaviour`.
  """

  @type rule_name :: String.t()
  @type rule_variables :: map()
  @type rule_spec :: %{
          String.t() => term()
        }
  @type rule_mapping :: %{rule_name() => module()}

  @doc """
  Returns all available rule implementations from application configuration.

  Reads rule mappings from the `:presto` application config under the
  `:rule_registry` key. Supports both simple map format and advanced
  list format for configuration.

  ## Examples

      # Simple format in config
      config :presto, :rule_registry,
        rules: %{"rule_name" => Module}

      # Advanced format in config
      config :presto, :rule_registry,
        rules: [{Module, ["rule1", "rule2"]}]

  """
  @spec get_available_rules() :: rule_mapping()
  def get_available_rules do
    # Check for dynamic rules first (used in testing)
    case Process.get(:presto_dynamic_rules) do
      nil -> get_configured_rules()
      dynamic_rules -> dynamic_rules
    end
  end

  @doc """
  Returns all available rule implementations (alias for backward compatibility).
  """
  @spec available_rules() :: rule_mapping()
  def available_rules, do: get_available_rules()

  @doc """
  Validates that a rule specification contains only available rules.

  Checks that:
  1. The rule spec has the correct structure
  2. All requested rules are available in the registry
  3. Each rule module validates the spec (if it implements validation)
  """
  @spec valid_rule_spec?(rule_spec()) :: boolean()
  def valid_rule_spec?(%{"rules_to_run" => rules, "variables" => variables})
      when is_list(rules) and is_map(variables) do
    available = Map.keys(get_available_rules())

    # Check all requested rules are available
    rules_available? = Enum.all?(rules, &(&1 in available))

    # Check each module validates the spec (if validation is implemented)
    modules_valid? =
      rules
      |> Enum.map(&get_rule_module/1)
      |> Enum.filter(&match?({:ok, _}, &1))
      |> Enum.map(fn {:ok, module} -> module end)
      |> Enum.uniq()
      |> Enum.all?(
        &module_validates_spec?(&1, %{"rules_to_run" => rules, "variables" => variables})
      )

    rules_available? && modules_valid?
  end

  def valid_rule_spec?(_), do: false

  @doc """
  Gets the implementation module for a specific rule.
  """
  @spec get_rule_module(rule_name()) :: {:ok, module()} | {:error, :not_found}
  def get_rule_module(rule_name) do
    case Map.get(get_available_rules(), rule_name) do
      nil ->
        {:error, :not_found}

      module when is_atom(module) ->
        if module_implements_behaviour?(module) do
          {:ok, module}
        else
          {:error, {:invalid_module, "#{module} does not implement Presto.RuleBehaviour"}}
        end

      _ ->
        {:error, :invalid_configuration}
    end
  end

  @doc """
  Extracts variables for a specific rule from the rule specification.
  """
  @spec get_rule_variables(rule_spec(), rule_name()) :: rule_variables()
  def get_rule_variables(%{"variables" => variables}, _rule_name) do
    variables
  end

  def get_rule_variables(_, _), do: %{}

  @doc """
  Registers a rule dynamically at runtime.

  Useful for testing or scenarios where rules need to be registered
  after application startup. Note that this only affects the current
  process and is not persisted.
  """
  @spec register_rule(rule_name(), module()) :: :ok | {:error, term()}
  def register_rule(rule_name, module) when is_binary(rule_name) and is_atom(module) do
    if module_implements_behaviour?(module) do
      current_rules = get_available_rules()
      new_rules = Map.put(current_rules, rule_name, module)
      Process.put(:presto_dynamic_rules, new_rules)
      :ok
    else
      {:error, {:invalid_module, "#{module} does not implement Presto.RuleBehaviour"}}
    end
  end

  @doc """
  Lists all configured rule names.
  """
  @spec list_rule_names() :: [rule_name()]
  def list_rule_names do
    get_available_rules() |> Map.keys()
  end

  @doc """
  Validates the current rule registry configuration.

  Returns `:ok` if all configured modules exist and implement the required
  behaviour, or `{:error, reasons}` with a list of validation errors.
  """
  @spec validate_configuration() :: :ok | {:error, [term()]}
  def validate_configuration do
    rules = get_available_rules()

    errors =
      rules
      |> Enum.flat_map(fn {rule_name, module} ->
        cond do
          not is_atom(module) ->
            [{:invalid_module_type, rule_name, module}]

          not Code.ensure_loaded?(module) ->
            [{:module_not_found, rule_name, module}]

          not module_implements_behaviour?(module) ->
            [{:behaviour_not_implemented, rule_name, module}]

          true ->
            []
        end
      end)

    case errors do
      [] -> :ok
      errors -> {:error, errors}
    end
  end

  # Private functions

  @spec normalize_rule_config(any()) :: rule_mapping()
  defp normalize_rule_config(rules) when is_map(rules), do: rules

  defp normalize_rule_config(rules) when is_list(rules) do
    rules
    |> Enum.flat_map(fn
      {module, rule_names} when is_list(rule_names) ->
        Enum.map(rule_names, fn rule_name -> {rule_name, module} end)

      _ ->
        []
    end)
    |> Map.new()
  end

  defp normalize_rule_config(_), do: %{}

  @spec module_implements_behaviour?(module()) :: boolean()
  defp module_implements_behaviour?(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} ->
        behaviours =
          module.module_info(:attributes)
          |> Keyword.get(:behaviour, [])

        Presto.RuleBehaviour in behaviours

      _ ->
        false
    end
  end

  @spec module_validates_spec?(module(), rule_spec()) :: boolean()
  defp module_validates_spec?(module, rule_spec) do
    if function_exported?(module, :valid_rule_spec?, 1) do
      module.valid_rule_spec?(rule_spec)
    else
      # If validation not implemented, assume valid
      true
    end
  end

  defp get_configured_rules do
    case Application.get_env(:presto, :rule_registry, []) do
      config when is_list(config) ->
        rules = Keyword.get(config, :rules, [])
        normalize_rule_config(rules)

      _ ->
        %{}
    end
  end
end
