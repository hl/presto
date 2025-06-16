defmodule Presto.Exceptions do
  @moduledoc """
  Custom exception types for the Presto rules engine.

  Provides structured error handling with domain-specific context
  for better debugging and error reporting.
  """

  defmodule RuleCompilationError do
    @moduledoc """
    Raised when a rule fails to compile properly.
    """
    defexception [:message, :rule_id, :compilation_stage, :details]

    @impl true
    def exception(opts) do
      rule_id = Keyword.get(opts, :rule_id, "unknown")
      stage = Keyword.get(opts, :compilation_stage, "unknown")
      details = Keyword.get(opts, :details, %{})

      message =
        Keyword.get(
          opts,
          :message,
          "Rule compilation failed for rule '#{rule_id}' at stage '#{stage}'"
        )

      %__MODULE__{
        message: message,
        rule_id: rule_id,
        compilation_stage: stage,
        details: details
      }
    end
  end

  defmodule FactValidationError do
    @moduledoc """
    Raised when a fact fails validation before being added to working memory.
    """
    defexception [:message, :fact, :validation_errors, :fact_type]

    @impl true
    def exception(opts) do
      fact = Keyword.get(opts, :fact)
      validation_errors = Keyword.get(opts, :validation_errors, [])
      fact_type = Keyword.get(opts, :fact_type, "unknown")

      message =
        Keyword.get(
          opts,
          :message,
          "Fact validation failed for #{fact_type}: #{inspect(validation_errors)}"
        )

      %__MODULE__{
        message: message,
        fact: fact,
        validation_errors: validation_errors,
        fact_type: fact_type
      }
    end
  end

  defmodule RuleExecutionError do
    @moduledoc """
    Raised when a rule fails during execution.
    """
    defexception [:message, :rule_id, :execution_context, :original_error]

    @impl true
    def exception(opts) do
      rule_id = Keyword.get(opts, :rule_id, "unknown")
      context = Keyword.get(opts, :execution_context, %{})
      original_error = Keyword.get(opts, :original_error)

      message =
        Keyword.get(
          opts,
          :message,
          "Rule execution failed for rule '#{rule_id}': #{inspect(original_error)}"
        )

      %__MODULE__{
        message: message,
        rule_id: rule_id,
        execution_context: context,
        original_error: original_error
      }
    end
  end

  defmodule EngineStateError do
    @moduledoc """
    Raised when the engine is in an invalid state for the requested operation.
    """
    defexception [:message, :current_state, :requested_operation, :engine_id]

    @impl true
    def exception(opts) do
      current_state = Keyword.get(opts, :current_state, "unknown")
      operation = Keyword.get(opts, :requested_operation, "unknown")
      engine_id = Keyword.get(opts, :engine_id, "unknown")

      message =
        Keyword.get(
          opts,
          :message,
          "Engine #{engine_id} in state '#{current_state}' cannot perform '#{operation}'"
        )

      %__MODULE__{
        message: message,
        current_state: current_state,
        requested_operation: operation,
        engine_id: engine_id
      }
    end
  end

  defmodule ConfigurationError do
    @moduledoc """
    Raised when configuration is invalid or missing.
    """
    defexception [:message, :config_key, :config_value, :validation_errors]

    @impl true
    def exception(opts) do
      config_key = Keyword.get(opts, :config_key, "unknown")
      config_value = Keyword.get(opts, :config_value)
      validation_errors = Keyword.get(opts, :validation_errors, [])

      message =
        Keyword.get(
          opts,
          :message,
          "Configuration error for '#{config_key}': #{inspect(validation_errors)}"
        )

      %__MODULE__{
        message: message,
        config_key: config_key,
        config_value: config_value,
        validation_errors: validation_errors
      }
    end
  end

  defmodule NetworkError do
    @moduledoc """
    Raised when there are issues with the RETE network structure.
    """
    defexception [:message, :network_type, :node_id, :operation, :details]

    @impl true
    def exception(opts) do
      network_type = Keyword.get(opts, :network_type, "unknown")
      node_id = Keyword.get(opts, :node_id, "unknown")
      operation = Keyword.get(opts, :operation, "unknown")
      details = Keyword.get(opts, :details, %{})

      message =
        Keyword.get(
          opts,
          :message,
          "Network error in #{network_type} network, node '#{node_id}' during '#{operation}'"
        )

      %__MODULE__{
        message: message,
        network_type: network_type,
        node_id: node_id,
        operation: operation,
        details: details
      }
    end
  end
end
