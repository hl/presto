defmodule Presto.RuleBehaviour do
  @moduledoc """
  Defines the behaviour that rule modules must implement.

  Rule modules provide the business logic for processing facts and generating
  new facts based on rule specifications. They act as the bridge between
  JSON rule configurations and the RETE engine.
  """

  @doc """
  Creates rule definitions based on the provided rule specification.

  Takes a rule specification (typically from JSON) and returns a list of
  rule definitions that the RETE engine can execute.

  ## Parameters
  - `rule_spec` - A map containing rule configuration, typically with
    "rules_to_run" and "variables" keys

  ## Returns
  A list of rule definition maps, each containing:
  - `:name` - atom identifying the rule
  - `:pattern` - function that matches facts
  - `:action` - function that processes matched facts
  """
  @callback create_rules(rule_spec :: map()) :: [map()]

  @doc """
  Validates that a rule specification is valid for this rule module.

  This is optional - if not implemented, the registry will assume
  all rule specifications are valid for this module.

  ## Parameters
  - `rule_spec` - A map containing rule configuration

  ## Returns
  `true` if the rule specification is valid, `false` otherwise
  """
  @callback valid_rule_spec?(rule_spec :: map()) :: boolean()

  @optional_callbacks [valid_rule_spec?: 1]
end
