defmodule Presto.TestRuleModule do
  @moduledoc """
  Test rule module that implements Presto.RuleBehaviour for testing purposes.
  """

  @behaviour Presto.RuleBehaviour

  @impl true
  def create_rules(_rule_spec) do
    [
      %{
        name: :test_rule,
        pattern: fn _facts -> [] end,
        action: fn _facts -> [] end
      }
    ]
  end

  @impl true
  def valid_rule_spec?(rule_spec) when is_map(rule_spec), do: true
  def valid_rule_spec?(_), do: false
end
