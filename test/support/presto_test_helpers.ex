defmodule Presto.PrestoTestHelpers do
  @moduledoc """
  General test helpers for Presto RETE engine integration testing.
  """

  import ExUnit.Callbacks, only: [start_supervised!: 1]

  @doc """
  Starts a Presto engine with the given rules.
  """
  def start_engine_with_rules(rules, opts \\ []) do
    engine_opts = Keyword.merge([rules: rules], opts)

    # Use start_supervised! to ensure proper cleanup in test environment
    start_supervised!({Presto.RuleEngine, engine_opts})
  end

  @doc """
  Asserts a list of facts into the engine's working memory.
  """
  def assert_facts(engine, facts) when is_list(facts) do
    facts
    |> Enum.each(&assert_fact(engine, &1))
  end

  def assert_facts(engine, fact) do
    assert_fact(engine, fact)
  end

  @doc """
  Asserts a single fact into the engine's working memory.
  """
  def assert_fact(engine, fact) do
    :ok = Presto.RuleEngine.assert_fact(engine, fact)
  end

  @doc """
  Waits for rule execution to complete and returns the current facts.
  """
  def wait_for_rule_execution(engine, _timeout \\ 5000) do
    # In a real implementation, this would wait for the engine to reach a stable state
    # For now, we'll simulate with a small delay
    Process.sleep(100)

    get_all_facts(engine)
  end

  @doc """
  Extracts all facts matching a pattern from the engine.
  """
  def extract_facts(engine, pattern) do
    facts = get_all_facts(engine)
    filter_facts_by_pattern(facts, pattern)
  end

  @doc """
  Gets all facts from the engine's working memory.
  """
  def get_all_facts(engine) do
    Presto.RuleEngine.get_facts(engine)
  end

  @doc """
  Filters facts by a pattern (tuple matching).
  """
  def filter_facts_by_pattern(facts, pattern) when is_atom(pattern) do
    facts
    |> Enum.filter(fn
      {^pattern, _, _} -> true
      _ -> false
    end)
  end

  def filter_facts_by_pattern(facts, {pattern, key_pattern}) do
    facts
    |> Enum.filter(fn
      {^pattern, ^key_pattern, _} -> true
      {^pattern, _key, _} when key_pattern == :_ -> true
      _ -> false
    end)
  end

  @doc """
  Stops the engine and cleans up resources.
  """
  def stop_engine(engine) do
    GenServer.stop(engine)
  end

  @doc """
  Runs a test scenario with engine setup and cleanup.
  """
  def with_engine(rules, test_func, opts \\ []) do
    engine = start_engine_with_rules(rules, opts)

    try do
      test_func.(engine)
    after
      stop_engine(engine)
    end
  end

  @doc """
  Validates that expected facts are present in the engine.
  """
  def assert_facts_present(engine, expected_facts) when is_list(expected_facts) do
    actual_facts = get_all_facts(engine)

    expected_facts
    |> Enum.each(fn expected_fact ->
      assert_fact_present(actual_facts, expected_fact)
    end)
  end

  def assert_facts_present(engine, expected_fact) do
    assert_facts_present(engine, [expected_fact])
  end

  @doc """
  Validates that a specific fact is present in the fact list.
  """
  def assert_fact_present(facts, expected_fact) do
    case Enum.find(facts, &facts_match?(&1, expected_fact)) do
      nil ->
        raise "Expected fact not found: #{inspect(expected_fact)}\nActual facts: #{inspect(facts)}"

      _found ->
        :ok
    end
  end

  @doc """
  Counts facts matching a pattern.
  """
  def count_facts(engine, pattern) do
    engine
    |> extract_facts(pattern)
    |> length()
  end

  @doc """
  Validates rule execution results meet expected conditions.
  """
  def validate_rule_results(engine, validations) when is_list(validations) do
    Enum.each(validations, &validate_single_result(engine, &1))
  end

  defp validate_single_result(engine, {:count, pattern, expected_count}) do
    actual_count = count_facts(engine, pattern)

    if actual_count != expected_count do
      raise "Expected #{expected_count} facts matching #{inspect(pattern)}, got #{actual_count}"
    end
  end

  defp validate_single_result(engine, {:present, fact}) do
    assert_facts_present(engine, fact)
  end

  defp validate_single_result(engine, {:absent, pattern}) do
    count = count_facts(engine, pattern)

    if count > 0 do
      raise "Expected no facts matching #{inspect(pattern)}, but found #{count}"
    end
  end

  @doc """
  Creates a test rule specification for domain-specific testing.
  """
  def create_test_rule_spec(rule_type, variables \\ %{}) do
    case rule_type do
      :payroll ->
        %{
          name: "test_payroll_rule",
          type: "payroll",
          pattern: "time_entry",
          action: "calculate_and_process",
          variables: Map.merge(%{"overtime_threshold" => 40.0}, variables)
        }

      :compliance ->
        %{
          name: "test_compliance_rule",
          type: "compliance",
          pattern: "weekly_hours",
          action: "check_compliance",
          variables: Map.merge(%{"max_weekly_hours" => 48.0}, variables)
        }
    end
  end

  @doc """
  Simulates rule execution cycle for testing.
  """
  def run_rule_cycle(engine, opts \\ []) do
    _max_cycles = Keyword.get(opts, :max_cycles, 10)
    timeout = Keyword.get(opts, :timeout, 5000)

    # Simulate rule engine cycles
    Process.sleep(50)

    wait_for_rule_execution(engine, timeout)
  end

  # Private functions

  defp facts_match?(actual_fact, expected_fact) do
    case {actual_fact, expected_fact} do
      # Exact match
      {fact, fact} ->
        true

      # Pattern match with wildcards
      {{type, _key, data}, {type, :_, expected_data}} ->
        maps_match?(data, expected_data)

      {{type, key, data}, {type, key, expected_data}} ->
        maps_match?(data, expected_data)

      _ ->
        false
    end
  end

  defp maps_match?(actual, expected) when is_map(actual) and is_map(expected) do
    expected
    |> Enum.all?(fn {key, expected_value} ->
      case Map.get(actual, key) do
        ^expected_value -> true
        _actual_value when expected_value == :_ -> true
        _ -> false
      end
    end)
  end

  defp maps_match?(actual, expected), do: actual == expected
end
