defmodule Presto do
  @moduledoc """
  Presto: RETE Algorithm Rules Engine for Elixir

  A high-performance rules engine implementing the RETE (Rapid, Efficient, Threaded Execution)
  algorithm, designed to leverage Elixir's strengths in pattern matching, concurrency, and
  fault tolerance.

  ## Features

  - **Efficient Rule Processing**: O(RFP) complexity instead of naive O(RF^P)
  - **Incremental Processing**: Only processes changes (deltas) to working memory
  - **Concurrent Execution**: Rules can fire in parallel processes
  - **Fault Tolerance**: Supervision trees protect against component failures
  - **Hot Updates**: Rules can be modified without stopping the engine

  ## Example Usage

      # Start a rule engine
      {:ok, engine} = Presto.start_engine()
      
      # Define a rule
      rule = %{
        id: :adult_rule,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 18}
        ],
        action: fn facts -> [{:adult, facts[:name]}] end
      }
      
      # Add rule and facts
      Presto.add_rule(engine, rule)
      Presto.assert_fact(engine, {:person, "John", 25})
      
      # Execute rules
      results = Presto.fire_rules(engine)
      # => [{:adult, "John"}]
  """

  alias Presto.RuleEngine

  @type rule :: %{
          id: atom(),
          conditions: [condition()],
          action: function(),
          priority: integer()
        }

  @type condition :: tuple()
  @type fact :: tuple()
  @type rule_result :: tuple()

  # Main API

  @doc """
  Starts a new rule engine process.

  Returns `{:ok, pid}` where `pid` is the engine process identifier.

  ## Options

  No options are currently supported.

  ## Examples

      {:ok, engine} = Presto.start_engine()
  """
  @spec start_engine(keyword()) :: {:ok, pid()}
  def start_engine(opts \\ []) do
    RuleEngine.start_link(opts)
  end

  @doc """
  Stops a rule engine process.

  ## Examples

      Presto.stop_engine(engine)
  """
  @spec stop_engine(GenServer.server()) :: :ok
  def stop_engine(engine) do
    GenServer.stop(engine)
  end

  @doc """
  Adds a rule to the engine.

  Rules consist of conditions (patterns to match against facts) and an action
  (function to execute when conditions are met).

  ## Rule Structure

  - `:id` - Unique identifier for the rule (atom)
  - `:conditions` - List of condition tuples to match facts
  - `:action` - Function that receives fact bindings and returns results
  - `:priority` - Optional priority for rule execution order (higher first)

  ## Examples

      rule = %{
        id: :drinking_age,
        conditions: [
          {:person, :name, :age},
          {:age, :>, 21}
        ],
        action: fn facts -> [{:can_drink, facts[:name]}] end
      }
      
      Presto.add_rule(engine, rule)
  """
  @spec add_rule(GenServer.server(), rule()) :: :ok | {:error, term()}
  def add_rule(engine, rule) do
    RuleEngine.add_rule(engine, rule)
  end

  @doc """
  Removes a rule from the engine.

  ## Examples

      Presto.remove_rule(engine, :drinking_age)
  """
  @spec remove_rule(GenServer.server(), atom()) :: :ok
  def remove_rule(engine, rule_id) do
    RuleEngine.remove_rule(engine, rule_id)
  end

  @doc """
  Gets all rules currently in the engine.

  Returns a map of rule IDs to rule definitions.

  ## Examples

      rules = Presto.get_rules(engine)
  """
  @spec get_rules(GenServer.server()) :: %{atom() => rule()}
  def get_rules(engine) do
    RuleEngine.get_rules(engine)
  end

  @doc """
  Asserts a fact into working memory.

  Facts are tuples that represent information in the system.

  ## Examples

      Presto.assert_fact(engine, {:person, "Alice", 30})
      Presto.assert_fact(engine, {:employment, "Alice", "TechCorp"})
  """
  @spec assert_fact(GenServer.server(), fact()) :: :ok
  def assert_fact(engine, fact) do
    RuleEngine.assert_fact(engine, fact)
  end

  @doc """
  Retracts a fact from working memory.

  ## Examples

      Presto.retract_fact(engine, {:person, "Alice", 30})
  """
  @spec retract_fact(GenServer.server(), fact()) :: :ok
  def retract_fact(engine, fact) do
    RuleEngine.retract_fact(engine, fact)
  end

  @doc """
  Gets all facts currently in working memory.

  ## Examples

      facts = Presto.get_facts(engine)
  """
  @spec get_facts(GenServer.server()) :: [fact()]
  def get_facts(engine) do
    RuleEngine.get_facts(engine)
  end

  @doc """
  Clears all facts from working memory.

  ## Examples

      Presto.clear_facts(engine)
  """
  @spec clear_facts(GenServer.server()) :: :ok
  def clear_facts(engine) do
    RuleEngine.clear_facts(engine)
  end

  @doc """
  Executes all applicable rules and returns results.

  This processes facts through the RETE network and executes any rules
  whose conditions are satisfied.

  ## Options

  - `:concurrent` - Execute rules concurrently when possible (default: false)

  ## Examples

      results = Presto.fire_rules(engine)
      results = Presto.fire_rules(engine, concurrent: true)
  """
  @spec fire_rules(GenServer.server(), keyword()) :: [rule_result()]
  def fire_rules(engine, opts \\ []) do
    RuleEngine.fire_rules(engine, opts)
  end

  @doc """
  Gets statistics about rule execution performance.

  Returns detailed statistics for each rule including execution count,
  total time, and average time.

  ## Examples

      stats = Presto.get_rule_statistics(engine)
  """
  @spec get_rule_statistics(GenServer.server()) :: %{atom() => map()}
  def get_rule_statistics(engine) do
    RuleEngine.get_rule_statistics(engine)
  end

  @doc """
  Gets overall engine performance statistics.

  Returns statistics about the engine including total facts, rules,
  and execution metrics.

  ## Examples

      stats = Presto.get_engine_statistics(engine)
  """
  @spec get_engine_statistics(GenServer.server()) :: map()
  def get_engine_statistics(engine) do
    RuleEngine.get_engine_statistics(engine)
  end
end
