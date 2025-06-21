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

  # Bulk Operations

  @doc """
  Asserts multiple facts at once.

  More efficient than asserting facts one by one when dealing with large datasets.

  ## Examples

      facts = [
        {:person, "Alice", 25},
        {:person, "Bob", 30},
        {:employment, "Alice", "TechCorp"},
        {:employment, "Bob", "StartupInc"}
      ]
      
      :ok = Presto.assert_facts(engine, facts)
  """
  @spec assert_facts(GenServer.server(), [fact()]) :: :ok
  def assert_facts(engine, facts) when is_list(facts) do
    RuleEngine.assert_facts_bulk(engine, facts)
  end

  @doc """
  Retracts multiple facts at once.

  More efficient than retracting facts one by one when dealing with large datasets.

  ## Examples

      facts_to_remove = [
        {:person, "Alice", 25},
        {:employment, "Alice", "TechCorp"}
      ]
      
      :ok = Presto.retract_facts(engine, facts_to_remove)
  """
  @spec retract_facts(GenServer.server(), [fact()]) :: :ok
  def retract_facts(engine, facts) when is_list(facts) do
    RuleEngine.retract_facts_bulk(engine, facts)
  end

  @doc """
  Adds multiple rules at once.

  ## Examples

      rules = [
        Presto.Rule.new(:rule1, conditions1, action1),
        Presto.Rule.new(:rule2, conditions2, action2)
      ]
      
      :ok = Presto.add_rules(engine, rules)
  """
  @spec add_rules(GenServer.server(), [rule()]) :: :ok | {:error, term()}
  def add_rules(engine, rules) when is_list(rules) do
    results = Enum.map(rules, &add_rule(engine, &1))

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> :ok
      error -> error
    end
  end

  @doc """
  Executes a complete batch operation with rules, facts, and execution in one call.

  This is the most efficient way to set up and execute a rule engine scenario.

  ## Examples

      result = Presto.execute_batch(engine,
        rules: [rule1, rule2],
        facts: [fact1, fact2, fact3],
        opts: [concurrent: true]
      )
  """
  @spec execute_batch(GenServer.server(), keyword()) :: [rule_result()]
  def execute_batch(engine, opts) do
    rules = Keyword.get(opts, :rules, [])
    facts = Keyword.get(opts, :facts, [])
    execution_opts = Keyword.get(opts, :opts, [])

    # Add rules if provided
    unless Enum.empty?(rules) do
      :ok = add_rules(engine, rules)
    end

    # Add facts if provided  
    unless Enum.empty?(facts) do
      :ok = assert_facts(engine, facts)
    end

    # Execute rules
    fire_rules(engine, execution_opts)
  end

  @doc """
  Creates and configures a new engine with rules and facts in one operation.

  ## Examples

      {:ok, engine, results} = Presto.create_and_execute(
        rules: [rule1, rule2],
        facts: [fact1, fact2],
        opts: [concurrent: true]
      )
  """
  @spec create_and_execute(keyword()) :: {:ok, pid(), [rule_result()]}
  def create_and_execute(opts) do
    {:ok, engine} = start_engine()
    results = execute_batch(engine, opts)
    {:ok, engine, results}
  end

  # Query Interface

  @doc """
  Queries facts using pattern matching without executing rules.

  This provides an ad-hoc query interface for examining the current state
  of working memory.

  ## Examples

      # Query all person facts
      people = Presto.query(engine, {:person, :_, :_})

      # Query people over 18
      adults = Presto.query(engine, {:person, :name, :age}, age: {:>, 18})

      # Query with multiple conditions
      results = Presto.query(engine, {:employment, :name, :company}, 
        name: "Alice", 
        company: {:match, ~r/Tech/}
      )
  """
  @spec query(GenServer.server(), tuple(), keyword()) :: [map()]
  def query(engine, pattern, conditions \\ []) do
    RuleEngine.query_facts(engine, pattern, conditions)
  end

  @doc """
  Executes a complex query with joins across multiple fact types.

  ## Examples

      # Find people and their employment information
      results = Presto.query_join(engine, [
        {:person, :name, :age},
        {:employment, :name, :company}
      ], join_on: [:name])
  """
  @spec query_join(GenServer.server(), [tuple()], keyword()) :: [map()]
  def query_join(engine, patterns, opts \\ []) do
    RuleEngine.query_facts_join(engine, patterns, opts)
  end

  @doc """
  Counts facts matching a pattern.

  ## Examples

      count = Presto.count_facts(engine, {:person, :_, :_})
      adult_count = Presto.count_facts(engine, {:person, :name, :age}, age: {:>, 18})
  """
  @spec count_facts(GenServer.server(), tuple(), keyword()) :: non_neg_integer()
  def count_facts(engine, pattern, conditions \\ []) do
    RuleEngine.count_facts(engine, pattern, conditions)
  end

  @doc """
  Explains how a fact would match against current rules.

  Useful for debugging rule behavior.

  ## Examples

      explanation = Presto.explain_fact(engine, {:person, "John", 25})
  """
  @spec explain_fact(GenServer.server(), tuple()) :: map()
  def explain_fact(engine, fact) do
    RuleEngine.explain_fact(engine, fact)
  end

  # Introspection and Debugging Tools

  @doc """
  Gets detailed information about a specific rule including execution statistics
  and network structure.

  ## Examples

      info = Presto.inspect_rule(engine, :adult_rule)
  """
  @spec inspect_rule(GenServer.server(), atom()) :: map()
  def inspect_rule(engine, rule_id) do
    RuleEngine.inspect_rule(engine, rule_id)
  end

  @doc """
  Gets comprehensive engine diagnostics including memory usage, performance
  metrics, and system health indicators.

  ## Examples

      diagnostics = Presto.diagnostics(engine)
  """
  @spec diagnostics(GenServer.server()) :: map()
  def diagnostics(engine) do
    RuleEngine.get_diagnostics(engine)
  end

  @doc """
  Profiles rule execution and returns detailed performance breakdown.

  ## Examples

      profile = Presto.profile_execution(engine, rules: [:rule1, :rule2])
  """
  @spec profile_execution(GenServer.server(), keyword()) :: map()
  def profile_execution(engine, opts \\ []) do
    RuleEngine.profile_execution(engine, opts)
  end

  @doc """
  Traces the execution path of facts through the RETE network.

  ## Examples

      trace = Presto.trace_fact(engine, {:person, "John", 25})
  """
  @spec trace_fact(GenServer.server(), tuple()) :: map()
  def trace_fact(engine, fact) do
    RuleEngine.trace_fact_execution(engine, fact)
  end

  @doc """
  Visualizes the current RETE network structure for debugging.

  Returns a map suitable for rendering network diagrams.

  ## Examples

      network = Presto.visualize_network(engine)
  """
  @spec visualize_network(GenServer.server()) :: map()
  def visualize_network(engine) do
    RuleEngine.get_network_visualization(engine)
  end

  @doc """
  Gets performance recommendations based on current usage patterns.

  ## Examples

      recommendations = Presto.performance_recommendations(engine)
  """
  @spec performance_recommendations(GenServer.server()) :: [map()]
  def performance_recommendations(engine) do
    RuleEngine.analyze_performance_recommendations(engine)
  end
end
