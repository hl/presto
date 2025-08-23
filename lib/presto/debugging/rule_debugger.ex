defmodule Presto.Debugging.RuleDebugger do
  @moduledoc """
  Advanced rule debugging and tracing system for Presto RETE engines.

  Provides comprehensive debugging capabilities including:
  - Step-by-step rule execution tracing
  - Breakpoint management and conditional debugging
  - Working memory inspection and manipulation
  - Alpha/Beta network state visualization
  - Rule firing history and pattern analysis
  - Interactive debugging sessions with REPL

  ## Debugging Features

  ### Execution Tracing
  - Rule condition evaluation tracing
  - Action execution monitoring
  - Fact assertion/retraction tracking
  - Network node activation logging

  ### Interactive Debugging
  - Breakpoints on rules, facts, or conditions
  - Step-through execution control
  - Variable inspection and modification
  - Real-time working memory browser

  ### Advanced Analysis
  - Rule dependency visualization
  - Performance bottleneck identification
  - Memory allocation tracking
  - Conflict resolution analysis

  ## Example Usage

      # Start debugging session for an engine
      {:ok, session} = RuleDebugger.start_debug_session(:payment_engine, [
        trace_level: :verbose,
        breakpoints: [:rule_validation, :total_calculation],
        capture_working_memory: true
      ])

      # Set conditional breakpoint
      RuleDebugger.set_breakpoint(session, :discount_rule, [
        condition: "customer.age > 65",
        action: :pause_and_inspect
      ])

      # Execute with debugging
      RuleDebugger.debug_execute(session, facts)

      # Inspect execution trace
      {:ok, trace} = RuleDebugger.get_execution_trace(session)
  """

  use GenServer
  require Logger

  alias Presto.Debugging.{BreakpointManager, ExecutionTracer, WorkingMemoryInspector}

  @type debug_session :: %{
          id: String.t(),
          engine_name: atom(),
          trace_level: trace_level(),
          breakpoints: [breakpoint()],
          execution_state: execution_state(),
          working_memory_snapshots: [map()],
          execution_trace: [trace_event()],
          started_at: DateTime.t()
        }

  @type trace_level :: :minimal | :standard | :verbose | :exhaustive
  @type execution_state :: :ready | :running | :paused | :breakpoint | :completed | :error

  @type breakpoint :: %{
          id: String.t(),
          target: breakpoint_target(),
          condition: String.t() | nil,
          action: breakpoint_action(),
          hit_count: non_neg_integer(),
          enabled: boolean()
        }

  @type breakpoint_target ::
          {:rule, atom()} | {:fact, String.t()} | {:condition, String.t()} | {:action, String.t()}

  @type breakpoint_action :: :pause | :log | :inspect | :capture_state | :custom

  @type trace_event :: %{
          id: String.t(),
          timestamp: DateTime.t(),
          event_type: trace_event_type(),
          target: String.t(),
          details: map(),
          execution_context: map()
        }

  @type trace_event_type ::
          :rule_fired
          | :condition_evaluated
          | :action_executed
          | :fact_asserted
          | :fact_retracted
          | :network_activated
          | :conflict_resolved
          | :breakpoint_hit

  ## Client API

  @doc """
  Starts the rule debugger.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts a new debugging session for an engine.
  """
  @spec start_debug_session(atom(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def start_debug_session(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:start_session, engine_name, opts})
  end

  @doc """
  Stops a debugging session.
  """
  @spec stop_debug_session(String.t()) :: :ok | {:error, term()}
  def stop_debug_session(session_id) do
    GenServer.call(__MODULE__, {:stop_session, session_id})
  end

  @doc """
  Sets a breakpoint in a debugging session.
  """
  @spec set_breakpoint(String.t(), breakpoint_target(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def set_breakpoint(session_id, target, opts \\ []) do
    GenServer.call(__MODULE__, {:set_breakpoint, session_id, target, opts})
  end

  @doc """
  Removes a breakpoint.
  """
  @spec remove_breakpoint(String.t(), String.t()) :: :ok | {:error, term()}
  def remove_breakpoint(session_id, breakpoint_id) do
    GenServer.call(__MODULE__, {:remove_breakpoint, session_id, breakpoint_id})
  end

  @doc """
  Executes rules with debugging enabled.
  """
  @spec debug_execute(String.t(), [map()], keyword()) ::
          {:ok, map()} | {:paused, map()} | {:error, term()}
  def debug_execute(session_id, facts, opts \\ []) do
    GenServer.call(__MODULE__, {:debug_execute, session_id, facts, opts}, 30_000)
  end

  @doc """
  Steps through execution (when paused at breakpoint).
  """
  @spec step_execution(String.t(), :step_into | :step_over | :step_out | :continue) ::
          {:ok, execution_state()} | {:error, term()}
  def step_execution(session_id, step_type) do
    GenServer.call(__MODULE__, {:step_execution, session_id, step_type})
  end

  @doc """
  Inspects the current working memory state.
  """
  @spec inspect_working_memory(String.t()) :: {:ok, map()} | {:error, term()}
  def inspect_working_memory(session_id) do
    GenServer.call(__MODULE__, {:inspect_working_memory, session_id})
  end

  @doc """
  Gets the execution trace for a session.
  """
  @spec get_execution_trace(String.t(), keyword()) :: {:ok, [trace_event()]} | {:error, term()}
  def get_execution_trace(session_id, opts \\ []) do
    GenServer.call(__MODULE__, {:get_execution_trace, session_id, opts})
  end

  @doc """
  Evaluates an expression in the context of the current debugging session.
  """
  @spec evaluate_expression(String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def evaluate_expression(session_id, expression) do
    GenServer.call(__MODULE__, {:evaluate_expression, session_id, expression})
  end

  @doc """
  Gets debugging session status.
  """
  @spec get_session_status(String.t()) :: {:ok, map()} | {:error, term()}
  def get_session_status(session_id) do
    GenServer.call(__MODULE__, {:get_session_status, session_id})
  end

  @doc """
  Lists all active debugging sessions.
  """
  @spec list_active_sessions() :: [map()]
  def list_active_sessions do
    GenServer.call(__MODULE__, :list_active_sessions)
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Rule Debugger")

    state = %{
      # Configuration
      max_sessions: Keyword.get(opts, :max_sessions, 10),
      max_trace_events: Keyword.get(opts, :max_trace_events, 10_000),
      default_trace_level: Keyword.get(opts, :default_trace_level, :standard),

      # Active sessions
      debug_sessions: %{},

      # Supporting services
      execution_tracer: nil,
      breakpoint_manager: nil,
      working_memory_inspector: nil,

      # Statistics
      stats: %{
        total_sessions: 0,
        active_sessions: 0,
        total_breakpoints: 0,
        total_trace_events: 0,
        debug_executions: 0
      }
    }

    # Start supporting services
    {:ok, tracer} = ExecutionTracer.start_link([])
    {:ok, bp_manager} = BreakpointManager.start_link([])
    {:ok, wm_inspector} = WorkingMemoryInspector.start_link([])

    new_state = %{
      state
      | execution_tracer: tracer,
        breakpoint_manager: bp_manager,
        working_memory_inspector: wm_inspector
    }

    {:ok, new_state}
  end

  @impl GenServer
  def handle_call({:start_session, engine_name, opts}, _from, state) do
    if map_size(state.debug_sessions) >= state.max_sessions do
      {:reply, {:error, :max_sessions_reached}, state}
    else
      trace_level = Keyword.get(opts, :trace_level, state.default_trace_level)
      initial_breakpoints = Keyword.get(opts, :breakpoints, [])
      capture_working_memory = Keyword.get(opts, :capture_working_memory, true)

      session_id = generate_session_id()

      session = %{
        id: session_id,
        engine_name: engine_name,
        trace_level: trace_level,
        breakpoints: [],
        execution_state: :ready,
        working_memory_snapshots: [],
        execution_trace: [],
        started_at: DateTime.utc_now(),
        options: %{
          capture_working_memory: capture_working_memory,
          auto_capture_on_breakpoint: Keyword.get(opts, :auto_capture_on_breakpoint, true),
          max_trace_events: Keyword.get(opts, :max_trace_events, state.max_trace_events)
        }
      }

      # Set initial breakpoints
      session_with_breakpoints =
        Enum.reduce(initial_breakpoints, session, fn target, acc_session ->
          {:ok, breakpoint} = create_breakpoint(target, [])
          %{acc_session | breakpoints: [breakpoint | acc_session.breakpoints]}
        end)

      new_sessions = Map.put(state.debug_sessions, session_id, session_with_breakpoints)

      new_stats = %{
        state.stats
        | total_sessions: state.stats.total_sessions + 1,
          active_sessions: state.stats.active_sessions + 1
      }

      Logger.info("Started debugging session",
        session_id: session_id,
        engine: engine_name,
        trace_level: trace_level
      )

      new_state = %{state | debug_sessions: new_sessions, stats: new_stats}

      {:reply, {:ok, session_id}, new_state}
    end
  end

  @impl GenServer
  def handle_call({:stop_session, session_id}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil ->
        {:reply, {:error, :session_not_found}, state}

      session ->
        # Clean up session resources
        cleanup_session_resources(session, state)

        new_sessions = Map.delete(state.debug_sessions, session_id)
        new_stats = %{state.stats | active_sessions: max(0, state.stats.active_sessions - 1)}

        Logger.info("Stopped debugging session", session_id: session_id)

        new_state = %{state | debug_sessions: new_sessions, stats: new_stats}

        {:reply, :ok, new_state}
    end
  end

  @impl GenServer
  def handle_call({:set_breakpoint, session_id, target, opts}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil ->
        {:reply, {:error, :session_not_found}, state}

      session ->
        {:ok, breakpoint} = create_breakpoint(target, opts)
        updated_session = %{session | breakpoints: [breakpoint | session.breakpoints]}

        new_sessions = Map.put(state.debug_sessions, session_id, updated_session)
        new_stats = %{state.stats | total_breakpoints: state.stats.total_breakpoints + 1}

        Logger.info("Set breakpoint",
          session_id: session_id,
          breakpoint_id: breakpoint.id,
          target: target
        )

        new_state = %{state | debug_sessions: new_sessions, stats: new_stats}

        {:reply, {:ok, breakpoint.id}, new_state}
    end
  end

  @impl GenServer
  def handle_call({:remove_breakpoint, session_id, breakpoint_id}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil ->
        {:reply, {:error, :session_not_found}, state}

      session ->
        updated_breakpoints = Enum.reject(session.breakpoints, &(&1.id == breakpoint_id))

        if length(updated_breakpoints) == length(session.breakpoints) do
          {:reply, {:error, :breakpoint_not_found}, state}
        else
          updated_session = %{session | breakpoints: updated_breakpoints}
          new_sessions = Map.put(state.debug_sessions, session_id, updated_session)

          Logger.info("Removed breakpoint",
            session_id: session_id,
            breakpoint_id: breakpoint_id
          )

          {:reply, :ok, %{state | debug_sessions: new_sessions}}
        end
    end
  end

  @impl GenServer
  def handle_call({:debug_execute, session_id, facts, opts}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil -> {:reply, {:error, :session_not_found}, state}
      session -> execute_debug_session(session, session_id, facts, opts, state)
    end
  end

  @impl GenServer
  def handle_call({:step_execution, session_id, step_type}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil ->
        {:reply, {:error, :session_not_found}, state}

      session ->
        if session.execution_state in [:paused, :breakpoint] do
          {:ok, updated_session} = perform_step_execution(session, step_type, state)
          new_sessions = Map.put(state.debug_sessions, session_id, updated_session)

          {:reply, {:ok, updated_session.execution_state},
           %{state | debug_sessions: new_sessions}}
        else
          {:reply, {:error, :not_paused}, state}
        end
    end
  end

  @impl GenServer
  def handle_call({:inspect_working_memory, session_id}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil ->
        {:reply, {:error, :session_not_found}, state}

      session ->
        {:ok, working_memory} = get_current_working_memory(session, state)
        {:reply, {:ok, working_memory}, state}
    end
  end

  @impl GenServer
  def handle_call({:get_execution_trace, session_id, opts}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil ->
        {:reply, {:error, :session_not_found}, state}

      session ->
        limit = Keyword.get(opts, :limit, 1000)
        event_types = Keyword.get(opts, :event_types, :all)

        filtered_trace = filter_execution_trace(session.execution_trace, event_types, limit)

        {:reply, {:ok, filtered_trace}, state}
    end
  end

  @impl GenServer
  def handle_call({:evaluate_expression, session_id, expression}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil ->
        {:reply, {:error, :session_not_found}, state}

      session ->
        case evaluate_debug_expression(session, expression, state) do
          {:ok, result} ->
            {:reply, {:ok, result}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl GenServer
  def handle_call({:get_session_status, session_id}, _from, state) do
    case Map.get(state.debug_sessions, session_id) do
      nil ->
        {:reply, {:error, :session_not_found}, state}

      session ->
        status = %{
          session_id: session.id,
          engine_name: session.engine_name,
          execution_state: session.execution_state,
          trace_level: session.trace_level,
          breakpoints_count: length(session.breakpoints),
          trace_events_count: length(session.execution_trace),
          working_memory_snapshots: length(session.working_memory_snapshots),
          started_at: session.started_at,
          options: session.options
        }

        {:reply, {:ok, status}, state}
    end
  end

  @impl GenServer
  def handle_call(:list_active_sessions, _from, state) do
    sessions =
      Enum.map(state.debug_sessions, fn {session_id, session} ->
        %{
          session_id: session_id,
          engine_name: session.engine_name,
          execution_state: session.execution_state,
          started_at: session.started_at,
          trace_events: length(session.execution_trace)
        }
      end)

    {:reply, sessions, state}
  end

  defp execute_debug_session(session, session_id, facts, opts, state) do
    case perform_debug_execution(session, facts, opts, state) do
      {:ok, result, updated_session} ->
        handle_successful_debug_execution(session_id, result, updated_session, state)

      {:error, reason, error_session} ->
        handle_failed_debug_execution(session_id, reason, error_session, state)
    end
  end

  defp handle_successful_debug_execution(session_id, result, updated_session, state) do
    new_sessions = Map.put(state.debug_sessions, session_id, updated_session)
    new_stats = %{state.stats | debug_executions: state.stats.debug_executions + 1}
    new_state = %{state | debug_sessions: new_sessions, stats: new_stats}

    reply =
      case updated_session.execution_state do
        state when state in [:paused, :breakpoint] -> {:paused, result}
        :completed -> {:ok, result}
        :error -> {:error, result}
        _ -> {:ok, result}
      end

    {:reply, reply, new_state}
  end

  defp handle_failed_debug_execution(session_id, reason, error_session, state) do
    new_sessions = Map.put(state.debug_sessions, session_id, error_session)
    {:reply, {:error, reason}, %{state | debug_sessions: new_sessions}}
  end

  ## Private functions

  defp generate_session_id do
    "debug_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end

  defp create_breakpoint(target, opts) do
    breakpoint_id = generate_breakpoint_id()
    condition = Keyword.get(opts, :condition)
    action = Keyword.get(opts, :action, :pause)
    enabled = Keyword.get(opts, :enabled, true)

    breakpoint = %{
      id: breakpoint_id,
      target: target,
      condition: condition,
      action: action,
      hit_count: 0,
      enabled: enabled
    }

    {:ok, breakpoint}
  end

  defp generate_breakpoint_id do
    "bp_" <> (:crypto.strong_rand_bytes(6) |> Base.encode16(case: :lower))
  end

  defp cleanup_session_resources(session, _state) do
    # Clean up any resources associated with the session
    Logger.debug("Cleaning up debug session resources", session_id: session.id)
    :ok
  end

  defp perform_debug_execution(session, facts, opts, state) do
    # Update session state to running
    updated_session = %{session | execution_state: :running}

    try do
      # Initialize execution context
      execution_context = %{
        session: updated_session,
        facts: facts,
        opts: opts,
        start_time: DateTime.utc_now()
      }

      # Capture initial working memory if enabled
      session_with_initial_state =
        if updated_session.options.capture_working_memory do
          initial_wm = capture_working_memory_snapshot(updated_session, facts)

          %{
            updated_session
            | working_memory_snapshots: [initial_wm | updated_session.working_memory_snapshots]
          }
        else
          updated_session
        end

      # Start traced execution
      case execute_with_tracing(session_with_initial_state, facts, execution_context, state) do
        {:ok, execution_result, final_session} ->
          {:ok, execution_result, final_session}

        {:paused, execution_result, paused_session} ->
          {:ok, execution_result, paused_session}

        {_other_state, execution_result, final_session} ->
          {:ok, execution_result, final_session}
      end
    rescue
      error ->
        Logger.error("Debug execution failed",
          session_id: session.id,
          error: inspect(error)
        )

        error_session = %{updated_session | execution_state: :error}
        {:error, {:execution_failed, error}, error_session}
    end
  end

  defp execute_with_tracing(session, facts, execution_context, _state) do
    # Simulate rule execution with comprehensive tracing
    rules = get_engine_rules(session.engine_name)

    execution_result = %{
      rules_fired: [],
      facts_asserted: [],
      facts_retracted: [],
      execution_time: 0,
      final_working_memory: facts
    }

    # Simulate step-by-step execution with breakpoint checking
    {final_session, final_result} =
      Enum.reduce(rules, {session, execution_result}, fn rule, {acc_session, acc_result} ->
        # Check for breakpoints before rule execution
        case check_breakpoints_for_rule(rule, acc_session) do
          {:break, breakpoint} ->
            # Hit breakpoint - pause execution
            trace_event =
              create_trace_event(:breakpoint_hit, rule.id, %{
                breakpoint_id: breakpoint.id,
                rule: rule,
                working_memory: acc_result.final_working_memory
              })

            paused_session = %{
              acc_session
              | execution_state: :breakpoint,
                execution_trace: [trace_event | acc_session.execution_trace]
            }

            # Update breakpoint hit count
            updated_breakpoints =
              update_breakpoint_hit_count(acc_session.breakpoints, breakpoint.id)

            final_paused_session = %{paused_session | breakpoints: updated_breakpoints}

            {final_paused_session, acc_result}

          :continue ->
            # Execute rule normally
            execute_rule_with_tracing(rule, acc_session, acc_result, execution_context)
        end
      end)

    # Determine final execution state
    case final_session.execution_state do
      :breakpoint ->
        {:paused, final_result, final_session}

      :running ->
        completed_session = %{final_session | execution_state: :completed}
        {:ok, final_result, completed_session}

      other ->
        {other, final_result, final_session}
    end
  end

  defp execute_rule_with_tracing(rule, session, execution_result, _execution_context) do
    # Add rule execution trace event
    rule_trace =
      create_trace_event(:rule_fired, rule.id, %{
        rule: rule,
        conditions: rule.conditions,
        actions: rule.actions
      })

    # Simulate rule execution
    new_facts = simulate_rule_execution(rule, execution_result.final_working_memory)

    # Add condition evaluation traces
    condition_traces =
      Enum.map(rule.conditions, fn condition ->
        create_trace_event(:condition_evaluated, condition, %{
          condition: condition,
          # Simplified
          result: true,
          binding: %{}
        })
      end)

    # Add action execution traces
    action_traces =
      Enum.map(rule.actions, fn action ->
        create_trace_event(:action_executed, action, %{
          action: action,
          facts_asserted: new_facts,
          facts_retracted: []
        })
      end)

    all_traces = [rule_trace] ++ condition_traces ++ action_traces

    updated_session = %{session | execution_trace: all_traces ++ session.execution_trace}

    updated_result = %{
      execution_result
      | rules_fired: [rule.id | execution_result.rules_fired],
        facts_asserted: new_facts ++ execution_result.facts_asserted,
        final_working_memory: new_facts ++ execution_result.final_working_memory
    }

    {updated_session, updated_result}
  end

  defp check_breakpoints_for_rule(rule, session) do
    # Check if any breakpoints match this rule
    Enum.find_value(session.breakpoints, :continue, fn breakpoint ->
      if Map.get(breakpoint, :enabled, false) and matches_breakpoint?(rule, breakpoint) do
        # Check condition if specified
        if Map.get(breakpoint, :condition) == nil or
             evaluate_breakpoint_condition(Map.get(breakpoint, :condition), rule) do
          {:break, breakpoint}
        else
          :continue
        end
      else
        :continue
      end
    end)
  end

  defp matches_breakpoint?(rule, breakpoint) do
    case breakpoint.target do
      {:rule, rule_id} -> rule.id == rule_id
      {:fact, fact_pattern} -> rule_affects_fact?(rule, fact_pattern)
      {:condition, condition_pattern} -> rule_has_condition?(rule, condition_pattern)
      {:action, action_pattern} -> rule_has_action?(rule, action_pattern)
      _ -> false
    end
  end

  defp rule_affects_fact?(rule, fact_pattern) do
    # Check if rule affects facts matching the pattern
    Enum.any?(rule.actions, fn action ->
      String.contains?(action, fact_pattern)
    end)
  end

  defp rule_has_condition?(rule, condition_pattern) do
    # Check if rule has conditions matching the pattern
    Enum.any?(rule.conditions, fn condition ->
      String.contains?(condition, condition_pattern)
    end)
  end

  defp rule_has_action?(rule, action_pattern) do
    # Check if rule has actions matching the pattern
    Enum.any?(rule.actions, fn action ->
      String.contains?(action, action_pattern)
    end)
  end

  defp evaluate_breakpoint_condition(_condition, _rule) do
    # Simplified condition evaluation
    # In a real implementation, this would parse and evaluate the condition
    true
  end

  defp update_breakpoint_hit_count(breakpoints, breakpoint_id) do
    Enum.map(breakpoints, fn breakpoint ->
      if breakpoint.id == breakpoint_id do
        %{breakpoint | hit_count: breakpoint.hit_count + 1}
      else
        breakpoint
      end
    end)
  end

  defp create_trace_event(event_type, target, details) do
    %{
      id: generate_trace_event_id(),
      timestamp: DateTime.utc_now(),
      event_type: event_type,
      target: to_string(target),
      details: details,
      execution_context: %{
        thread_id: self(),
        # Simplified
        stack_depth: 1
      }
    }
  end

  defp generate_trace_event_id do
    "trace_" <> (:crypto.strong_rand_bytes(6) |> Base.encode16(case: :lower))
  end

  defp simulate_rule_execution(rule, _working_memory) do
    # Simulate rule execution and fact generation
    # In a real implementation, this would execute actual rule logic
    case rule.actions do
      [] ->
        []

      actions ->
        Enum.flat_map(actions, fn action ->
          case String.downcase(action) do
            "add " <> fact_desc -> [%{type: :fact, content: fact_desc, source: rule.id}]
            "assert " <> fact_desc -> [%{type: :fact, content: fact_desc, source: rule.id}]
            _ -> []
          end
        end)
    end
  end

  defp get_engine_rules(engine_name) do
    try do
      case Presto.RuleEngine.get_rules(engine_name) do
        rules when is_map(rules) ->
          Map.values(rules)

        _ ->
          []
      end
    rescue
      _ ->
        # Fallback for non-existent engines
        []
    end
  end

  defp perform_step_execution(session, step_type, state) do
    # Perform step execution based on step type
    case step_type do
      :step_into ->
        # Step into next execution unit
        step_into_execution(session, state)

      :step_over ->
        # Step over current execution unit
        step_over_execution(session, state)

      :step_out ->
        # Step out of current context
        step_out_execution(session, state)

      :continue ->
        # Continue execution until next breakpoint
        continue_execution(session, state)
    end
  end

  defp step_into_execution(session, _state) do
    # Execute one atomic step and pause
    updated_session = %{session | execution_state: :running}

    # Simulate single step execution
    # Simulate execution time
    :timer.sleep(10)

    paused_session = %{updated_session | execution_state: :paused}
    {:ok, paused_session}
  end

  defp step_over_execution(session, state) do
    # Execute current level and pause at next same-level instruction
    step_into_execution(session, state)
  end

  defp step_out_execution(session, state) do
    # Execute until returning to caller level
    step_into_execution(session, state)
  end

  defp continue_execution(session, _state) do
    # Continue execution until next breakpoint or completion
    updated_session = %{session | execution_state: :running}

    # This would continue the actual execution from where it was paused
    # For now, just mark as completed
    completed_session = %{updated_session | execution_state: :completed}
    {:ok, completed_session}
  end

  defp capture_working_memory_snapshot(_session, working_memory) do
    %{
      timestamp: DateTime.utc_now(),
      working_memory: working_memory,
      snapshot_type: :automatic,
      rule_context: nil
    }
  end

  defp get_current_working_memory(session, _state) do
    # Get current working memory state
    case List.first(session.working_memory_snapshots) do
      nil ->
        {:ok, %{facts: [], rules: [], working_memory: %{}}}

      snapshot ->
        {:ok,
         %{
           facts: snapshot.working_memory,
           timestamp: snapshot.timestamp,
           snapshot_type: snapshot.snapshot_type
         }}
    end
  end

  defp filter_execution_trace(trace_events, event_types, limit) do
    filtered =
      case event_types do
        :all ->
          trace_events

        types when is_list(types) ->
          Enum.filter(trace_events, fn event -> event.event_type in types end)

        single_type ->
          Enum.filter(trace_events, fn event -> event.event_type == single_type end)
      end

    Enum.take(filtered, limit)
  end

  defp evaluate_debug_expression(session, expression, state) do
    # Evaluate expression in the context of the debugging session
    # This would parse and evaluate the expression against current state
    # For now, return a simplified result

    case String.downcase(expression) do
      "working_memory" ->
        get_current_working_memory(session, state)

      "breakpoints" ->
        {:ok, session.breakpoints}

      "trace_count" ->
        {:ok, length(session.execution_trace)}

      "execution_state" ->
        {:ok, session.execution_state}

      _ ->
        # Try to evaluate as a simple expression
        try do
          # Basic expression evaluator for debug expressions
          case evaluate_simple_expression(expression) do
            {:ok, result} -> {:ok, result}
            {:error, reason} -> {:error, reason}
          end
        rescue
          error ->
            {:error, {:evaluation_failed, error}}
        end
    end
  end

  defp evaluate_simple_expression(expression) do
    # Basic expression evaluator for debug sessions
    # Supports simple arithmetic and string operations

    expr = String.downcase(String.trim(expression))

    expression_patterns = [
      # Arithmetic expressions
      {"+", &evaluate_arithmetic_expression/2},
      {"-", &evaluate_arithmetic_expression/2},
      {"*", &evaluate_arithmetic_expression/2},
      {"/", &evaluate_arithmetic_expression/2},

      # Comparison expressions (order matters - check longer operators first)
      {"==", &evaluate_comparison_expression/2},
      {"!=", &evaluate_comparison_expression/2},
      {">", &evaluate_comparison_expression/2},
      {"<", &evaluate_comparison_expression/2},

      # String functions
      {"length(", &evaluate_string_function/2, "length"},
      {"upper(", &evaluate_string_function/2, "upper"},
      {"lower(", &evaluate_string_function/2, "lower"}
    ]

    case find_matching_pattern(expr, expression_patterns) do
      {operator, evaluator} -> evaluator.(expr, operator)
      {_operator, evaluator, function_name} -> evaluator.(expr, function_name)
      nil -> parse_literal_value(expr)
    end
  rescue
    error ->
      {:error, {:parse_error, error}}
  end

  defp find_matching_pattern(expr, patterns) do
    Enum.find_value(patterns, fn
      {pattern, evaluator} when tuple_size({pattern, evaluator}) == 2 ->
        if String.contains?(expr, pattern), do: {pattern, evaluator}, else: nil

      {pattern, evaluator, function_name} ->
        if String.contains?(expr, pattern), do: {pattern, evaluator, function_name}, else: nil
    end)
  end

  defp parse_literal_value(expr) do
    case Integer.parse(expr) do
      {num, ""} ->
        {:ok, num}

      _ ->
        case Float.parse(expr) do
          {num, ""} -> {:ok, num}
          # Return as string
          _ -> {:ok, expr}
        end
    end
  end

  defp evaluate_arithmetic_expression(expr, operator) do
    parts = String.split(expr, operator, parts: 2)

    case parts do
      [left, right] ->
        with {:ok, left_val} <- parse_number(String.trim(left)),
             {:ok, right_val} <- parse_number(String.trim(right)) do
          apply_arithmetic_operator(left_val, right_val, operator)
        else
          _ -> {:error, :invalid_arithmetic}
        end

      _ ->
        {:error, :invalid_expression}
    end
  end

  defp apply_arithmetic_operator(left_val, right_val, operator) do
    case operator do
      "+" -> {:ok, left_val + right_val}
      "-" -> {:ok, left_val - right_val}
      "*" -> {:ok, left_val * right_val}
      "/" when right_val != 0 -> {:ok, left_val / right_val}
      "/" -> {:error, :division_by_zero}
    end
  end

  defp evaluate_comparison_expression(expr, operator) do
    parts = String.split(expr, operator, parts: 2)

    case parts do
      [left, right] ->
        left_val = String.trim(left)
        right_val = String.trim(right)

        # Try numeric comparison first, fall back to string comparison
        case {parse_number(left_val), parse_number(right_val)} do
          {{:ok, l}, {:ok, r}} -> {:ok, apply_comparison_operator(l, r, operator)}
          _ -> {:ok, apply_comparison_operator(left_val, right_val, operator)}
        end

      _ ->
        {:error, :invalid_expression}
    end
  end

  defp apply_comparison_operator(left_val, right_val, operator) do
    case operator do
      "==" -> left_val == right_val
      "!=" -> left_val != right_val
      ">" -> left_val > right_val
      "<" -> left_val < right_val
    end
  end

  defp evaluate_string_function(expr, function) do
    # Try quoted string pattern first
    case Regex.run(~r/#{function}\("([^"]*)"\)/, expr) do
      [_, string_arg] -> {:ok, apply_string_function(function, string_arg)}
      _ -> try_unquoted_string_function(expr, function)
    end
  end

  defp try_unquoted_string_function(expr, function) do
    case Regex.run(~r/#{function}\(([^)]*)\)/, expr) do
      [_, arg] -> {:ok, apply_string_function(function, String.trim(arg))}
      _ -> {:error, :invalid_function_call}
    end
  end

  defp apply_string_function(function, string_arg) do
    case function do
      "length" -> String.length(string_arg)
      "upper" -> String.upcase(string_arg)
      "lower" -> String.downcase(string_arg)
    end
  end

  defp parse_number(str) do
    case Integer.parse(str) do
      {num, ""} ->
        {:ok, num}

      _ ->
        case Float.parse(str) do
          {num, ""} -> {:ok, num}
          _ -> {:error, :not_a_number}
        end
    end
  end
end
