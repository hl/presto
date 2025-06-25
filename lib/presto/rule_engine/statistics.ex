defmodule Presto.RuleEngine.Statistics do
  @moduledoc """
  Statistics and performance tracking for the Presto Rule Engine.

  This module handles all statistics collection and management operations, including:
  - Execution statistics tracking
  - Rule-specific performance metrics
  - Engine-wide statistics
  - ETS-based concurrent statistics updates
  - Performance analysis and reporting

  The module provides both real-time statistics updates and batch collection
  operations for efficient performance monitoring.
  """

  require Logger
  alias Presto.RuleEngine.State

  @doc """
  Updates execution statistics after a rule firing cycle.

  This function updates the engine-wide statistics with execution time
  and result count information.
  """
  @spec update_execution_statistics(State.t(), integer(), integer()) :: State.t()
  def update_execution_statistics(%State{} = state, execution_time, result_count) do
    new_engine_stats =
      state.engine_statistics
      |> Map.put(:last_execution_time, execution_time)
      |> Map.update!(:total_rule_firings, &(&1 + result_count))

    %{state | engine_statistics: new_engine_stats}
  end

  @doc """
  Updates rule statistics using ETS for efficient concurrent access.

  This function uses atomic ETS operations for thread-safe statistics updates.
  """
  @spec update_rule_statistics(State.t(), atom(), integer(), integer()) :: :ok
  def update_rule_statistics(%State{} = state, rule_id, execution_time, facts_processed) do
    stats_table = State.get_rule_statistics_table(state)

    # Performance optimization: Use ETS counters for efficient statistics updates
    # Default tuple: {rule_id, executions, total_time, facts_processed}
    default_tuple = {rule_id, 0, 0, 0}

    # Use atomic counter operations for thread-safe updates
    try do
      :ets.update_counter(
        stats_table,
        rule_id,
        [
          # increment executions
          {2, 1},
          # add to total_time
          {3, execution_time},
          # add to facts_processed
          {4, facts_processed}
        ],
        default_tuple
      )
    catch
      :error, :badarg ->
        # Table might not exist during shutdown, ignore silently
        :ok
    end

    :ok
  end

  @doc """
  Collects rule statistics from ETS into the state.

  This function gathers statistics for the specified rules and updates
  the in-memory statistics map in the state.
  """
  @spec collect_rule_statistics(State.t(), [atom()]) :: State.t()
  def collect_rule_statistics(%State{} = state, rule_ids) do
    stats_table = State.get_rule_statistics_table(state)

    updated_rule_stats =
      Enum.reduce(rule_ids, state.rule_statistics, fn rule_id, acc_stats ->
        build_rule_stats_from_ets(rule_id, stats_table, acc_stats)
      end)

    %{state | rule_statistics: updated_rule_stats}
  end

  @doc """
  Gets rule statistics for a specific rule.
  """
  @spec get_rule_statistics(State.t(), atom()) :: map() | nil
  def get_rule_statistics(%State{} = state, rule_id) do
    Map.get(state.rule_statistics, rule_id)
  end

  @doc """
  Gets all rule statistics from the state.
  """
  @spec get_all_rule_statistics(State.t()) :: %{atom() => map()}
  def get_all_rule_statistics(%State{} = state), do: state.rule_statistics

  @doc """
  Gets engine statistics from the state.
  """
  @spec get_engine_statistics(State.t()) :: map()
  def get_engine_statistics(%State{} = state) do
    Map.put(state.engine_statistics, :engine_id, state.engine_id)
  end

  @doc """
  Updates the total facts count in engine statistics.
  """
  @spec update_total_facts(State.t(), integer()) :: State.t()
  def update_total_facts(%State{} = state, delta) do
    new_engine_stats = Map.update!(state.engine_statistics, :total_facts, &(&1 + delta))
    %{state | engine_statistics: new_engine_stats}
  end

  @doc """
  Resets the total facts count in engine statistics.
  """
  @spec reset_total_facts(State.t()) :: State.t()
  def reset_total_facts(%State{} = state) do
    new_engine_stats = Map.put(state.engine_statistics, :total_facts, 0)
    %{state | engine_statistics: new_engine_stats}
  end

  @doc """
  Updates optimization-specific statistics.
  """
  @spec update_optimization_statistics(State.t(), [tuple()], [tuple()]) :: State.t()
  def update_optimization_statistics(%State{} = state, fast_path_rules, rete_rules) do
    new_engine_stats =
      state.engine_statistics
      |> Map.update!(:fast_path_executions, &(&1 + length(fast_path_rules)))
      |> Map.update!(:rete_network_executions, &(&1 + length(rete_rules)))

    %{state | engine_statistics: new_engine_stats}
  end

  @doc """
  Gets performance metrics summary.
  """
  @spec get_performance_summary(State.t()) :: map()
  def get_performance_summary(%State{} = state) do
    engine_stats = state.engine_statistics
    rule_stats = state.rule_statistics

    # Calculate averages and aggregates
    total_executions = Map.values(rule_stats) |> Enum.map(&(&1[:executions] || 0)) |> Enum.sum()

    total_execution_time =
      Map.values(rule_stats) |> Enum.map(&(&1[:total_time] || 0)) |> Enum.sum()

    avg_execution_time =
      if total_executions > 0 do
        div(total_execution_time, total_executions)
      else
        0
      end

    most_executed_rule =
      rule_stats
      |> Enum.max_by(fn {_rule_id, stats} -> stats[:executions] || 0 end, fn -> {nil, %{}} end)
      |> elem(0)

    slowest_rule =
      rule_stats
      |> Enum.max_by(fn {_rule_id, stats} -> stats[:average_time] || 0 end, fn -> {nil, %{}} end)
      |> elem(0)

    %{
      # Engine-wide metrics
      total_facts: engine_stats.total_facts,
      total_rules: engine_stats.total_rules,
      total_rule_firings: engine_stats.total_rule_firings,
      last_execution_time: engine_stats.last_execution_time,

      # Optimization metrics
      fast_path_executions: engine_stats.fast_path_executions,
      rete_network_executions: engine_stats.rete_network_executions,
      fast_path_ratio:
        if engine_stats.fast_path_executions + engine_stats.rete_network_executions > 0 do
          engine_stats.fast_path_executions /
            (engine_stats.fast_path_executions + engine_stats.rete_network_executions)
        else
          0
        end,

      # Rule-specific aggregates
      total_rule_executions: total_executions,
      average_execution_time: avg_execution_time,
      most_executed_rule: most_executed_rule,
      slowest_rule: slowest_rule,

      # Performance indicators
      rules_with_statistics: map_size(rule_stats),
      avg_executions_per_rule:
        if map_size(rule_stats) > 0 do
          total_executions / map_size(rule_stats)
        else
          0
        end
    }
  end

  @doc """
  Gets rule performance rankings.
  """
  @spec get_rule_performance_rankings(State.t()) :: map()
  def get_rule_performance_rankings(%State{} = state) do
    rule_stats = state.rule_statistics

    # Sort rules by different metrics
    by_executions =
      rule_stats
      |> Enum.sort_by(fn {_rule_id, stats} -> stats[:executions] || 0 end, :desc)
      |> Enum.take(10)
      |> Enum.map(fn {rule_id, _stats} -> rule_id end)

    by_total_time =
      rule_stats
      |> Enum.sort_by(fn {_rule_id, stats} -> stats[:total_time] || 0 end, :desc)
      |> Enum.take(10)
      |> Enum.map(fn {rule_id, _stats} -> rule_id end)

    by_average_time =
      rule_stats
      |> Enum.sort_by(fn {_rule_id, stats} -> stats[:average_time] || 0 end, :desc)
      |> Enum.take(10)
      |> Enum.map(fn {rule_id, _stats} -> rule_id end)

    by_facts_processed =
      rule_stats
      |> Enum.sort_by(fn {_rule_id, stats} -> stats[:facts_processed] || 0 end, :desc)
      |> Enum.take(10)
      |> Enum.map(fn {rule_id, _stats} -> rule_id end)

    %{
      most_executed: by_executions,
      highest_total_time: by_total_time,
      highest_average_time: by_average_time,
      most_facts_processed: by_facts_processed
    }
  end

  @doc """
  Resets all rule statistics.
  """
  @spec reset_rule_statistics(State.t()) :: State.t()
  def reset_rule_statistics(%State{} = state) do
    # Clear ETS table
    :ets.delete_all_objects(State.get_rule_statistics_table(state))

    # Clear in-memory statistics
    %{state | rule_statistics: %{}}
  end

  @doc """
  Resets engine statistics to initial values.
  """
  @spec reset_engine_statistics(State.t()) :: State.t()
  def reset_engine_statistics(%State{} = state) do
    initial_stats = %{
      total_facts: 0,
      total_rules: map_size(state.rules),
      total_rule_firings: 0,
      last_execution_time: 0,
      fast_path_executions: 0,
      rete_network_executions: 0,
      alpha_nodes_saved_by_sharing: 0
    }

    %{state | engine_statistics: initial_stats}
  end

  @doc """
  Gets comprehensive statistics report.
  """
  @spec get_statistics_report(State.t()) :: map()
  def get_statistics_report(%State{} = state) do
    %{
      engine_statistics: get_engine_statistics(state),
      rule_statistics: get_all_rule_statistics(state),
      performance_summary: get_performance_summary(state),
      performance_rankings: get_rule_performance_rankings(state),
      timestamp: System.system_time(:microsecond)
    }
  end

  @doc """
  Initializes statistics for a rule.
  """
  @spec initialize_rule_statistics(State.t(), atom(), map()) :: State.t()
  def initialize_rule_statistics(%State{} = state, rule_id, initial_stats) do
    updated_rule_statistics = Map.put(state.rule_statistics, rule_id, initial_stats)
    %{state | rule_statistics: updated_rule_statistics}
  end

  @doc """
  Removes statistics for a rule.
  """
  @spec remove_rule_statistics(State.t(), atom()) :: State.t()
  def remove_rule_statistics(%State{} = state, rule_id) do
    # Remove from ETS table
    :ets.delete(State.get_rule_statistics_table(state), rule_id)

    # Remove from in-memory statistics
    updated_rule_statistics = Map.delete(state.rule_statistics, rule_id)
    %{state | rule_statistics: updated_rule_statistics}
  end

  @doc """
  Restores statistics from a snapshot.
  """
  @spec restore_statistics(State.t(), map()) :: State.t()
  def restore_statistics(%State{} = state, statistics_data) do
    try do
      # Restore engine statistics
      updated_state =
        case Map.get(statistics_data, :engine_statistics) do
          engine_stats when is_map(engine_stats) ->
            %{state | engine_statistics: engine_stats}

          _ ->
            state
        end

      # Restore rule statistics
      final_state =
        case Map.get(statistics_data, :rule_statistics) do
          rule_stats when is_map(rule_stats) ->
            # Clear existing rule statistics
            stats_table = State.get_rule_statistics_table(updated_state)
            :ets.delete_all_objects(stats_table)

            # Restore rule statistics to ETS and in-memory
            Enum.reduce(rule_stats, updated_state, fn {rule_id, stats}, acc_state ->
              # Add to ETS table
              :ets.insert(stats_table, {rule_id, stats})

              # Add to in-memory statistics
              updated_rule_stats = Map.put(acc_state.rule_statistics, rule_id, stats)
              %{acc_state | rule_statistics: updated_rule_stats}
            end)

          _ ->
            updated_state
        end

      final_state
    rescue
      _error ->
        # If restoration fails, return the original state
        state
    end
  end

  # Private functions

  defp build_rule_stats_from_ets(rule_id, stats_table, acc_stats) do
    case :ets.lookup(stats_table, rule_id) do
      [] ->
        # No stats for this rule yet
        acc_stats

      [{^rule_id, executions, total_time, facts_processed}] ->
        # Calculate average time from ETS counters
        average_time = if executions > 0, do: div(total_time, executions), else: 0

        updated_stats = %{
          executions: executions,
          total_time: total_time,
          average_time: average_time,
          facts_processed: facts_processed
        }

        Map.put(acc_stats, rule_id, updated_stats)
    end
  end
end
