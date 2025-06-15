defmodule Presto.Examples.OvertimeRules do
  @moduledoc """
  Overtime rules engine implementation for Presto.

  Implements a comprehensive overtime calculation system with the following features:
  1. Calculate hours and minutes from datetime ranges
  2. Aggregate hours by employee and pay code
  3. Apply overtime rules with thresholds and pay code filters
  4. Mark processed entries as paid to prevent double-processing
  5. Generate overtime entries with proper pay codes

  This example demonstrates:
  - Complex business rule sequencing
  - State management with paid/unpaid tracking
  - Multi-stage processing with intermediate facts
  - Real-world payroll calculation scenarios

  ## Example Usage

      # Define time entries
      time_entries = [
        {:time_entry, "shift_1", %{
          start_dt: ~U[2025-01-01 12:00:00Z],
          finish_dt: ~U[2025-01-01 22:00:00Z],
          pay_code: "basic_pay",
          paid: false
        }},
        {:time_entry, "shift_2", %{
          start_dt: ~U[2025-01-02 12:00:00Z],
          finish_dt: ~U[2025-01-02 22:00:00Z],
          pay_code: "basic_pay",
          paid: false
        }}
      ]

      # Define overtime rules
      overtime_rules = [
        {:overtime_rule, "overtime_bp", %{
          threshold: 15,
          filter_pay_code: "basic_pay",
          pay_code: "overtime_basic_pay"
        }},
        {:overtime_rule, "overtime_sp", %{
          threshold: 15,
          filter_pay_code: "special_pay",
          pay_code: "overtime_special_pay"
        }},
        {:overtime_rule, "overtime", %{
          threshold: 5,
          filter_pay_code: nil,
          pay_code: "overtime_rest"
        }}
      ]

      # Process overtime
      result = OvertimeRules.process_overtime(time_entries, overtime_rules)
  """

  @behaviour Presto.RuleBehaviour

  @type time_entry :: {:time_entry, String.t(), map()}
  @type overtime_rule :: {:overtime_rule, String.t(), map()}
  @type pay_aggregate :: {:pay_aggregate, {String.t(), String.t()}, map()}
  @type overtime_entry :: {:overtime_entry, String.t(), map()}
  @type rule_variables :: map()
  @type rule_spec :: map()
  @type overtime_result :: %{
          processed_entries: [time_entry()],
          overtime_entries: [overtime_entry()],
          pay_aggregates: [pay_aggregate()],
          summary: map()
        }

  @doc """
  Creates overtime calculation rules based on the provided specification.
  """
  @spec create_rules(rule_spec()) :: [map()]
  def create_rules(rule_spec) do
    variables = extract_variables(rule_spec)

    [
      time_calculation_rule(),
      pay_aggregation_rule(),
      overtime_processing_rule(variables)
    ]
  end

  @doc """
  Rule 1: Calculate hours and minutes from datetime ranges.

  Pattern: Matches time entries with start_dt and finish_dt but no calculated values
  Action: Calculates minutes and units (hours) and updates the entry
  """
  @spec time_calculation_rule() :: map()
  def time_calculation_rule do
    %{
      name: :calculate_time_duration,
      pattern: fn facts ->
        facts
        |> Enum.filter(&time_entry_needs_calculation?/1)
      end,
      action: fn matched_facts ->
        matched_facts
        |> Enum.map(&calculate_time_duration/1)
      end
    }
  end

  @doc """
  Rule 2: Aggregate hours by employee and pay code.

  Pattern: Matches calculated time entries that haven't been aggregated
  Action: Groups by employee and pay_code, creates pay aggregates
  """
  @spec pay_aggregation_rule() :: map()
  def pay_aggregation_rule do
    %{
      name: :aggregate_pay_hours,
      pattern: fn facts ->
        # Get calculated time entries that need aggregation
        time_entries = extract_calculated_time_entries(facts)
        existing_aggregates = extract_pay_aggregates(facts)

        filter_unaggregated_entries(time_entries, existing_aggregates)
      end,
      action: fn time_entries ->
        time_entries
        |> group_by_employee_and_pay_code()
        |> create_pay_aggregates()
      end
    }
  end

  @doc """
  Rule 3: Process overtime rules in priority order.

  Pattern: Matches pay aggregates and overtime rules
  Action: Applies overtime rules, creates overtime entries, marks source entries as paid
  """
  @spec overtime_processing_rule(rule_variables()) :: map()
  def overtime_processing_rule(_variables \\ %{}) do
    %{
      name: :process_overtime_rules,
      pattern: fn facts ->
        pay_aggregates = extract_unpaid_pay_aggregates(facts)
        overtime_rules = extract_overtime_rules(facts)
        processed_rules = extract_processed_overtime_rules(facts)

        find_applicable_overtime_rules(pay_aggregates, overtime_rules, processed_rules)
      end,
      action: fn {pay_aggregates, overtime_rule} ->
        apply_overtime_rule(pay_aggregates, overtime_rule)
      end
    }
  end

  @doc """
  Processes time entries through the complete overtime calculation workflow.
  """
  @spec process_overtime([time_entry()], [overtime_rule()], rule_spec()) :: overtime_result()
  def process_overtime(time_entries, overtime_rules, _rule_spec \\ %{}) do
    # Step 1: Calculate time durations
    calculated_entries =
      time_entries
      |> Enum.map(&calculate_time_duration/1)

    # Step 2: Create pay aggregates
    pay_aggregates = create_initial_pay_aggregates(calculated_entries)

    # Step 3: Process overtime rules in sequence
    {final_aggregates, overtime_entries} =
      process_overtime_rules_sequentially(pay_aggregates, overtime_rules)

    # Step 4: Update time entries with paid status
    final_entries = update_time_entries_paid_status(calculated_entries, overtime_entries)

    # Step 5: Generate summary
    summary = generate_overtime_summary(final_entries, overtime_entries, final_aggregates)

    %{
      processed_entries: final_entries,
      overtime_entries: overtime_entries,
      pay_aggregates: final_aggregates,
      summary: summary
    }
  end

  @doc """
  Example data generator for testing and demonstration.
  """
  def generate_example_data do
    time_entries = [
      {:time_entry, "shift_1",
       %{
         start_dt: ~U[2025-01-01 12:00:00Z],
         finish_dt: ~U[2025-01-01 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "basic_pay",
         paid: false
       }},
      {:time_entry, "shift_2",
       %{
         start_dt: ~U[2025-01-02 12:00:00Z],
         finish_dt: ~U[2025-01-02 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "basic_pay",
         paid: false
       }},
      {:time_entry, "shift_3",
       %{
         start_dt: ~U[2025-01-03 12:00:00Z],
         finish_dt: ~U[2025-01-03 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "special_pay",
         paid: false
       }},
      {:time_entry, "shift_4",
       %{
         start_dt: ~U[2025-01-04 12:00:00Z],
         finish_dt: ~U[2025-01-04 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "special_pay",
         paid: false
       }},
      {:time_entry, "shift_5",
       %{
         start_dt: ~U[2025-01-05 12:00:00Z],
         finish_dt: ~U[2025-01-05 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "extra_pay",
         paid: false
       }}
    ]

    overtime_rules = [
      {:overtime_rule, "overtime_bp",
       %{
         threshold: 15,
         filter_pay_code: "basic_pay",
         pay_code: "overtime_basic_pay",
         priority: 1
       }},
      {:overtime_rule, "overtime_sp",
       %{
         threshold: 15,
         filter_pay_code: "special_pay",
         pay_code: "overtime_special_pay",
         priority: 2
       }},
      {:overtime_rule, "overtime",
       %{
         threshold: 5,
         filter_pay_code: nil,
         pay_code: "overtime_rest",
         priority: 3
       }}
    ]

    {time_entries, overtime_rules}
  end

  @doc """
  Demonstrates the complete overtime calculation workflow.
  """
  def run_example do
    IO.puts("=== Presto Overtime Rules Example ===\n")

    {time_entries, overtime_rules} = generate_example_data()

    IO.puts("Input Time Entries:")
    print_time_entries(time_entries)

    IO.puts("\nOvertime Rules:")
    print_overtime_rules(overtime_rules)

    IO.puts("\nProcessing overtime...")
    result = process_overtime(time_entries, overtime_rules)

    IO.puts("\nFinal Time Entries:")
    print_time_entries(result.processed_entries)

    IO.puts("\nGenerated Overtime Entries:")
    print_overtime_entries(result.overtime_entries)

    IO.puts("\nSummary:")
    print_summary(result.summary)

    result
  end

  # Private implementation functions

  defp time_entry_needs_calculation?(
         {:time_entry, _id, %{start_dt: start_dt, finish_dt: finish_dt, minutes: nil, units: nil}}
       )
       when not is_nil(start_dt) and not is_nil(finish_dt),
       do: true

  defp time_entry_needs_calculation?(_), do: false

  defp calculate_time_duration(
         {:time_entry, id, %{start_dt: start_dt, finish_dt: finish_dt} = data}
       ) do
    minutes = DateTime.diff(finish_dt, start_dt, :second) / 60.0
    units = minutes / 60.0

    updated_data =
      data
      |> Map.put(:minutes, Float.round(minutes, 2))
      |> Map.put(:units, Float.round(units, 2))

    {:time_entry, id, updated_data}
  end

  defp extract_calculated_time_entries(facts) do
    facts
    |> Enum.filter(fn
      {:time_entry, _, %{minutes: minutes, units: units}}
      when not is_nil(minutes) and not is_nil(units) ->
        true

      _ ->
        false
    end)
  end

  defp extract_pay_aggregates(facts) do
    facts
    |> Enum.filter(fn
      {:pay_aggregate, _, _} -> true
      _ -> false
    end)
  end

  defp filter_unaggregated_entries(time_entries, existing_aggregates) do
    existing_keys =
      existing_aggregates
      |> Enum.map(fn {:pay_aggregate, key, _} -> key end)
      |> MapSet.new()

    time_entries
    |> Enum.reject(fn {:time_entry, _, %{pay_code: pay_code}} ->
      # For simplicity, assume single employee "shift_1" - in real usage this would be employee_id
      employee_id = "employee_1"
      MapSet.member?(existing_keys, {employee_id, pay_code})
    end)
  end

  defp group_by_employee_and_pay_code(time_entries) do
    time_entries
    |> Enum.group_by(fn {:time_entry, _, %{pay_code: pay_code}} ->
      # For simplicity, assume single employee - in real usage this would be from the data
      employee_id = "employee_1"
      {employee_id, pay_code}
    end)
  end

  defp create_pay_aggregates(grouped_entries) do
    grouped_entries
    |> Enum.map(fn {{employee_id, pay_code}, entries} ->
      total_hours = calculate_total_hours(entries)
      entry_ids = extract_entry_ids(entries)

      {:pay_aggregate, {employee_id, pay_code},
       %{
         employee_id: employee_id,
         pay_code: pay_code,
         total_hours: Float.round(total_hours, 2),
         entries: entry_ids,
         paid: false,
         created_at: DateTime.utc_now()
       }}
    end)
  end

  defp create_initial_pay_aggregates(time_entries) do
    time_entries
    |> group_by_employee_and_pay_code()
    |> create_pay_aggregates()
  end

  defp extract_unpaid_pay_aggregates(facts) do
    facts
    |> Enum.filter(fn
      {:pay_aggregate, _, %{paid: false}} -> true
      _ -> false
    end)
  end

  defp extract_overtime_rules(facts) do
    facts
    |> Enum.filter(fn
      {:overtime_rule, _, _} -> true
      _ -> false
    end)
    |> Enum.sort_by(fn {:overtime_rule, _, %{priority: priority}} -> priority end)
  end

  defp extract_processed_overtime_rules(facts) do
    facts
    |> Enum.filter(fn
      {:processed_overtime_rule, _, _} -> true
      _ -> false
    end)
    |> Enum.map(fn {:processed_overtime_rule, rule_id, _} -> rule_id end)
    |> MapSet.new()
  end

  defp find_applicable_overtime_rules(pay_aggregates, overtime_rules, processed_rules) do
    # Find the next unprocessed rule that has applicable aggregates
    overtime_rules
    |> Enum.reject(fn {:overtime_rule, rule_id, _} ->
      MapSet.member?(processed_rules, rule_id)
    end)
    |> Enum.find_value(fn rule ->
      applicable_aggregates = find_applicable_aggregates(pay_aggregates, rule)
      if Enum.empty?(applicable_aggregates), do: nil, else: {applicable_aggregates, rule}
    end)
  end

  defp find_applicable_aggregates(
         pay_aggregates,
         {:overtime_rule, _, %{threshold: threshold, filter_pay_code: filter_pay_code}}
       ) do
    pay_aggregates
    |> Enum.filter(fn {:pay_aggregate, _,
                       %{pay_code: pay_code, total_hours: total_hours, paid: paid}} ->
      not paid and
        total_hours > threshold and
        (is_nil(filter_pay_code) or pay_code == filter_pay_code)
    end)
  end

  defp apply_overtime_rule(
         pay_aggregates,
         {:overtime_rule, rule_id, %{threshold: threshold, pay_code: overtime_pay_code}} = _rule
       ) do
    # Calculate total overtime hours
    total_overtime_hours =
      pay_aggregates
      |> Enum.map(fn {:pay_aggregate, _, %{total_hours: total_hours}} ->
        max(0, total_hours - threshold)
      end)
      |> Enum.sum()

    # Create overtime entry
    overtime_entry =
      {:overtime_entry, rule_id,
       %{
         id: rule_id,
         start_dt: nil,
         finish_dt: nil,
         units: Float.round(total_overtime_hours, 2),
         minutes: Float.round(total_overtime_hours * 60, 2),
         pay_code: overtime_pay_code,
         created_at: DateTime.utc_now()
       }}

    # Mark aggregates as paid
    updated_aggregates =
      pay_aggregates
      |> Enum.map(fn {:pay_aggregate, key, data} ->
        {:pay_aggregate, key, Map.put(data, :paid, true)}
      end)

    # Mark rule as processed
    processed_rule =
      {:processed_overtime_rule, rule_id,
       %{
         processed_at: DateTime.utc_now()
       }}

    [overtime_entry, processed_rule | updated_aggregates]
  end

  defp process_overtime_rules_sequentially(pay_aggregates, overtime_rules) do
    # Sort rules by priority
    sorted_rules =
      Enum.sort_by(overtime_rules, fn {:overtime_rule, _, %{priority: priority}} -> priority end)

    # Process each rule in sequence
    {final_aggregates, overtime_entries} =
      Enum.reduce(sorted_rules, {pay_aggregates, []}, fn rule,
                                                         {current_aggregates, acc_overtime} ->
        applicable_aggregates = find_applicable_aggregates(current_aggregates, rule)

        if Enum.empty?(applicable_aggregates) do
          {current_aggregates, acc_overtime}
        else
          results = apply_overtime_rule(applicable_aggregates, rule)

          # Extract the overtime entry and updated aggregates
          overtime_entry =
            Enum.find(results, fn
              {:overtime_entry, _, _} -> true
              _ -> false
            end)

          updated_aggregates =
            results
            |> Enum.filter(fn
              {:pay_aggregate, _, _} -> true
              _ -> false
            end)

          # Update the aggregates list
          new_aggregates =
            current_aggregates
            |> Enum.map(fn {type, key, _data} = aggregate ->
              case Enum.find(updated_aggregates, fn
                     {:pay_aggregate, ^key, _} -> true
                     _ -> false
                   end) do
                nil -> aggregate
                {:pay_aggregate, ^key, new_data} -> {type, key, new_data}
              end
            end)

          {new_aggregates, [overtime_entry | acc_overtime]}
        end
      end)

    {final_aggregates, Enum.reverse(overtime_entries)}
  end

  defp update_time_entries_paid_status(time_entries, overtime_entries) do
    # For this example, we'll mark entries as paid based on whether overtime was generated
    # In a real system, this would be more sophisticated
    has_overtime = not Enum.empty?(overtime_entries)

    if has_overtime do
      time_entries
      |> Enum.map(fn {:time_entry, id, data} ->
        {:time_entry, id, Map.put(data, :paid, true)}
      end)
    else
      time_entries
    end
  end

  defp calculate_total_hours(entries) do
    entries
    |> Enum.map(fn
      {:time_entry, _, %{units: units}} when is_number(units) -> units
      _ -> 0
    end)
    |> Enum.sum()
  end

  defp extract_entry_ids(entries) do
    entries
    |> Enum.map(fn {:time_entry, id, _} -> id end)
  end

  defp generate_overtime_summary(time_entries, overtime_entries, pay_aggregates) do
    total_regular_hours =
      time_entries
      |> Enum.map(fn {:time_entry, _, %{units: units}} -> units end)
      |> Enum.sum()
      |> Float.round(2)

    total_overtime_hours =
      overtime_entries
      |> Enum.map(fn {:overtime_entry, _, %{units: units}} -> units end)
      |> Enum.sum()
      |> Float.round(2)

    pay_code_breakdown =
      pay_aggregates
      |> Enum.map(fn {:pay_aggregate, _, %{pay_code: pay_code, total_hours: hours}} ->
        {pay_code, hours}
      end)
      |> Map.new()

    %{
      total_regular_hours: total_regular_hours,
      total_overtime_hours: total_overtime_hours,
      total_hours: total_regular_hours + total_overtime_hours,
      overtime_rules_triggered: length(overtime_entries),
      pay_code_breakdown: pay_code_breakdown,
      summary_generated_at: DateTime.utc_now()
    }
  end

  defp extract_variables(rule_spec) do
    case rule_spec do
      %{"rules" => rules} when is_list(rules) ->
        rules
        |> Enum.flat_map(fn rule -> Map.get(rule, "variables", %{}) |> Map.to_list() end)
        |> Map.new()

      %{"variables" => variables} ->
        variables

      _ ->
        %{}
    end
  end

  # Utility functions for display

  defp print_time_entries(entries) do
    entries
    |> Enum.each(fn {:time_entry, id, data} ->
      IO.puts("  #{id}: #{format_time_entry(data)}")
    end)
  end

  defp print_overtime_entries(entries) do
    entries
    |> Enum.each(fn {:overtime_entry, id, data} ->
      IO.puts("  #{id}: #{data.units} hours (#{data.minutes} min) - #{data.pay_code}")
    end)
  end

  defp print_overtime_rules(rules) do
    rules
    |> Enum.each(fn {:overtime_rule, id, data} ->
      filter_text =
        if data.filter_pay_code, do: "filter: #{data.filter_pay_code}", else: "no filter"

      IO.puts("  #{id}: threshold #{data.threshold}h, #{filter_text} -> #{data.pay_code}")
    end)
  end

  defp print_summary(summary) do
    IO.puts("  Regular hours: #{summary.total_regular_hours}")
    IO.puts("  Overtime hours: #{summary.total_overtime_hours}")
    IO.puts("  Total hours: #{summary.total_hours}")
    IO.puts("  Overtime rules triggered: #{summary.overtime_rules_triggered}")
    IO.puts("  Pay code breakdown: #{inspect(summary.pay_code_breakdown)}")
  end

  defp format_time_entry(%{
         start_dt: start_dt,
         finish_dt: finish_dt,
         units: units,
         minutes: minutes,
         pay_code: pay_code,
         paid: paid
       }) do
    start_str = if start_dt, do: DateTime.to_string(start_dt), else: "nil"
    finish_str = if finish_dt, do: DateTime.to_string(finish_dt), else: "nil"

    "#{start_str} to #{finish_str} | #{units}h (#{minutes}min) | #{pay_code} | paid: #{paid}"
  end

  @doc """
  Validates an overtime rule specification.
  """
  @spec valid_rule_spec?(rule_spec()) :: boolean()
  def valid_rule_spec?(rule_spec) do
    case rule_spec do
      %{"rules" => rules} when is_list(rules) ->
        Enum.all?(rules, &valid_overtime_rule?/1)

      _ ->
        false
    end
  end

  defp valid_overtime_rule?(%{"name" => name, "type" => "overtime"}) when is_binary(name),
    do: true

  defp valid_overtime_rule?(_), do: false
end
