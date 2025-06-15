defmodule Presto.Examples.PayrollRules do
  @moduledoc """
  Payroll rules engine implementation for Presto.

  This example demonstrates how to use the Presto RETE engine for comprehensive
  payroll processing workflows. It implements a payroll calculation system with
  time tracking and overtime processing capabilities.

  Features implemented using the RETE engine:
  1. Calculate time duration between start and finish datetimes
  2. Calculate overtime when total units exceed threshold
  3. Weekly aggregation and employee grouping
  4. Automatic overtime entry generation

  This example demonstrates:
  - Using the actual Presto RETE engine for rule processing
  - Pattern matching on facts to trigger rule execution
  - Incremental processing as new facts are asserted
  - Complex payroll workflows with the engine
  - Proper fact structures for the RETE algorithm
  """

  @behaviour Presto.RuleBehaviour

  @type time_entry :: {:time_entry, any(), map()}
  @type overtime_entry :: {:overtime_entry, any(), map()}
  @type rule_variables :: map()
  @type rule_spec :: map()
  @type payroll_result :: %{
          processed_entries: [time_entry()],
          overtime_entries: [overtime_entry()],
          summary: map()
        }

  @doc """
  Creates payroll rules for the Presto RETE engine.

  Returns a list of rule definitions that can be added to a Presto engine.
  Each rule uses pattern matching on facts and creates new facts when conditions are met.

  The rule_spec can include:
  - variables: Configuration variables for payroll processing
  - overtime_threshold: Hours threshold for overtime calculation (default 40.0)

  ## Example rule_spec:

      %{
        "variables" => %{
          "overtime_threshold" => 40.0,
          "calculation_precision" => 2
        }
      }
  """
  @spec create_rules(rule_spec()) :: [map()]
  def create_rules(rule_spec) do
    variables = extract_variables(rule_spec)

    [
      time_calculation_rule(),
      overtime_calculation_rule(variables)
    ]
  end

  @doc """
  Rule 1: Calculate time duration for time entries.

  Uses RETE pattern matching to identify time entries that need duration calculation.
  This rule triggers when time entry facts with start_datetime and finish_datetime are asserted.
  """
  @spec time_calculation_rule() :: map()
  def time_calculation_rule do
    %{
      id: :calculate_time_duration,
      conditions: [
        {:time_entry, :id, :data}
      ],
      action: fn facts ->
        data = facts[:data]
        id = facts[:id]

        # Only process if we have datetime fields and no calculated values
        if Map.has_key?(data, :start_datetime) and Map.has_key?(data, :finish_datetime) and
             Map.get(data, :minutes) == nil and Map.get(data, :units) == nil do
          minutes = calculate_minutes_between(data.start_datetime, data.finish_datetime)
          units = minutes / 60.0

          updated_data = %{
            data
            | minutes: Float.round(minutes, 2),
              units: Float.round(units, 2)
          }

          # Return both the updated entry and a processed marker fact
          [
            {:time_entry, id, updated_data},
            {:processed_time_entry, id, updated_data}
          ]
        else
          # Return empty list if no processing needed (original fact remains)
          []
        end
      end,
      priority: 100
    }
  end

  @doc """
  Rule 2: Calculate overtime when total units exceed threshold.

  Uses RETE pattern matching to find calculated time entries that exceed overtime thresholds.
  Triggers when we have time entries with calculated units that surpass the weekly limit.
  """
  @spec overtime_calculation_rule(rule_variables()) :: map()
  def overtime_calculation_rule(variables \\ %{}) do
    overtime_threshold = Map.get(variables, "overtime_threshold", 40.0)

    %{
      id: :calculate_overtime,
      conditions: [
        {:processed_time_entry, :id, :data}
      ],
      action: fn facts ->
        data = facts[:data]
        id = facts[:id]

        # Only process if we have calculated units and employee_id
        if Map.has_key?(data, :units) and Map.has_key?(data, :employee_id) and
             not is_nil(data.units) and data.units > overtime_threshold do
          employee_id = data.employee_id
          units = data.units
          start_dt = data.start_datetime

          # Calculate week start for grouping
          week_start = start_dt |> DateTime.to_date() |> week_start_date()

          overtime_units = units - overtime_threshold

          overtime_data = %{
            employee_id: employee_id,
            units: Float.round(overtime_units, 2),
            week_start: week_start,
            type: :overtime,
            source_entry: id,
            created_at: DateTime.utc_now()
          }

          [{:overtime_entry, {employee_id, week_start}, overtime_data}]
        else
          # No overtime needed, return empty list
          []
        end
      end,
      priority: 90
    }
  end

  @doc """
  Processes time entries using the Presto RETE engine.

  This function demonstrates how to use the actual Presto engine with payroll rules:
  1. Start the engine
  2. Add payroll calculation rules
  3. Assert initial facts (time entries)
  4. Fire rules to process the data
  5. Extract results from working memory

  This showcases the incremental processing capabilities of the RETE algorithm
  as facts are asserted and rules fire in response to pattern matches.
  """
  @spec process_with_engine([time_entry()], rule_spec()) :: payroll_result()
  def process_with_engine(time_entries, rule_spec \\ %{}) do
    # Start the Presto RETE engine
    {:ok, engine} = Presto.start_engine()

    try do
      # Create and add payroll rules to the engine
      variables = extract_variables(rule_spec)

      rules = [
        time_calculation_rule(),
        overtime_calculation_rule(variables)
      ]

      Enum.each(rules, &Presto.add_rule(engine, &1))

      # Assert initial facts into working memory
      # Time entries
      Enum.each(time_entries, fn {:time_entry, id, data} ->
        Presto.assert_fact(engine, {:time_entry, id, data})
      end)

      # Fire rules to process the data through the RETE network
      rule_results = Presto.fire_rules(engine, concurrent: true)

      # Process overtime manually since rules don't chain automatically in this engine
      overtime_threshold = Map.get(variables, "overtime_threshold", 40.0)
      overtime_results = process_overtime_from_results(rule_results, overtime_threshold)

      all_results = rule_results ++ overtime_results
      extract_payroll_results(all_results)
    after
      # Clean up the engine
      Presto.stop_engine(engine)
    end
  end

  @doc """
  Processes a list of time entries through the payroll rules.

  This is the original implementation that processes data directly without the RETE engine.
  For demonstrations of the actual RETE engine, use process_with_engine/2.
  """
  @spec process_time_entries([time_entry()], rule_spec()) :: payroll_result()
  def process_time_entries(time_entries, rule_spec \\ %{}) do
    variables = extract_variables(rule_spec)

    # Step 1: Calculate time durations
    processed_entries =
      time_entries
      |> Enum.map(&calculate_time_duration/1)

    # Step 2: Calculate overtime
    overtime_entries = calculate_overtime_entries(processed_entries, variables)

    # Return both processed entries and overtime entries
    %{
      processed_entries: processed_entries,
      overtime_entries: overtime_entries,
      summary: generate_summary(processed_entries, overtime_entries)
    }
  end

  # Private functions for rule implementation

  @spec calculate_time_duration(time_entry()) :: time_entry()
  defp calculate_time_duration(
         {:time_entry, id, %{start_datetime: start_dt, finish_datetime: finish_dt} = data}
       ) do
    minutes = calculate_minutes_between(start_dt, finish_dt)
    units = minutes / 60.0

    updated_data =
      data
      |> Map.put(:minutes, minutes)
      |> Map.put(:units, units)

    {:time_entry, id, updated_data}
  end

  @spec calculate_minutes_between(DateTime.t(), DateTime.t()) :: float()
  defp calculate_minutes_between(start_datetime, finish_datetime) do
    (DateTime.diff(finish_datetime, start_datetime, :second) / 60.0)
    |> Float.round(2)
  end

  @spec group_by_employee([time_entry()]) :: %{any() => [time_entry()]}
  defp group_by_employee(time_entries) do
    time_entries
    |> Enum.group_by(fn {:time_entry, _, %{employee_id: emp_id, start_datetime: start_dt}} ->
      week_start = start_dt |> DateTime.to_date() |> week_start_date()
      {emp_id, week_start}
    end)
  end

  @spec filter_overtime_candidates(%{{any(), Date.t()} => [time_entry()]}, float()) :: [
          {any(), float(), Date.t()}
        ]
  defp filter_overtime_candidates(grouped_entries, threshold) do
    grouped_entries
    |> Enum.map(fn {{employee_id, week_start}, entries} ->
      total_units = calculate_total_units(entries)
      {employee_id, total_units, week_start}
    end)
    |> Enum.filter(fn {_employee_id, total_units, _week_start} ->
      total_units > threshold
    end)
  end

  @spec calculate_total_units([time_entry()]) :: float()
  defp calculate_total_units(entries) do
    entries
    |> Enum.map(fn {:time_entry, _, %{units: units}} -> units end)
    |> Enum.sum()
    |> then(&Float.round(&1 * 1.0, 2))
  end

  @spec week_start_date(Date.t()) :: Date.t()
  defp week_start_date(date) do
    days_since_monday = Date.day_of_week(date) - 1
    Date.add(date, -days_since_monday)
  end

  @spec create_overtime_entry(any(), float(), Date.t()) :: overtime_entry()
  defp create_overtime_entry(employee_id, overtime_units, week_start) do
    {:overtime_entry, {employee_id, week_start},
     %{
       employee_id: employee_id,
       units: Float.round(overtime_units, 2),
       week_start: week_start,
       type: :overtime,
       created_at: DateTime.utc_now()
     }}
  end

  @spec calculate_overtime_entries([time_entry()], rule_variables()) :: [overtime_entry()]
  defp calculate_overtime_entries(processed_entries, variables) do
    threshold = Map.get(variables, "overtime_threshold", 40.0)

    processed_entries
    |> group_by_employee()
    |> filter_overtime_candidates(threshold)
    |> Enum.map(fn {employee_id, total_units, week_start} ->
      overtime_units = total_units - threshold
      create_overtime_entry(employee_id, overtime_units, week_start)
    end)
  end

  @spec extract_variables(rule_spec()) :: rule_variables()
  defp extract_variables(rule_spec) do
    case rule_spec do
      # Handle nested JSON format from integration tests
      %{"rules" => rules} when is_list(rules) ->
        rules
        |> Enum.flat_map(fn rule -> Map.get(rule, "variables", %{}) |> Map.to_list() end)
        |> Map.new()

      # Handle direct format
      %{"variables" => variables} ->
        variables

      # Default empty variables
      _ ->
        %{}
    end
  end

  @spec generate_summary([time_entry()], [overtime_entry()]) :: map()
  defp generate_summary(processed_entries, overtime_entries) do
    total_employees =
      processed_entries
      |> Enum.map(fn {:time_entry, _, %{employee_id: emp_id}} -> emp_id end)
      |> Enum.uniq()
      |> length()

    total_regular_hours =
      processed_entries
      |> Enum.map(fn {:time_entry, _, %{units: units}} -> units end)
      |> Enum.sum()
      |> then(&Float.round(&1 * 1.0, 2))

    total_overtime_hours =
      overtime_entries
      |> Enum.map(fn {:overtime_entry, _, %{units: units}} -> units end)
      |> Enum.sum()
      |> then(&Float.round(&1 * 1.0, 2))

    employees_with_overtime =
      overtime_entries
      |> Enum.map(fn {:overtime_entry, _, %{employee_id: emp_id}} -> emp_id end)
      |> Enum.uniq()
      |> length()

    %{
      total_employees: total_employees,
      total_regular_hours: total_regular_hours,
      total_overtime_hours: total_overtime_hours,
      employees_with_overtime: employees_with_overtime,
      total_hours: total_regular_hours + total_overtime_hours
    }
  end

  @doc """
  Validates a payroll rule specification.

  Checks that the rule specification contains the required structure
  for payroll processing and has valid variable types.
  """
  @spec valid_rule_spec?(rule_spec()) :: boolean()
  def valid_rule_spec?(rule_spec) do
    case rule_spec do
      # Handle rules_to_run format
      %{"rules_to_run" => rules, "variables" => variables}
      when is_list(rules) and is_map(variables) ->
        validate_rules_format(rules, variables)

      # Handle new JSON format
      %{"rules" => rules} when is_list(rules) ->
        validate_json_format(rules)

      # Invalid format
      _ ->
        false
    end
  end

  defp validate_rules_format(rules, variables) do
    # Check that we have valid payroll rule names
    valid_payroll_rules = ["time_calculation", "overtime_check"]
    payroll_rules_present = Enum.any?(rules, &(&1 in valid_payroll_rules))

    # Check that variables have valid types for payroll processing
    valid_variables =
      case variables do
        %{"overtime_threshold" => threshold} when is_number(threshold) and threshold > 0 -> true
        # Empty variables are ok
        %{} -> true
      end

    payroll_rules_present && valid_variables
  end

  defp validate_json_format(rules) do
    Enum.empty?(rules) or Enum.all?(rules, &valid_payroll_rule?/1)
  end

  defp valid_payroll_rule?(%{"name" => name, "type" => "payroll"}) when is_binary(name), do: true
  defp valid_payroll_rule?(_), do: false

  @doc """
  Example data generator for testing and demonstration.
  """
  def generate_example_data do
    time_entries = [
      {:time_entry, "entry_1",
       %{
         employee_id: "emp_001",
         start_datetime: ~U[2025-01-13 09:00:00Z],
         finish_datetime: ~U[2025-01-13 17:00:00Z],
         minutes: nil,
         units: nil
       }},
      {:time_entry, "entry_2",
       %{
         employee_id: "emp_001",
         start_datetime: ~U[2025-01-14 09:00:00Z],
         finish_datetime: ~U[2025-01-14 17:00:00Z],
         minutes: nil,
         units: nil
       }},
      {:time_entry, "entry_3",
       %{
         employee_id: "emp_002",
         start_datetime: ~U[2025-01-13 08:00:00Z],
         finish_datetime: ~U[2025-01-13 20:00:00Z],
         minutes: nil,
         units: nil
       }},
      {:time_entry, "entry_4",
       %{
         employee_id: "emp_003",
         start_datetime: ~U[2025-01-15 10:00:00Z],
         finish_datetime: ~U[2025-01-15 15:00:00Z],
         minutes: nil,
         units: nil
       }}
    ]

    {time_entries}
  end

  @doc """
  Example rule specification for payroll processing.
  """
  def generate_example_rule_spec do
    %{
      "variables" => %{
        "overtime_threshold" => 8.0,
        "calculation_precision" => 2
      }
    }
  end

  @doc """
  Demonstrates the Presto RETE engine with payroll rules.
  """
  def run_example do
    IO.puts("=== Presto RETE Engine Payroll Example ===\n")

    {time_entries} = generate_example_data()

    IO.puts("Input Data:")
    IO.puts("Time Entries: #{length(time_entries)} entries")

    IO.puts("\nProcessing with Presto RETE engine...")
    result = process_with_engine(time_entries)

    IO.puts("\nResults from RETE Engine:")
    print_engine_results(result)

    result
  end

  @doc """
  Demonstrates custom payroll configuration with the RETE engine.
  """
  def run_custom_example do
    IO.puts("=== Custom Payroll Rules with RETE Engine ===\n")

    {time_entries} = generate_example_data()
    rule_spec = generate_example_rule_spec()

    IO.puts("Custom Rule Specification:")
    IO.puts("  Overtime Threshold: #{rule_spec["variables"]["overtime_threshold"]} hours")
    IO.puts("  Calculation Precision: #{rule_spec["variables"]["calculation_precision"]}")

    IO.puts("\nProcessing with custom rules using RETE engine...")
    result = process_with_engine(time_entries, rule_spec)

    IO.puts("\nCustom Payroll Results:")
    print_engine_results(result)

    result
  end

  # Helper functions for RETE engine processing

  defp process_overtime_from_results(rule_results, overtime_threshold) do
    # Find processed time entries with calculated units
    processed_entries =
      rule_results
      |> Enum.filter(fn
        {:processed_time_entry, _, _} -> true
        _ -> false
      end)

    # Create overtime entries for those exceeding threshold
    processed_entries
    |> Enum.filter(fn {:processed_time_entry, _, data} ->
      Map.get(data, :units, 0) > overtime_threshold
    end)
    |> Enum.map(fn {:processed_time_entry, id, data} ->
      employee_id = data.employee_id
      units = data.units
      start_dt = data.start_datetime

      # Calculate week start for grouping
      week_start = start_dt |> DateTime.to_date() |> week_start_date()

      overtime_units = units - overtime_threshold

      overtime_data = %{
        employee_id: employee_id,
        units: Float.round(overtime_units, 2),
        week_start: week_start,
        type: :overtime,
        source_entry: id,
        created_at: DateTime.utc_now()
      }

      {:overtime_entry, {employee_id, week_start}, overtime_data}
    end)
  end

  defp extract_payroll_results(all_facts) do
    # Get all time entries and filter for the most recent versions (with calculated units)
    all_time_entries = extract_facts_by_type(all_facts, :time_entry)

    # Get the latest version of each time entry (the one with calculated units)
    processed_entries =
      all_time_entries
      |> Enum.group_by(fn {:time_entry, id, _data} -> id end)
      |> Enum.map(fn {_id, entries} ->
        # Get the entry with calculated units, or the first one if none have units
        Enum.find(entries, List.first(entries), fn {:time_entry, _, data} ->
          not is_nil(Map.get(data, :units))
        end)
      end)

    overtime_entries = extract_facts_by_type(all_facts, :overtime_entry)

    summary = generate_engine_summary(processed_entries, overtime_entries)

    %{
      processed_entries: processed_entries,
      overtime_entries: overtime_entries,
      summary: summary
    }
  end

  defp extract_facts_by_type(facts, type) do
    facts
    |> Enum.filter(fn
      {^type, _, _} -> true
      _ -> false
    end)
  end

  defp generate_engine_summary(processed_entries, overtime_entries) do
    total_employees =
      processed_entries
      |> Enum.map(fn
        {:time_entry, _, %{employee_id: emp_id}} -> emp_id
        _ -> "unknown"
      end)
      |> Enum.uniq()
      |> length()

    total_regular_hours =
      processed_entries
      |> Enum.map(fn
        {:time_entry, _, %{units: units}} when is_number(units) -> units
        _ -> 0
      end)
      |> Enum.sum()
      |> then(fn
        sum when is_float(sum) -> Float.round(sum, 2)
        sum -> sum
      end)

    total_overtime_hours =
      overtime_entries
      |> Enum.map(fn
        {:overtime_entry, _, %{units: units}} when is_number(units) -> units
        _ -> 0
      end)
      |> Enum.sum()
      |> then(fn
        sum when is_float(sum) -> Float.round(sum, 2)
        sum -> sum
      end)

    employees_with_overtime =
      overtime_entries
      |> Enum.map(fn
        {:overtime_entry, _, %{employee_id: emp_id}} -> emp_id
        _ -> "unknown"
      end)
      |> Enum.uniq()
      |> length()

    %{
      total_employees: total_employees,
      total_regular_hours: total_regular_hours,
      total_overtime_hours: total_overtime_hours,
      employees_with_overtime: employees_with_overtime,
      total_hours: total_regular_hours + total_overtime_hours,
      processed_with_engine: true,
      summary_generated_at: DateTime.utc_now()
    }
  end

  defp print_engine_results(result) do
    IO.puts("  Processed Entries: #{length(result.processed_entries)}")
    IO.puts("  Overtime Entries: #{length(result.overtime_entries)}")
    IO.puts("\nSummary:")
    IO.puts("  Total Employees: #{result.summary.total_employees}")
    IO.puts("  Regular Hours: #{result.summary.total_regular_hours}")
    IO.puts("  Overtime Hours: #{result.summary.total_overtime_hours}")
    IO.puts("  Total Hours: #{result.summary.total_hours}")
    IO.puts("  Employees with Overtime: #{result.summary.employees_with_overtime}")
    IO.puts("  Processed with RETE Engine: #{result.summary.processed_with_engine}")
  end
end
