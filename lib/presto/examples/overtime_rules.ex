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

  The rule_spec can now include:
  - rule_execution_order: Array specifying the order of main rules
  - overtime_rules: Array of overtime rule specifications with priorities
  - variables: Additional variables for rule processing

  ## Example rule_spec:

      %{
        "rule_execution_order" => ["time_calculation", "pay_aggregation", "overtime_processing"],
        "overtime_rules" => [
          %{
            "name" => "overtime_bp",
            "priority" => 1,
            "threshold" => 15,
            "filter_pay_code" => "basic_pay",
            "pay_code" => "overtime_basic_pay"
          }
        ],
        "variables" => %{}
      }
  """
  @spec create_rules(rule_spec()) :: [map()]
  def create_rules(rule_spec) do
    variables = extract_variables(rule_spec)
    execution_order = extract_rule_execution_order(rule_spec)

    # Create mapping of rule names to rule creation functions
    rule_creators = %{
      "time_calculation" => fn -> time_calculation_rule() end,
      "pay_aggregation" => fn -> pay_aggregation_rule() end,
      "overtime_processing" => fn -> overtime_processing_rule(variables, rule_spec) end
    }

    # Create rules in the specified order
    execution_order
    |> Enum.map(fn rule_name ->
      case Map.get(rule_creators, rule_name) do
        nil -> raise ArgumentError, "Unknown rule name: #{rule_name}"
        creator -> creator.()
      end
    end)
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
  @spec overtime_processing_rule(rule_variables(), rule_spec()) :: map()
  def overtime_processing_rule(_variables \\ %{}, _rule_spec \\ %{}) do
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
  Processes time entries and overtime rules using the Presto RETE engine.

  This function demonstrates how to use the actual Presto engine with overtime rules:
  1. Start the engine
  2. Add overtime calculation rules
  3. Assert initial facts (time entries and overtime rules)
  4. Fire rules to process the data
  5. Extract results from working memory

  This showcases the incremental processing capabilities of the RETE algorithm
  as facts are asserted and rules fire in response to pattern matches.
  """
  @spec process_with_engine([time_entry()], [overtime_rule()], rule_spec()) :: overtime_result()
  def process_with_engine(time_entries, overtime_rules, rule_spec \\ %{}) do
    # Start the Presto RETE engine
    {:ok, engine} = Presto.start_engine()

    try do
      # Create and add overtime rules to the engine
      variables = extract_variables(rule_spec)

      rules = [
        time_calculation_rule(),
        pay_aggregation_rule(),
        overtime_processing_rule(variables, rule_spec)
      ]

      Enum.each(rules, &Presto.add_rule(engine, &1))

      # Assert initial facts into working memory
      # Time entries
      Enum.each(time_entries, fn {:time_entry, id, data} ->
        Presto.assert_fact(engine, {:time_entry, id, data})
      end)

      # Overtime rules as facts (if provided)
      Enum.each(overtime_rules, fn {:overtime_rule, id, data} ->
        Presto.assert_fact(engine, {:overtime_rule, id, data})
      end)

      # Fire rules to process the data through the RETE network
      _rule_results = Presto.fire_rules(engine, concurrent: true)

      # Extract results from working memory
      all_facts = Presto.get_facts(engine)
      extract_overtime_results(all_facts)
    after
      # Clean up the engine
      Presto.stop_engine(engine)
    end
  end

  @doc """
  Processes time entries through the complete overtime calculation workflow.

  This is the original implementation that processes data directly without the RETE engine.
  For demonstrations of the actual RETE engine, use process_with_engine/3.
  """
  @spec process_overtime([time_entry()], [overtime_rule()], rule_spec()) :: overtime_result()
  def process_overtime(time_entries, overtime_rules, rule_spec \\ %{}) do
    # Use overtime rules from spec if available, otherwise use provided rules
    effective_overtime_rules =
      case extract_overtime_rules_from_spec(rule_spec) do
        [] -> overtime_rules
        spec_rules -> spec_rules
      end

    # Step 1: Calculate time durations
    calculated_entries =
      time_entries
      |> Enum.map(&calculate_time_duration/1)

    # Step 2: Create pay aggregates
    pay_aggregates = create_initial_pay_aggregates(calculated_entries)

    # Step 3: Process overtime rules in sequence
    {final_aggregates, overtime_entries} =
      process_overtime_rules_sequentially(pay_aggregates, effective_overtime_rules)

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
         employee_id: "emp_001",
         start_dt: ~U[2025-01-01 12:00:00Z],
         finish_dt: ~U[2025-01-01 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "basic_pay",
         paid: false
       }},
      {:time_entry, "shift_2",
       %{
         employee_id: "emp_001",
         start_dt: ~U[2025-01-02 12:00:00Z],
         finish_dt: ~U[2025-01-02 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "basic_pay",
         paid: false
       }},
      {:time_entry, "shift_3",
       %{
         employee_id: "emp_002",
         start_dt: ~U[2025-01-03 12:00:00Z],
         finish_dt: ~U[2025-01-03 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "basic_pay",
         paid: false
       }},
      {:time_entry, "shift_4",
       %{
         employee_id: "emp_002",
         start_dt: ~U[2025-01-04 12:00:00Z],
         finish_dt: ~U[2025-01-04 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "basic_pay",
         paid: false
       }},
      {:time_entry, "shift_5",
       %{
         employee_id: "emp_003",
         start_dt: ~U[2025-01-05 12:00:00Z],
         finish_dt: ~U[2025-01-05 22:00:00Z],
         units: nil,
         minutes: nil,
         pay_code: "basic_pay",
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
  Example rule specification with custom rule ordering and overtime rules.
  """
  def generate_example_rule_spec do
    %{
      "rule_execution_order" => ["time_calculation", "pay_aggregation", "overtime_processing"],
      "overtime_rules" => [
        %{
          "name" => "overtime_basic_priority_1",
          "priority" => 1,
          "threshold" => 15,
          "filter_pay_code" => "basic_pay",
          "pay_code" => "overtime_basic_pay"
        },
        %{
          "name" => "overtime_special_priority_2",
          "priority" => 2,
          "threshold" => 15,
          "filter_pay_code" => "special_pay",
          "pay_code" => "overtime_special_pay"
        },
        %{
          "name" => "overtime_general_priority_3",
          "priority" => 3,
          "threshold" => 5,
          "filter_pay_code" => nil,
          "pay_code" => "overtime_rest"
        }
      ],
      "variables" => %{
        "max_overtime_hours" => 20,
        "calculation_precision" => 2
      }
    }
  end

  @doc """
  Demonstrates custom overtime configuration with the RETE engine.
  """
  def run_custom_overtime_example do
    IO.puts("=== Custom Overtime Rules with RETE Engine ===\n")

    {time_entries, overtime_rules} = generate_example_data()
    rule_spec = generate_example_rule_spec()

    IO.puts("Custom Rule Specification:")
    IO.puts("  Overtime Threshold: #{rule_spec["variables"]["overtime_threshold"]} hours")
    IO.puts("  Max Overtime Hours: #{rule_spec["variables"]["max_overtime_hours"]} hours")

    IO.puts("\nProcessing with custom rules using RETE engine...")
    result = process_with_engine(time_entries, overtime_rules, rule_spec)

    IO.puts("\nCustom Overtime Results:")
    print_engine_results(result)

    result
  end

  @doc """
  Demonstrates rule ordering with custom execution sequence (direct processing).
  """
  def run_custom_order_example do
    IO.puts("=== Custom Rule Ordering Example (Direct Processing) ===\n")

    {time_entries, _} = generate_example_data()
    rule_spec = generate_example_rule_spec()

    IO.puts("Rule Specification:")
    IO.puts("  Execution Order: #{inspect(Map.get(rule_spec, "rule_execution_order"))}")

    IO.puts("\nOvertime Rules from Spec:")

    rule_spec["overtime_rules"]
    |> Enum.each(fn rule ->
      IO.puts(
        "  #{rule["name"]}: priority #{rule["priority"]}, threshold #{rule["threshold"]}h -> #{rule["pay_code"]}"
      )
    end)

    IO.puts("\nValidating rule specification...")

    case valid_rule_spec?(rule_spec) do
      true -> IO.puts("✓ Rule specification is valid")
      false -> IO.puts("✗ Rule specification is invalid")
    end

    IO.puts("\nCreating rules from specification...")
    rules = create_rules(rule_spec)
    IO.puts("✓ Created #{length(rules)} rules in custom order")

    IO.puts("\nProcessing with spec-defined overtime rules (direct processing mode)...")
    result = process_overtime(time_entries, [], rule_spec)

    IO.puts("\nResults:")
    print_summary(result.summary)

    result
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
      Enum.reduce(sorted_rules, {pay_aggregates, []}, &process_single_overtime_rule/2)

    {final_aggregates, Enum.reverse(overtime_entries)}
  end

  defp process_single_overtime_rule(rule, {current_aggregates, acc_overtime}) do
    applicable_aggregates = find_applicable_aggregates(current_aggregates, rule)

    if Enum.empty?(applicable_aggregates) do
      {current_aggregates, acc_overtime}
    else
      process_applicable_overtime_rule(
        applicable_aggregates,
        rule,
        current_aggregates,
        acc_overtime
      )
    end
  end

  defp process_applicable_overtime_rule(
         applicable_aggregates,
         rule,
         current_aggregates,
         acc_overtime
       ) do
    results = apply_overtime_rule(applicable_aggregates, rule)
    overtime_entry = extract_overtime_entry(results)
    updated_aggregates = extract_pay_aggregates_from_results(results)
    new_aggregates = update_aggregates_with_results(current_aggregates, updated_aggregates)

    {new_aggregates, [overtime_entry | acc_overtime]}
  end

  defp extract_overtime_entry(results) do
    Enum.find(results, fn
      {:overtime_entry, _, _} -> true
      _ -> false
    end)
  end

  defp extract_pay_aggregates_from_results(results) do
    results
    |> Enum.filter(fn
      {:pay_aggregate, _, _} -> true
      _ -> false
    end)
  end

  defp update_aggregates_with_results(current_aggregates, updated_aggregates) do
    current_aggregates
    |> Enum.map(&update_single_aggregate(&1, updated_aggregates))
  end

  defp update_single_aggregate({type, key, _data} = aggregate, updated_aggregates) do
    case find_updated_aggregate(updated_aggregates, key) do
      nil -> aggregate
      {:pay_aggregate, ^key, new_data} -> {type, key, new_data}
    end
  end

  defp find_updated_aggregate(updated_aggregates, key) do
    Enum.find(updated_aggregates, fn
      {:pay_aggregate, ^key, _} -> true
      _ -> false
    end)
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

  defp extract_rule_execution_order(rule_spec) do
    case rule_spec do
      %{"rule_execution_order" => order} when is_list(order) ->
        order

      _ ->
        # Default execution order if not specified
        ["time_calculation", "pay_aggregation", "overtime_processing"]
    end
  end

  defp extract_overtime_rules_from_spec(rule_spec) do
    case rule_spec do
      %{"overtime_rules" => rules} when is_list(rules) ->
        rules
        |> Enum.map(fn rule ->
          {:overtime_rule, Map.get(rule, "name", "unknown"),
           %{
             threshold: Map.get(rule, "threshold", 0),
             filter_pay_code: Map.get(rule, "filter_pay_code"),
             pay_code: Map.get(rule, "pay_code", "overtime"),
             priority: Map.get(rule, "priority", 1)
           }}
        end)

      _ ->
        []
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

  Supports:
  - rule_execution_order: Array of valid rule names
  - overtime_rules: Array of overtime rule objects
  - variables: Optional variables map
  """
  @spec valid_rule_spec?(rule_spec()) :: boolean()
  def valid_rule_spec?(rule_spec) do
    case rule_spec do
      # Format with rule ordering
      %{"rule_execution_order" => order} = spec when is_list(order) ->
        valid_rule_execution_order?(order) and
          valid_overtime_rules_in_spec?(Map.get(spec, "overtime_rules", []))

      # Format with just overtime rules
      %{"overtime_rules" => rules} when is_list(rules) ->
        valid_overtime_rules_in_spec?(rules)

      # Empty spec is valid (uses defaults)
      spec when map_size(spec) == 0 ->
        true

      _ ->
        false
    end
  end

  defp valid_rule_execution_order?(order) do
    valid_rules = ["time_calculation", "pay_aggregation", "overtime_processing"]

    Enum.all?(order, fn rule_name ->
      is_binary(rule_name) and rule_name in valid_rules
    end)
  end

  defp valid_overtime_rules_in_spec?(rules) when is_list(rules) do
    Enum.all?(rules, &valid_overtime_rule_in_spec?/1)
  end

  defp valid_overtime_rules_in_spec?(_), do: false

  defp valid_overtime_rule_in_spec?(%{"name" => name} = rule) when is_binary(name) do
    valid_threshold?(rule) and valid_pay_code?(rule) and valid_priority?(rule)
  end

  defp valid_overtime_rule_in_spec?(_), do: false

  defp valid_threshold?(rule) do
    case Map.get(rule, "threshold") do
      threshold when is_number(threshold) and threshold >= 0 -> true
      _ -> false
    end
  end

  defp valid_pay_code?(rule) do
    case Map.get(rule, "pay_code") do
      pay_code when is_binary(pay_code) -> true
      _ -> false
    end
  end

  defp valid_priority?(rule) do
    case Map.get(rule, "priority") do
      priority when is_integer(priority) and priority > 0 -> true
      # Optional field
      nil -> true
      _ -> false
    end
  end

  # Helper functions for RETE engine processing

  defp extract_overtime_results(all_facts) do
    processed_entries = extract_facts_by_type(all_facts, :time_entry)
    overtime_entries = extract_facts_by_type(all_facts, :overtime_entry)
    pay_aggregates = extract_facts_by_type(all_facts, :pay_aggregate)

    summary = generate_engine_summary(processed_entries, overtime_entries, pay_aggregates)

    %{
      processed_entries: processed_entries,
      overtime_entries: overtime_entries,
      pay_aggregates: pay_aggregates,
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

  defp generate_engine_summary(processed_entries, overtime_entries, pay_aggregates) do
    total_regular_hours =
      processed_entries
      |> Enum.map(fn
        {:time_entry, _, %{units: units}} when is_number(units) -> units
        _ -> 0
      end)
      |> Enum.sum()
      |> Float.round(2)

    total_overtime_hours =
      overtime_entries
      |> Enum.map(fn
        {:overtime_entry, _, %{units: units}} when is_number(units) -> units
        _ -> 0
      end)
      |> Enum.sum()
      |> Float.round(2)

    unique_employees =
      processed_entries
      |> Enum.map(fn
        {:time_entry, _, %{employee_id: emp_id}} -> emp_id
        _ -> "employee_1"
      end)
      |> Enum.uniq()
      |> length()

    %{
      total_regular_hours: total_regular_hours,
      total_overtime_hours: total_overtime_hours,
      total_hours: total_regular_hours + total_overtime_hours,
      unique_employees: unique_employees,
      aggregates_created: length(pay_aggregates),
      overtime_entries_generated: length(overtime_entries),
      processed_with_engine: true,
      summary_generated_at: DateTime.utc_now()
    }
  end

  defp print_engine_results(result) do
    IO.puts("  Processed Entries: #{length(result.processed_entries)}")
    IO.puts("  Pay Aggregates: #{length(result.pay_aggregates)}")
    IO.puts("  Overtime Entries: #{length(result.overtime_entries)}")
    IO.puts("\\nSummary:")
    IO.puts("  Regular Hours: #{result.summary.total_regular_hours}")
    IO.puts("  Overtime Hours: #{result.summary.total_overtime_hours}")
    IO.puts("  Total Hours: #{result.summary.total_hours}")
    IO.puts("  Employees: #{result.summary.unique_employees}")
    IO.puts("  Processed with RETE Engine: #{result.summary.processed_with_engine}")
  end
end
