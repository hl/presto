defmodule Examples.PayrollExample do
  @moduledoc """
  Simplified payroll processing example using Presto v0.2 API.
  
  This example demonstrates payroll business logic using the RETE rules engine
  without over-engineering. Following BSSN principles, it focuses on solving
  the current problem simply and directly.
  
  ## Usage
  
      # Process a single employee
      result = Examples.PayrollExample.process_employee(employee_data)
      
      # Process multiple employees
      results = Examples.PayrollExample.process_batch(employees)
      
      # Run demo with sample data
      Examples.PayrollExample.run_demo()
  """

  @doc """
  Process payroll for a single employee using business rules.
  """
  def process_employee(employee_data) do
    {:ok, engine} = Presto.RuleEngine.start_link()
    
    # Define payroll rules
    rules = [
      overtime_rule(),
      holiday_pay_rule(),
      bonus_rule(),
      deduction_rule()
    ]
    
    # Add rules
    :ok = Presto.RuleEngine.add_rules(engine, rules)
    
    # Convert employee data to facts
    facts = employee_to_facts(employee_data)
    
    # Process through rules engine
    :ok = Presto.RuleEngine.assert_facts(engine, facts)
    results = Presto.RuleEngine.fire_rules(engine)
    
    # Clean up and return processed results
    GenServer.stop(engine)
    
    %{
      employee_id: employee_data.id,
      base_pay: calculate_base_pay(employee_data),
      results: results,
      total_pay: calculate_total_pay(results, employee_data),
      processed_at: DateTime.utc_now()
    }
  end

  @doc """
  Process payroll for multiple employees efficiently.
  """
  def process_batch(employees) when is_list(employees) do
    {:ok, engine} = Presto.RuleEngine.start_link()
    
    # Define payroll rules once
    rules = [
      overtime_rule(),
      holiday_pay_rule(), 
      bonus_rule(),
      deduction_rule()
    ]
    
    :ok = Presto.RuleEngine.add_rules(engine, rules)
    
    # Process all employees
    results = Enum.map(employees, fn employee ->
      # Convert to facts
      facts = employee_to_facts(employee)
      
      # Process this employee
      :ok = Presto.RuleEngine.assert_facts(engine, facts)
      rule_results = Presto.RuleEngine.fire_rules(engine)
      
      # Calculate final amounts
      %{
        employee_id: employee.id,
        base_pay: calculate_base_pay(employee),
        results: rule_results,
        total_pay: calculate_total_pay(rule_results, employee)
      }
    end)
    
    GenServer.stop(engine)
    results
  end

  @doc """
  Run a demonstration of the payroll system.
  """
  def run_demo do
    IO.puts("=== Payroll Processing Demo ===\n")
    
    # Sample employee data
    employees = [
      %{id: "emp_001", name: "Alice", hourly_rate: 25.0, hours_worked: 45, department: "Engineering", performance: 4.2},
      %{id: "emp_002", name: "Bob", hourly_rate: 22.0, hours_worked: 40, department: "Sales", performance: 3.8},
      %{id: "emp_003", name: "Carol", hourly_rate: 30.0, hours_worked: 50, department: "Engineering", performance: 4.8},
      %{id: "emp_004", name: "David", hourly_rate: 20.0, hours_worked: 35, department: "Support", performance: 3.5}
    ]
    
    IO.puts("Processing #{length(employees)} employees...")
    
    # Measure performance
    {time_microseconds, results} = :timer.tc(fn ->
      process_batch(employees)
    end)
    
    # Display results
    IO.puts("\n=== Payroll Results ===")
    total_payroll = 0
    
    total_payroll = Enum.reduce(results, 0, fn result, acc ->
      IO.puts("#{result.employee_id}:")
      IO.puts("  Base Pay: $#{Float.round(result.base_pay, 2)}")
      IO.puts("  Total Pay: $#{Float.round(result.total_pay, 2)}")
      
      if length(result.results) > 0 do
        IO.puts("  Adjustments:")
        Enum.each(result.results, fn adjustment ->
          IO.puts("    → #{inspect(adjustment)}")
        end)
      end
      
      IO.puts("")
      acc + result.total_pay
    end)
    
    # Summary
    IO.puts("=== Summary ===")
    IO.puts("Total Payroll: $#{Float.round(total_payroll, 2)}")
    IO.puts("Processing Time: #{Float.round(time_microseconds / 1000, 2)}ms")
    IO.puts("Throughput: #{Float.round(length(employees) / (time_microseconds / 1_000_000), 1)} employees/second")
    
    IO.puts("\n=== Demo Complete ===")
  end

  # Private helper functions

  defp overtime_rule do
    Presto.Rule.new(
      :overtime_pay,
      [
        Presto.Rule.pattern(:employee, [:id, :hourly_rate, :hours_worked]),
        Presto.Rule.test(:hours_worked, :>, 40)
      ],
      fn facts ->
        overtime_hours = facts[:hours_worked] - 40
        overtime_rate = facts[:hourly_rate] * 1.5
        overtime_pay = overtime_hours * overtime_rate
        [{:overtime, facts[:id], overtime_hours, overtime_pay}]
      end
    )
  end

  defp holiday_pay_rule do
    Presto.Rule.new(
      :holiday_pay,
      [
        Presto.Rule.pattern(:employee, [:id, :hourly_rate, :hours_worked]),
        Presto.Rule.pattern(:holiday_hours, [:id, :holiday_hours])
      ],
      fn facts ->
        holiday_pay = facts[:holiday_hours] * facts[:hourly_rate] * 2.0
        [{:holiday_pay, facts[:id], holiday_pay}]
      end
    )
  end

  defp bonus_rule do
    Presto.Rule.new(
      :performance_bonus,
      [
        Presto.Rule.pattern(:employee, [:id, :department, :performance]),
        Presto.Rule.test(:performance, :>, 4.0)
      ],
      fn facts ->
        base_bonus = case facts[:department] do
          "Engineering" -> 1000
          "Sales" -> 800
          _ -> 500
        end
        
        performance_multiplier = facts[:performance] / 4.0
        bonus = base_bonus * performance_multiplier
        [{:bonus, facts[:id], bonus}]
      end
    )
  end

  defp deduction_rule do
    Presto.Rule.new(
      :standard_deductions,
      [
        Presto.Rule.pattern(:employee, [:id, :hourly_rate, :hours_worked])
      ],
      fn facts ->
        gross_pay = facts[:hourly_rate] * facts[:hours_worked]
        
        # Standard deductions
        federal_tax = gross_pay * 0.22
        state_tax = gross_pay * 0.08
        social_security = gross_pay * 0.062
        medicare = gross_pay * 0.0145
        
        [{:deductions, facts[:id], %{
          federal_tax: federal_tax,
          state_tax: state_tax,
          social_security: social_security,
          medicare: medicare,
          total: federal_tax + state_tax + social_security + medicare
        }}]
      end
    )
  end

  defp employee_to_facts(employee) do
    base_facts = [
      {:employee, employee.id, employee.hourly_rate, employee.hours_worked},
      {:employee, employee.id, employee.department, employee.performance}
    ]
    
    # Add optional holiday hours if present
    holiday_facts = case Map.get(employee, :holiday_hours) do
      nil -> []
      hours -> [{:holiday_hours, employee.id, hours}]
    end
    
    base_facts ++ holiday_facts
  end

  defp calculate_base_pay(employee) do
    employee.hourly_rate * employee.hours_worked
  end

  defp calculate_total_pay(results, employee) do
    base_pay = calculate_base_pay(employee)
    
    # Add bonuses and overtime
    additions = results
    |> Enum.filter(fn result ->
      match?({:overtime, _, _, _}, result) or
      match?({:bonus, _, _}, result) or
      match?({:holiday_pay, _, _}, result)
    end)
    |> Enum.reduce(0, fn
      {:overtime, _id, _hours, pay}, acc -> acc + pay
      {:bonus, _id, amount}, acc -> acc + amount
      {:holiday_pay, _id, amount}, acc -> acc + amount
      _, acc -> acc
    end)
    
    # Subtract deductions
    deductions = results
    |> Enum.find_value(0, fn
      {:deductions, _id, %{total: total}} -> total
      _ -> false
    end)
    
    base_pay + additions - deductions
  end
end