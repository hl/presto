#!/usr/bin/env elixir

# Advanced TRONC (Tips, Gratuities & Service Charges) Example
# This example demonstrates comprehensive tip pooling and distribution

IO.puts("=== Advanced TRONC Distribution Example ===\n")

# Start the application
Application.ensure_all_started(:presto)

# 1. Start the rules engine
{:ok, engine} = Presto.RuleEngine.start_link()

# 2. Define TRONC distribution rules
IO.puts("Defining TRONC distribution rules...")

# Define role weights for fair distribution
role_weights = %{
  "manager" => 1.5,
  "head_chef" => 1.4,
  "chef" => 1.2,
  "waiter" => 1.0,
  "waitress" => 1.0,
  "bartender" => 1.1,
  "host" => 0.9,
  "kitchen_assistant" => 0.8,
  "cleaner" => 0.7
}

# Rule 1: Collect tips, gratuities and service charges into daily pools
pool_collection_rule = %{
  id: :collect_tronc_pools,
  conditions: [
    {:revenue_entry, :id, :data}
  ],
  action: fn facts ->
    revenue_data = facts[:data]
    date = revenue_data.date

    # Calculate total TRONC amount
    service_charge = Map.get(revenue_data, :service_charge, 0.0)
    tips_cash = Map.get(revenue_data, :tips_cash, 0.0)
    tips_card = Map.get(revenue_data, :tips_card, 0.0)
    gross_amount = service_charge + tips_cash + tips_card

    pool_data = %{
      date: date,
      gross_amount: Float.round(gross_amount, 2),
      service_charges: Float.round(service_charge, 2),
      tips_cash: Float.round(tips_cash, 2),
      tips_card: Float.round(tips_card, 2),
      admin_deducted: false,
      net_amount: nil,
      revenue_entries: [facts[:id]],
      created_at: DateTime.utc_now()
    }

    IO.puts("  ✓ Collected £#{Float.round(gross_amount, 2)} into TRONC pool for #{date}")

    [{:tronc_pool, date, pool_data}]
  end,
  priority: 100
}

# Rule 2: Deduct administration costs (5%)
admin_cost_rate = 5.0

admin_deduction_rule = %{
  id: :deduct_admin_costs,
  conditions: [
    {:tronc_pool, :date, :pool_data}
  ],
  action: fn facts ->
    pool_data = facts[:pool_data]
    date = facts[:date]

    # Only process if admin costs haven't been deducted yet
    if not pool_data.admin_deducted do
      admin_amount = pool_data.gross_amount * (admin_cost_rate / 100.0)
      net_amount = pool_data.gross_amount - admin_amount

      updated_pool_data = %{
        pool_data
        | admin_deducted: true,
          net_amount: Float.round(net_amount, 2)
      }

      admin_deduction_data = %{
        date: date,
        gross_amount: pool_data.gross_amount,
        admin_rate: admin_cost_rate,
        admin_amount: Float.round(admin_amount, 2),
        net_amount: Float.round(net_amount, 2),
        created_at: DateTime.utc_now()
      }

      IO.puts("  ✓ Deducted £#{Float.round(admin_amount, 2)} admin costs (#{admin_cost_rate}%)")

      [
        {:tronc_pool, date, updated_pool_data},
        {:admin_deduction, date, admin_deduction_data}
      ]
    else
      []
    end
  end,
  priority: 90
}

# Rule 3: Calculate role-based allocations
role_allocation_rule = %{
  id: :allocate_to_roles,
  conditions: [
    {:tronc_pool, :date, :pool_data},
    {:staff_shift, :shift_id, :shift_data}
  ],
  action: fn facts ->
    pool_data = facts[:pool_data]
    shift_data = facts[:shift_data]
    date = facts[:date]

    # Only process if admin has been deducted and dates match
    if pool_data.admin_deducted and shift_data.date == date and
         Map.get(shift_data, :is_tronc_eligible, false) do
      role = shift_data.role
      hours_worked = shift_data.hours_worked
      role_weight = Map.get(role_weights, role, 1.0)

      # Calculate weighted hours for this shift
      weighted_hours = hours_worked * role_weight

      # Estimate total weighted hours across all roles (simplified)
      # This would be calculated from all shifts
      total_estimated_weighted_hours = 50.0

      # Calculate allocation percentage
      allocation_percentage = weighted_hours / total_estimated_weighted_hours
      allocation_amount = pool_data.net_amount * allocation_percentage

      allocation_data = %{
        role: role,
        date: date,
        employee_id: shift_data.employee_id,
        hours_worked: hours_worked,
        role_weight: role_weight,
        weighted_hours: Float.round(weighted_hours * 1.0, 2),
        allocation_percentage: Float.round(allocation_percentage * 100.0, 2),
        allocation_amount: Float.round(allocation_amount * 1.0, 2),
        created_at: DateTime.utc_now()
      }

      IO.puts(
        "  ✓ Allocated £#{Float.round(allocation_amount, 2)} to #{shift_data.employee_id} (#{role})"
      )

      [{:role_allocation, {shift_data.employee_id, date}, allocation_data}]
    else
      []
    end
  end,
  priority: 80
}

# Rule 4: Apply seniority bonuses and generate final payments
staff_payment_rule = %{
  id: :generate_staff_payments,
  conditions: [
    {:role_allocation, :allocation_key, :allocation_data}
  ],
  action: fn facts ->
    allocation_data = facts[:allocation_data]
    allocation_key = facts[:allocation_key]
    {employee_id, date} = allocation_key

    # Apply seniority bonus (2% per year)
    seniority_years = Map.get(allocation_data, :seniority_years, 0)
    seniority_bonus_rate = 0.02
    seniority_multiplier = 1.0 + seniority_years * seniority_bonus_rate

    final_payment = allocation_data.allocation_amount * seniority_multiplier

    payment_data = %{
      employee_id: employee_id,
      role: allocation_data.role,
      date: date,
      base_allocation: allocation_data.allocation_amount,
      seniority_years: seniority_years,
      seniority_bonus: Float.round((final_payment - allocation_data.allocation_amount) * 1.0, 2),
      final_payment: Float.round(final_payment * 1.0, 2),
      # TRONC payments can be National Insurance exempt
      is_ni_exempt: true,
      created_at: DateTime.utc_now()
    }

    IO.puts("  ✓ Final payment £#{Float.round(final_payment, 2)} for #{employee_id}")

    [{:staff_payment, allocation_key, payment_data}]
  end,
  priority: 70
}

# 3. Add rules to the engine
IO.puts("\nAdding rules to engine...")
:ok = Presto.RuleEngine.add_rule(engine, pool_collection_rule)
:ok = Presto.RuleEngine.add_rule(engine, admin_deduction_rule)
:ok = Presto.RuleEngine.add_rule(engine, role_allocation_rule)
:ok = Presto.RuleEngine.add_rule(engine, staff_payment_rule)

# 4. Create sample data
IO.puts("\nCreating sample restaurant data...")

# Sample staff shifts for a busy Friday night
staff_shifts = [
  {:staff_shift, "shift_001",
   %{
     employee_id: "emp_001",
     employee_name: "Alice Smith",
     role: "waiter",
     date: ~D[2025-01-15],
     hours_worked: 8.0,
     hourly_rate: 12.50,
     is_tronc_eligible: true,
     seniority_years: 2
   }},
  {:staff_shift, "shift_002",
   %{
     employee_id: "emp_002",
     employee_name: "Bob Johnson",
     role: "chef",
     date: ~D[2025-01-15],
     hours_worked: 10.0,
     hourly_rate: 15.00,
     is_tronc_eligible: true,
     seniority_years: 5
   }},
  {:staff_shift, "shift_003",
   %{
     employee_id: "emp_003",
     employee_name: "Carol Wilson",
     role: "bartender",
     date: ~D[2025-01-15],
     hours_worked: 7.5,
     hourly_rate: 13.25,
     is_tronc_eligible: true,
     seniority_years: 1
   }},
  {:staff_shift, "shift_004",
   %{
     employee_id: "emp_004",
     employee_name: "David Brown",
     role: "manager",
     date: ~D[2025-01-15],
     hours_worked: 9.0,
     hourly_rate: 18.00,
     is_tronc_eligible: true,
     seniority_years: 8
   }},
  {:staff_shift, "shift_005",
   %{
     employee_id: "emp_005",
     employee_name: "Eve Davis",
     role: "kitchen_assistant",
     date: ~D[2025-01-15],
     hours_worked: 6.0,
     hourly_rate: 10.50,
     is_tronc_eligible: true,
     seniority_years: 0
   }}
]

# Sample revenue entries from tables and bar
revenue_entries = [
  {:revenue_entry, "table_001",
   %{
     bill_amount: 125.50,
     # 10% service charge
     service_charge: 12.55,
     tips_cash: 15.00,
     tips_card: 8.50,
     date: ~D[2025-01-15],
     table_number: 1,
     covers: 4
   }},
  {:revenue_entry, "table_002",
   %{
     bill_amount: 89.75,
     # 10% service charge
     service_charge: 8.98,
     tips_cash: 5.00,
     tips_card: 12.25,
     date: ~D[2025-01-15],
     table_number: 2,
     covers: 2
   }},
  {:revenue_entry, "bar_001",
   %{
     bill_amount: 67.25,
     # No service charge on bar orders
     service_charge: 0.0,
     tips_cash: 8.00,
     tips_card: 4.75,
     date: ~D[2025-01-15],
     table_number: nil,
     covers: 1
   }},
  {:revenue_entry, "private_001",
   %{
     bill_amount: 450.00,
     # 15% service charge for private dining
     service_charge: 67.50,
     tips_cash: 50.00,
     tips_card: 25.00,
     date: ~D[2025-01-15],
     table_number: "private",
     covers: 12
   }}
]

IO.puts(
  "Sample data: #{length(staff_shifts)} staff shifts, #{length(revenue_entries)} revenue entries"
)

# 5. Assert facts to working memory
IO.puts("\nAsserting staff shifts...")

Enum.each(staff_shifts, fn staff_shift ->
  :ok = Presto.RuleEngine.assert_fact(engine, staff_shift)
end)

IO.puts("Asserting revenue entries...")

Enum.each(revenue_entries, fn revenue_entry ->
  :ok = Presto.RuleEngine.assert_fact(engine, revenue_entry)
end)

# 6. Fire rules and process TRONC distribution
IO.puts("\nFiring TRONC distribution rules:")
results = Presto.RuleEngine.fire_rules(engine)

# 7. Analyze results
IO.puts("\nProcessing Results:")

# Extract different types of results
tronc_pools =
  Enum.filter(results, fn
    {:tronc_pool, _, _} -> true
    _ -> false
  end)

admin_deductions =
  Enum.filter(results, fn
    {:admin_deduction, _, _} -> true
    _ -> false
  end)

role_allocations =
  Enum.filter(results, fn
    {:role_allocation, _, _} -> true
    _ -> false
  end)

staff_payments =
  Enum.filter(results, fn
    {:staff_payment, _, _} -> true
    _ -> false
  end)

IO.puts("  → Created #{length(tronc_pools)} TRONC pools")
IO.puts("  → Processed #{length(admin_deductions)} admin deductions")
IO.puts("  → Generated #{length(role_allocations)} role allocations")
IO.puts("  → Created #{length(staff_payments)} staff payments")

# 8. Calculate summary statistics
total_gross =
  tronc_pools
  |> Enum.map(fn {:tronc_pool, _, %{gross_amount: amount}} -> amount end)
  |> Enum.sum()
  |> case do
    sum when is_number(sum) -> Float.round(sum / 1.0, 2)
    _ -> 0.0
  end

total_admin =
  admin_deductions
  |> Enum.map(fn {:admin_deduction, _, %{admin_amount: amount}} -> amount end)
  |> Enum.sum()
  |> case do
    sum when is_number(sum) -> Float.round(sum / 1.0, 2)
    _ -> 0.0
  end

total_payments =
  staff_payments
  |> Enum.map(fn {:staff_payment, _, %{final_payment: amount}} -> amount end)
  |> Enum.sum()
  |> case do
    sum when is_number(sum) -> Float.round(sum / 1.0, 2)
    _ -> 0.0
  end

unique_employees =
  staff_payments
  |> Enum.map(fn {:staff_payment, {emp_id, _}, _} -> emp_id end)
  |> Enum.uniq()
  |> length()

# 9. Display summary
IO.puts("\n=== TRONC Distribution Summary ===")
IO.puts("Total Revenue Collected: £#{total_gross}")
IO.puts("Administration Costs (#{admin_cost_rate}%): £#{total_admin}")
IO.puts("Net Amount Distributed: £#{total_gross - total_admin}")
IO.puts("Total Staff Payments: £#{total_payments}")
IO.puts("Staff Members Paid: #{unique_employees}")

# 10. Show detailed staff payments
if length(staff_payments) > 0 do
  IO.puts("\n=== Individual Staff Payments ===")

  Enum.each(staff_payments, fn {:staff_payment, {emp_id, _}, data} ->
    base = data.base_allocation
    bonus = data.seniority_bonus
    final = data.final_payment
    years = data.seniority_years
    IO.puts("  #{emp_id} (#{data.role}): £#{base} + £#{bonus} seniority (#{years}y) = £#{final}")
  end)
end

# 11. Show TRONC pool breakdown
if length(tronc_pools) > 0 do
  IO.puts("\n=== TRONC Pool Breakdown ===")

  Enum.each(tronc_pools, fn {:tronc_pool, date, data} ->
    IO.puts(
      "  #{date}: £#{data.service_charges} service + £#{data.tips_cash} cash tips + £#{data.tips_card} card tips = £#{data.gross_amount}"
    )
  end)
end

# 12. Demonstrate custom allocation weights
IO.puts("\n" <> String.duplicate("=", 50))
IO.puts("CUSTOM ALLOCATION WEIGHTS DEMO")
IO.puts(String.duplicate("=", 50))

# Start a new engine with enhanced role weights
{:ok, engine2} = Presto.RuleEngine.start_link()

# Enhanced role weights that favor experience
enhanced_role_weights = %{
  # Increased from 1.5
  "manager" => 1.8,
  # Increased from 1.4
  "head_chef" => 1.6,
  # Increased from 1.2
  "chef" => 1.4,
  # Baseline
  "waiter" => 1.0,
  # Baseline
  "waitress" => 1.0,
  # Increased from 1.1
  "bartender" => 1.2,
  # Same
  "host" => 0.9,
  # Same
  "kitchen_assistant" => 0.8,
  # Same
  "cleaner" => 0.7
}

# Create enhanced allocation rule
enhanced_role_allocation_rule = %{
  id: :enhanced_allocate_to_roles,
  conditions: [
    {:tronc_pool, :date, :pool_data},
    {:staff_shift, :shift_id, :shift_data}
  ],
  action: fn facts ->
    pool_data = facts[:pool_data]
    shift_data = facts[:shift_data]
    date = facts[:date]

    if pool_data.admin_deducted and shift_data.date == date and
         Map.get(shift_data, :is_tronc_eligible, false) do
      role = shift_data.role
      hours_worked = shift_data.hours_worked
      role_weight = Map.get(enhanced_role_weights, role, 1.0)

      weighted_hours = hours_worked * role_weight
      # Higher due to enhanced weights
      total_estimated_weighted_hours = 55.0

      allocation_percentage = weighted_hours / total_estimated_weighted_hours
      allocation_amount = pool_data.net_amount * allocation_percentage

      allocation_data = %{
        role: role,
        date: date,
        employee_id: shift_data.employee_id,
        hours_worked: hours_worked,
        role_weight: role_weight,
        weighted_hours: Float.round(weighted_hours * 1.0, 2),
        allocation_percentage: Float.round(allocation_percentage * 100.0, 2),
        allocation_amount: Float.round(allocation_amount * 1.0, 2),
        created_at: DateTime.utc_now()
      }

      IO.puts(
        "  ✓ Enhanced allocation £#{Float.round(allocation_amount, 2)} to #{shift_data.employee_id} (#{role}, weight: #{role_weight})"
      )

      [{:role_allocation, {shift_data.employee_id, date}, allocation_data}]
    else
      []
    end
  end,
  priority: 80
}

IO.puts("\nUsing enhanced role weights...")
# Add basic rules plus enhanced allocation rule
:ok = Presto.RuleEngine.add_rule(engine2, pool_collection_rule)
:ok = Presto.RuleEngine.add_rule(engine2, admin_deduction_rule)
:ok = Presto.RuleEngine.add_rule(engine2, enhanced_role_allocation_rule)
:ok = Presto.RuleEngine.add_rule(engine2, staff_payment_rule)

# Assert same data
Enum.each(staff_shifts, fn staff_shift ->
  :ok = Presto.RuleEngine.assert_fact(engine2, staff_shift)
end)

Enum.each(revenue_entries, fn revenue_entry ->
  :ok = Presto.RuleEngine.assert_fact(engine2, revenue_entry)
end)

IO.puts("\nFiring rules with enhanced allocation weights:")
enhanced_results = Presto.RuleEngine.fire_rules(engine2)

enhanced_payments =
  Enum.filter(enhanced_results, fn
    {:staff_payment, _, _} -> true
    _ -> false
  end)

enhanced_total =
  enhanced_payments
  |> Enum.map(fn {:staff_payment, _, %{final_payment: amount}} -> amount end)
  |> Enum.sum()
  |> case do
    sum when is_number(sum) -> Float.round(sum / 1.0, 2)
    _ -> 0.0
  end

IO.puts("\nEnhanced Allocation Results:")
IO.puts("  Total Staff Payments: £#{enhanced_total}")

if length(enhanced_payments) > 0 do
  IO.puts("\nEnhanced Payment Details:")

  Enum.each(enhanced_payments, fn {:staff_payment, {emp_id, _}, data} ->
    IO.puts("    #{emp_id} (#{data.role}): £#{data.final_payment}")
  end)
end

# 13. Check engine statistics
engine_stats = Presto.RuleEngine.get_engine_statistics(engine)
IO.puts("\n=== Engine Statistics ===")
IO.puts("Rules: #{engine_stats.total_rules}")
IO.puts("Facts: #{engine_stats.total_facts}")
IO.puts("Rule Firings: #{engine_stats.total_rule_firings}")

# 14. Clean up
GenServer.stop(engine)
GenServer.stop(engine2)

IO.puts("\n=== TRONC Distribution Example Complete ===")
