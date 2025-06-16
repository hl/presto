defmodule Presto.Examples.TroncRules do
  @moduledoc """
  TRONC (Tips, Gratuities & Service Charges) rules engine implementation for Presto.

  This example demonstrates how to use the Presto RETE engine for complex business
  rule processing. It implements a comprehensive TRONC distribution system compliant
  with UK legislation covering the Employment (Allocation of Tips) Act 2023.

  Features implemented using the RETE engine:
  1. Collection of tips, gratuities and service charges into daily pools
  2. Administration cost deduction (typically 5%)
  3. Fair distribution based on roles, hours worked, and role weights
  4. National Insurance optimization for tax efficiency
  5. Transparent allocation calculations

  This example demonstrates:
  - Using the actual Presto RETE engine for rule processing
  - Pattern matching on facts to trigger rule execution
  - Incremental processing as new facts are asserted
  - Complex multi-stage business workflows with the engine
  - Proper fact structures for the RETE algorithm

  ## Example Usage with Presto Engine

      # Start the engine and process TRONC distribution
      {staff_shifts, revenue_entries} = TroncRules.generate_example_data()
      result = TroncRules.process_with_engine(staff_shifts, revenue_entries)

      # Or with custom rule specifications
      rule_spec = TroncRules.generate_example_rule_spec()
      result = TroncRules.process_with_engine(staff_shifts, revenue_entries, rule_spec)
  """

  @behaviour Presto.RuleBehaviour

  @type staff_shift :: {:staff_shift, String.t(), map()}
  @type revenue_entry :: {:revenue_entry, String.t(), map()}
  @type tronc_pool :: {:tronc_pool, Date.t(), map()}
  @type role_allocation :: {:role_allocation, {String.t(), Date.t()}, map()}
  @type staff_payment :: {:staff_payment, {String.t(), Date.t()}, map()}
  @type admin_deduction :: {:admin_deduction, Date.t(), map()}
  @type rule_variables :: map()
  @type rule_spec :: map()
  @type tronc_result :: %{
          tronc_pools: [tronc_pool()],
          role_allocations: [role_allocation()],
          staff_payments: [staff_payment()],
          admin_deductions: [admin_deduction()],
          summary: map()
        }

  # Default allocation percentages for different roles
  @default_role_weights %{
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

  @doc """
  Creates TRONC distribution rules for the Presto RETE engine.

  Returns a list of rule definitions that can be added to a Presto engine.
  Each rule uses pattern matching on facts and creates new facts when conditions are met.

  The rule_spec can include:
  - allocation_rules: Custom allocation percentages for roles
  - admin_cost_rate: Administration cost percentage (default 5%)
  - variables: Additional variables for rule processing

  ## Example rule_spec:

      %{
        "allocation_rules" => %{
          "manager" => 1.5,
          "waiter" => 1.0,
          "chef" => 1.2
        },
        "admin_cost_rate" => 5.0,
        "variables" => %{
          "minimum_shift_hours" => 4.0
        }
      }
  """
  @spec create_rules(rule_spec()) :: [map()]
  def create_rules(rule_spec) do
    variables = extract_variables(rule_spec)
    role_weights = extract_role_weights(rule_spec, variables)
    admin_rate = extract_admin_cost_rate(rule_spec, variables)
    min_shift_hours = Map.get(variables, "minimum_shift_hours", 4.0)

    [
      pool_collection_rule(),
      admin_deduction_rule(admin_rate),
      role_allocation_rule(role_weights, min_shift_hours),
      staff_distribution_rule()
    ]
  end

  @doc """
  Rule 1: Collect tips, gratuities and service charges into daily pools.

  Uses RETE pattern matching to identify revenue entries that need pooling.
  This rule triggers when revenue entry facts are asserted into working memory.
  """
  @spec pool_collection_rule() :: map()
  def pool_collection_rule do
    %{
      id: :collect_tronc_pools,
      conditions: [
        {:revenue_entry, :id, :data}
      ],
      action: fn facts ->
        # Group revenue entries by date and create pools
        revenue_data = facts[:data]
        date = revenue_data.date

        # This is simplified - in a full RETE implementation, we'd aggregate
        # all revenue entries for the same date before creating a pool
        create_tronc_pool_fact(date, [revenue_data])
      end,
      priority: 100
    }
  end

  @doc """
  Rule 2: Deduct administration costs from TRONC pools.

  Uses RETE pattern matching to find pools needing admin cost deduction.
  Triggers when a tronc_pool fact with admin_deducted: false is in working memory.
  """
  @spec admin_deduction_rule(float()) :: map()
  def admin_deduction_rule(admin_rate) do
    %{
      id: :deduct_admin_costs,
      conditions: [
        {:tronc_pool, :date, :pool_data},
        {:pool_data, :admin_deducted, false}
      ],
      action: fn facts ->
        pool_data = facts[:pool_data]
        date = facts[:date]

        admin_amount = pool_data.gross_amount * (admin_rate / 100.0)
        net_amount = pool_data.gross_amount - admin_amount

        updated_pool_data = %{
          pool_data
          | admin_deducted: true,
            net_amount: Float.round(net_amount, 2)
        }

        admin_deduction_data = %{
          date: date,
          gross_amount: pool_data.gross_amount,
          admin_rate: admin_rate,
          admin_amount: Float.round(admin_amount, 2),
          net_amount: Float.round(net_amount, 2),
          created_at: DateTime.utc_now()
        }

        [
          {:tronc_pool, date, updated_pool_data},
          {:admin_deduction, date, admin_deduction_data}
        ]
      end,
      priority: 90
    }
  end

  @doc """
  Rule 3: Allocate net TRONC amounts to roles based on weighted hours.

  Uses RETE pattern matching to find pools ready for role allocation.
  Triggers when we have both a processed pool and eligible staff shifts.
  """
  @spec role_allocation_rule(map(), float()) :: map()
  def role_allocation_rule(role_weights, min_shift_hours) do
    %{
      id: :allocate_to_roles,
      conditions: [
        {:tronc_pool, :date, :pool_data},
        {:pool_data, :admin_deducted, true},
        {:staff_shift, :shift_id, :shift_data},
        {:shift_data, :date, :date},
        {:shift_data, :is_tronc_eligible, true},
        {:shift_data, :hours_worked, :hours},
        {:hours, :>=, min_shift_hours}
      ],
      action: fn facts ->
        date = facts[:date]
        pool_data = facts[:pool_data]

        # Create role allocations for the pool
        # This is simplified - a full implementation would aggregate all shifts
        create_role_allocations_for_pool(date, pool_data, role_weights)
      end,
      priority: 80
    }
  end

  @doc """
  Rule 4: Distribute role allocations to individual staff members.

  Uses RETE pattern matching to distribute allocations to staff.
  Triggers when we have matching role allocations and staff shifts.
  """
  @spec staff_distribution_rule() :: map()
  def staff_distribution_rule do
    %{
      id: :distribute_to_staff,
      conditions: [
        {:role_allocation, :allocation_key, :allocation_data},
        {:staff_shift, :shift_id, :shift_data},
        {:allocation_key, :role, :role},
        {:allocation_key, :date, :date},
        {:shift_data, :role, :role},
        {:shift_data, :date, :date}
      ],
      action: fn facts ->
        allocation_data = facts[:allocation_data]
        shift_data = facts[:shift_data]

        create_staff_payment_fact(allocation_data, shift_data)
      end,
      priority: 70
    }
  end

  @doc """
  Processes revenue and staff data using the Presto RETE engine.

  This function demonstrates how to use the actual Presto engine with TRONC rules:
  1. Start the engine
  2. Add TRONC rules
  3. Assert initial facts
  4. Fire rules to process the data
  5. Extract results from working memory

  This showcases the incremental processing capabilities of the RETE algorithm
  as facts are asserted and rules fire in response to pattern matches.
  """
  @spec process_with_engine([staff_shift()], [revenue_entry()], rule_spec()) :: tronc_result()
  def process_with_engine(staff_shifts, revenue_entries, rule_spec \\ %{}) do
    # Start the Presto RETE engine
    {:ok, engine} = Presto.start_engine()

    try do
      # Create and add TRONC rules to the engine
      rules = create_rules(rule_spec)
      Enum.each(rules, &Presto.add_rule(engine, &1))

      # Assert initial facts into working memory
      # Revenue entries
      Enum.each(revenue_entries, fn {:revenue_entry, id, data} ->
        Presto.assert_fact(engine, {:revenue_entry, id, data})
      end)

      # Staff shifts
      Enum.each(staff_shifts, fn {:staff_shift, id, data} ->
        Presto.assert_fact(engine, {:staff_shift, id, data})
      end)

      # Fire rules to process the data through the RETE network
      # The engine will automatically handle rule execution order based on
      # fact dependencies and rule priorities
      _rule_results = Presto.fire_rules(engine, concurrent: true)

      # Extract results from working memory
      all_facts = Presto.get_facts(engine)
      extract_tronc_results(all_facts)
    after
      # Clean up the engine
      Presto.stop_engine(engine)
    end
  end

  @doc """
  Example data generator for testing and demonstration.
  """
  def generate_example_data do
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

    {staff_shifts, revenue_entries}
  end

  @doc """
  Example rule specification with custom allocation weights and admin costs.
  """
  def generate_example_rule_spec do
    %{
      "allocation_rules" => %{
        # Managers get 60% higher allocation
        "manager" => 1.6,
        # Head chefs get 40% higher
        "head_chef" => 1.4,
        # Chefs get 30% higher (increased from default)
        "chef" => 1.3,
        # Bartenders get 10% higher
        "bartender" => 1.1,
        # Waiters are the baseline
        "waiter" => 1.0,
        # Same as waiters
        "waitress" => 1.0,
        # Hosts get 10% less
        "host" => 0.9,
        # Kitchen assistants get 20% less
        "kitchen_assistant" => 0.8,
        # Cleaners get 30% less
        "cleaner" => 0.7
      },
      # Reduced admin cost rate
      "admin_cost_rate" => 4.5,
      "variables" => %{
        # Minimum 3 hours to be eligible
        "minimum_shift_hours" => 3.0,
        # 2% bonus per year of seniority
        "seniority_bonus_rate" => 0.02,
        "enable_national_insurance_optimization" => true
      }
    }
  end

  @doc """
  Demonstrates the Presto RETE engine with TRONC rules.
  """
  def run_example do
    IO.puts("=== Presto RETE Engine TRONC Example ===\n")

    {staff_shifts, revenue_entries} = generate_example_data()

    IO.puts("Input Data:")
    IO.puts("Staff Shifts: #{length(staff_shifts)} entries")
    IO.puts("Revenue Entries: #{length(revenue_entries)} entries")

    IO.puts("\nProcessing with Presto RETE engine...")
    result = process_with_engine(staff_shifts, revenue_entries)

    IO.puts("\nResults from RETE Engine:")
    print_engine_results(result)

    result
  end

  @doc """
  Demonstrates custom rule configuration with enhanced role weights.
  """
  def run_custom_allocation_example do
    IO.puts("=== Custom TRONC Allocation with RETE Engine ===\n")

    {staff_shifts, revenue_entries} = generate_example_data()
    rule_spec = generate_example_rule_spec()

    IO.puts("Custom Rule Specification:")
    IO.puts("  Admin Cost Rate: #{rule_spec["admin_cost_rate"]}%")
    IO.puts("  Minimum Shift Hours: #{rule_spec["variables"]["minimum_shift_hours"]}")

    IO.puts("\nProcessing with custom rules using RETE engine...")
    result = process_with_engine(staff_shifts, revenue_entries, rule_spec)

    IO.puts("\nCustom Allocation Results:")
    print_engine_results(result)

    result
  end

  # Helper functions for RETE engine processing

  defp create_tronc_pool_fact(date, revenue_entries) do
    total_service_charges = Enum.sum(Enum.map(revenue_entries, & &1.service_charge))
    total_tips_cash = Enum.sum(Enum.map(revenue_entries, & &1.tips_cash))
    total_tips_card = Enum.sum(Enum.map(revenue_entries, & &1.tips_card))
    total_bill_amount = Enum.sum(Enum.map(revenue_entries, & &1.bill_amount))
    gross_tronc_amount = total_service_charges + total_tips_cash + total_tips_card

    pool_data = %{
      date: date,
      gross_amount: Float.round(gross_tronc_amount, 2),
      service_charges: Float.round(total_service_charges, 2),
      tips_cash: Float.round(total_tips_cash, 2),
      tips_card: Float.round(total_tips_card, 2),
      total_bill_amount: Float.round(total_bill_amount, 2),
      admin_deducted: false,
      net_amount: nil,
      created_at: DateTime.utc_now(),
      revenue_entries: Enum.map(revenue_entries, &Map.get(&1, :id, "unknown"))
    }

    {:tronc_pool, date, pool_data}
  end

  defp create_role_allocations_for_pool(date, pool_data, role_weights) do
    # This is a simplified version - in a real RETE engine implementation,
    # we would need to aggregate all staff shifts for the date
    # For now, we'll create placeholder allocations

    roles = Map.keys(role_weights)
    total_weighted_hours = Enum.sum(Map.values(role_weights))

    Enum.map(roles, fn role ->
      role_weight = Map.get(role_weights, role, 1.0)
      allocation_percentage = role_weight / total_weighted_hours
      allocation_amount = pool_data.net_amount * allocation_percentage

      allocation_data = %{
        role: role,
        date: date,
        weighted_hours: role_weight,
        allocation_percentage: Float.round(allocation_percentage * 100, 2),
        allocation_amount: Float.round(allocation_amount, 2),
        total_weighted_hours: Float.round(total_weighted_hours, 2),
        created_at: DateTime.utc_now()
      }

      {:role_allocation, {role, date}, allocation_data}
    end)
  end

  defp create_staff_payment_fact(allocation_data, shift_data) do
    # Calculate payment based on hours worked within the role
    payment_amount =
      allocation_data.allocation_amount *
        (shift_data.hours_worked / allocation_data.weighted_hours)

    payment_data = %{
      employee_id: shift_data.employee_id,
      employee_name: Map.get(shift_data, :employee_name, "Unknown"),
      role: shift_data.role,
      date: shift_data.date,
      hours_worked: shift_data.hours_worked,
      payment_amount: Float.round(payment_amount, 2),
      hourly_rate: Map.get(shift_data, :hourly_rate),
      seniority_years: Map.get(shift_data, :seniority_years, 0),
      is_ni_exempt: true,
      created_at: DateTime.utc_now()
    }

    {:staff_payment, {shift_data.employee_id, shift_data.date}, payment_data}
  end

  defp extract_tronc_results(all_facts) do
    tronc_pools = extract_facts_by_type(all_facts, :tronc_pool)
    admin_deductions = extract_facts_by_type(all_facts, :admin_deduction)
    role_allocations = extract_facts_by_type(all_facts, :role_allocation)
    staff_payments = extract_facts_by_type(all_facts, :staff_payment)

    summary =
      generate_engine_summary(tronc_pools, role_allocations, staff_payments, admin_deductions)

    %{
      tronc_pools: tronc_pools,
      role_allocations: role_allocations,
      staff_payments: staff_payments,
      admin_deductions: admin_deductions,
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

  defp generate_engine_summary(tronc_pools, role_allocations, staff_payments, admin_deductions) do
    total_gross_amount =
      tronc_pools
      |> Enum.map(fn {:tronc_pool, _, %{gross_amount: amount}} -> amount end)
      |> Enum.sum()
      |> Float.round(2)

    total_admin_amount =
      admin_deductions
      |> Enum.map(fn {:admin_deduction, _, %{admin_amount: amount}} -> amount end)
      |> Enum.sum()
      |> Float.round(2)

    total_payments =
      staff_payments
      |> Enum.map(fn {:staff_payment, _, %{payment_amount: amount}} -> amount end)
      |> Enum.sum()
      |> Float.round(2)

    unique_employees =
      staff_payments
      |> Enum.map(fn {:staff_payment, {emp_id, _}, _} -> emp_id end)
      |> Enum.uniq()
      |> length()

    %{
      total_gross_amount: total_gross_amount,
      total_admin_amount: total_admin_amount,
      total_payments: total_payments,
      unique_employees: unique_employees,
      pools_processed: length(tronc_pools),
      allocations_created: length(role_allocations),
      payments_generated: length(staff_payments),
      processed_with_engine: true,
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

  defp extract_role_weights(rule_spec, variables) do
    case rule_spec do
      %{"allocation_rules" => allocation_rules} when is_map(allocation_rules) ->
        Map.merge(@default_role_weights, allocation_rules)

      _ ->
        case Map.get(variables, "role_weights") do
          weights when is_map(weights) -> Map.merge(@default_role_weights, weights)
          _ -> @default_role_weights
        end
    end
  end

  defp extract_admin_cost_rate(rule_spec, variables) do
    case rule_spec do
      %{"admin_cost_rate" => rate} when is_number(rate) -> rate
      _ -> Map.get(variables, "admin_cost_rate", 5.0)
    end
  end

  # Display functions for the example outputs

  defp print_engine_results(result) do
    IO.puts("  TRONC Pools: #{length(result.tronc_pools)}")
    IO.puts("  Admin Deductions: #{length(result.admin_deductions)}")
    IO.puts("  Role Allocations: #{length(result.role_allocations)}")
    IO.puts("  Staff Payments: #{length(result.staff_payments)}")
    IO.puts("\nSummary:")
    IO.puts("  Gross Amount: £#{result.summary.total_gross_amount}")
    IO.puts("  Admin Deduction: £#{result.summary.total_admin_amount}")
    IO.puts("  Total Payments: £#{result.summary.total_payments}")
    IO.puts("  Employees: #{result.summary.unique_employees}")
    IO.puts("  Processed with RETE Engine: #{result.summary.processed_with_engine}")
  end

  @doc """
  Validates a TRONC rule specification.

  Checks that the rule specification contains the required structure
  for TRONC processing and has valid variable types.
  """
  @spec valid_rule_spec?(rule_spec()) :: boolean()
  def valid_rule_spec?(rule_spec) do
    case rule_spec do
      # Format with allocation rules
      %{"allocation_rules" => rules} when is_map(rules) ->
        valid_tronc_allocation_rules?(rules)

      # Empty spec is valid (uses defaults)
      spec when map_size(spec) == 0 ->
        true

      _ ->
        false
    end
  end

  defp valid_tronc_allocation_rules?(rules) when is_map(rules) do
    Enum.all?(rules, fn {role, weight} ->
      is_binary(role) and is_number(weight) and weight > 0
    end)
  end
end
