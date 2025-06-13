# Presto - Configuration-Driven Rules Engine

Presto is a flexible, high-performance rules engine built on the RETE algorithm that lets you define complex business logic through configuration rather than code.

## Why Presto?

**Perfect for scenarios like:**
- 📊 **Payroll Processing**: Calculate overtime, deductions, bonuses with complex rules
- 📋 **Compliance Checking**: Validate data against regulatory requirements  
- 🎯 **Business Rule Validation**: Pricing rules, eligibility checks, approval workflows
- 🔄 **Data Transformation**: Complex ETL logic that changes frequently
- 🏢 **Multi-tenant Systems**: Different rule sets per client or environment

**Key Benefits:**
- **No Deployments for Rule Changes**: Update rules via configuration
- **High Performance**: RETE algorithm efficiently handles large fact sets
- **Type-Safe**: Comprehensive validation and error checking
- **Auditable**: Clear separation between rules and data
- **Testable**: Rules can be tested independently of application logic

## Quick Start (5 minutes)

Let's process employee timesheets and calculate overtime pay:

### 1. Installation

```elixir
# mix.exs
def deps do
  [
    {:presto, "~> 0.1.0"}
  ]
end
```

### 2. Configuration

```elixir
# config/config.exs
config :presto, :rule_registry,
  rules: %{
    "time_calculation" => Presto.Examples.PayrollRules,
    "overtime_check" => Presto.Examples.PayrollRules
  }
```

### 3. Process Timesheet Data

```elixir
# Create timesheet entries
timesheet_data = [
  {:time_entry, "entry_1", %{
    employee_id: "emp_001",
    start_datetime: ~U[2024-01-15 09:00:00Z],
    finish_datetime: ~U[2024-01-15 18:00:00Z],  # 9 hours
    minutes: nil,
    units: nil
  }},
  {:time_entry, "entry_2", %{
    employee_id: "emp_001", 
    start_datetime: ~U[2024-01-16 09:00:00Z],
    finish_datetime: ~U[2024-01-16 19:00:00Z],  # 10 hours
    minutes: nil,
    units: nil
  }}
]

# Define rule specification
rule_spec = %{
  "rules_to_run" => ["time_calculation", "overtime_check"],
  "variables" => %{
    "overtime_threshold" => 40.0  # Hours per week
  }
}

# Process through Presto
result = Presto.Examples.PayrollRules.process_time_entries(timesheet_data, rule_spec)

# Results
IO.inspect(result.summary)
# %{
#   total_employees: 1,
#   total_regular_hours: 19.0,
#   total_overtime_hours: 0.0,  # Under 40 hours for the week
#   employees_with_overtime: 0
# }
```

**🎉 That's it!** You've just processed timesheets with configurable business rules.

## How It Works

Presto uses the **RETE algorithm** to efficiently process rules:

```
Facts (Data) → Rules Engine → Results

timesheet entries → payroll rules → calculated hours + overtime
compliance data → audit rules → violations + penalties  
pricing inputs → business rules → final prices + discounts
```

### Core Components

1. **Facts**: Your data (employee records, transactions, etc.)
2. **Rules**: Business logic defined in modules implementing `Presto.RuleBehaviour`
3. **Rule Specs**: JSON configuration that specifies which rules to run
4. **Working Memory**: RETE engine that efficiently matches facts to rules

## Complete Tutorial: Payroll System

Let's build a complete payroll processing system step by step.

### Step 1: Set Up Your Application

```elixir
# mix.exs
defmodule MyPayroll.MixProject do
  use Mix.Project

  def project do
    [
      app: :my_payroll,
      version: "0.1.0",
      elixir: "~> 1.14",
      deps: deps()
    ]
  end

  defp deps do
    [
      {:presto, "~> 0.1.0"}
    ]
  end
end
```

### Step 2: Configure Rules

```elixir
# config/config.exs
config :presto, :rule_registry,
  rules: %{
    # Basic time calculations
    "time_calculation" => Presto.Examples.PayrollRules,
    "overtime_check" => Presto.Examples.PayrollRules,
    
    # Compliance checking
    "weekly_compliance" => Presto.Examples.ComplianceRules,
    
    # Custom payroll rules (you'll create these)
    "holiday_pay" => MyPayroll.HolidayRules,
    "bonus_calculation" => MyPayroll.BonusRules
  }
```

### Step 3: Create Input Data

```elixir
defmodule MyPayroll.Data do
  def sample_timesheet do
    # Employee worked 4 days, 10 hours each = 40 hours (no overtime)
    base_date = ~D[2024-01-15]
    
    Enum.map(0..3, fn day_offset ->
      date = Date.add(base_date, day_offset)
      start_dt = DateTime.new!(date, ~T[08:00:00], "Etc/UTC")
      finish_dt = DateTime.new!(date, ~T[18:00:00], "Etc/UTC")  # 10 hours
      
      {:time_entry, "entry_#{day_offset}", %{
        employee_id: "emp_001",
        start_datetime: start_dt,
        finish_datetime: finish_dt,
        minutes: nil,
        units: nil
      }}
    end)
  end
  
  def overtime_timesheet do
    # Employee worked 5 days, 10 hours each = 50 hours (10 hours overtime)
    base_date = ~D[2024-01-15]
    
    Enum.map(0..4, fn day_offset ->  # 5 days instead of 4
      date = Date.add(base_date, day_offset)
      start_dt = DateTime.new!(date, ~T[08:00:00], "Etc/UTC")
      finish_dt = DateTime.new!(date, ~T[18:00:00], "Etc/UTC")  # 10 hours
      
      {:time_entry, "entry_#{day_offset}", %{
        employee_id: "emp_001",
        start_datetime: start_dt,
        finish_datetime: finish_dt,
        minutes: nil,
        units: nil
      }}
    end)
  end
end
```

### Step 4: Process Payroll

```elixir
defmodule MyPayroll.Processor do
  def process_weekly_payroll(timesheet_entries, overtime_threshold \\ 40.0) do
    # Create rule specification
    rule_spec = %{
      "rules_to_run" => ["time_calculation", "overtime_check"],
      "variables" => %{
        "overtime_threshold" => overtime_threshold
      }
    }
    
    # Process through Presto
    result = Presto.Examples.PayrollRules.process_time_entries(timesheet_entries, rule_spec)
    
    # Format results for payroll system
    %{
      employee_hours: format_employee_hours(result.processed_entries),
      overtime_pay: format_overtime_pay(result.overtime_entries),
      summary: result.summary,
      processed_at: DateTime.utc_now()
    }
  end
  
  defp format_employee_hours(processed_entries) do
    Enum.map(processed_entries, fn {:time_entry, id, data} ->
      %{
        entry_id: id,
        employee_id: data.employee_id,
        date: DateTime.to_date(data.start_datetime),
        hours_worked: data.units,
        minutes_worked: data.minutes
      }
    end)
  end
  
  defp format_overtime_pay(overtime_entries) do
    Enum.map(overtime_entries, fn {:overtime_entry, {employee_id, week_start}, data} ->
      %{
        employee_id: employee_id,
        week_starting: week_start,
        overtime_hours: data.units,
        overtime_rate: 1.5,  # Time and a half
        created_at: data.created_at
      }
    end)
  end
end
```

### Step 5: Run and Test

```elixir
# No overtime scenario
regular_timesheet = MyPayroll.Data.sample_timesheet()
regular_result = MyPayroll.Processor.process_weekly_payroll(regular_timesheet)

IO.inspect(regular_result.summary)
# %{
#   total_employees: 1,
#   total_regular_hours: 40.0,
#   total_overtime_hours: 0.0,
#   employees_with_overtime: 0,
#   total_hours: 40.0
# }

# Overtime scenario  
overtime_timesheet = MyPayroll.Data.overtime_timesheet()
overtime_result = MyPayroll.Processor.process_weekly_payroll(overtime_timesheet)

IO.inspect(overtime_result.summary)
# %{
#   total_employees: 1,
#   total_regular_hours: 50.0,
#   total_overtime_hours: 10.0,  # 10 hours over 40-hour threshold
#   employees_with_overtime: 1,
#   total_hours: 60.0
# }

IO.inspect(overtime_result.overtime_pay)
# [%{
#   employee_id: "emp_001",
#   week_starting: ~D[2024-01-15],
#   overtime_hours: 10.0,
#   overtime_rate: 1.5,
#   created_at: ~U[2024-01-20 10:30:00.123456Z]
# }]
```

### Step 6: Add Compliance Checking

```elixir
defmodule MyPayroll.ComplianceProcessor do
  def check_compliance(timesheet_entries, max_weekly_hours \\ 48.0) do
    rule_spec = %{
      "rules_to_run" => ["weekly_compliance"],
      "variables" => %{
        "max_weekly_hours" => max_weekly_hours
      }
    }
    
    # First process hours
    payroll_result = Presto.Examples.PayrollRules.process_time_entries(timesheet_entries)
    
    # Then check compliance using the processed entries
    compliance_result = Presto.Examples.ComplianceRules.process_compliance_check(
      payroll_result.processed_entries, 
      rule_spec
    )
    
    %{
      payroll: payroll_result,
      compliance: compliance_result,
      violations: extract_violations(compliance_result.compliance_results)
    }
  end
  
  defp extract_violations(compliance_results) do
    compliance_results
    |> Enum.filter(fn {:compliance_result, _, %{status: status}} -> 
      status == :non_compliant 
    end)
    |> Enum.map(fn {:compliance_result, {employee_id, week_start}, data} ->
      %{
        employee_id: employee_id,
        week_start: week_start,
        violation_type: "excessive_hours",
        actual_hours: data.actual_value,
        max_allowed: data.threshold,
        penalty_recommended: true
      }
    end)
  end
end

# Test compliance
overtime_timesheet = MyPayroll.Data.overtime_timesheet()
compliance_result = MyPayroll.ComplianceProcessor.check_compliance(overtime_timesheet, 45.0)

IO.inspect(compliance_result.violations)
# [%{
#   employee_id: "emp_001",
#   week_start: ~D[2024-01-15],
#   violation_type: "excessive_hours", 
#   actual_hours: 50.0,
#   max_allowed: 45.0,
#   penalty_recommended: true
# }]
```

## Using the RETE Engine Directly

For advanced use cases, you can interact with the RETE engine directly:

```elixir
defmodule MyApp.ReteExample do
  def process_with_rete_engine(facts, rule_spec) do
    # Get configured rule module
    {:ok, rule_module} = Presto.RuleRegistry.get_rule_module("time_calculation")
    
    # Create rules from specification
    rules = rule_module.create_rules(rule_spec)
    
    # Start working memory
    {:ok, working_memory} = Presto.WorkingMemory.start_link()
    
    # Start rule engine with rules
    {:ok, rule_engine} = Presto.RuleEngine.start_link(working_memory, rules)
    
    # Assert facts into working memory
    Enum.each(facts, fn fact ->
      Presto.WorkingMemory.assert_fact(working_memory, fact)
    end)
    
    # Execute rules
    Presto.RuleEngine.execute_rules(rule_engine)
    
    # Get all facts (original + derived)
    all_facts = Presto.WorkingMemory.get_all_facts(working_memory)
    
    # Clean up
    GenServer.stop(rule_engine)
    GenServer.stop(working_memory)
    
    all_facts
  end
end
```

## JSON Rule Specifications

Presto uses simple, declarative JSON to specify which rules to run:

```json
{
  "rules_to_run": ["time_calculation", "overtime_check"],
  "variables": {
    "overtime_threshold": 40.0,
    "overtime_multiplier": 1.5,
    "holiday_rate": 2.0
  }
}
```

### Dynamic Rule Configuration

```elixir
# Load rules from database
def load_client_rules(client_id) do
  case MyApp.ClientRules.get_rules(client_id) do
    %{overtime_threshold: threshold, holiday_multiplier: holiday_rate} ->
      %{
        "rules_to_run" => ["time_calculation", "overtime_check", "holiday_pay"],
        "variables" => %{
          "overtime_threshold" => threshold,
          "holiday_multiplier" => holiday_rate
        }
      }
    
    nil ->
      # Fallback to default rules
      %{
        "rules_to_run" => ["time_calculation"],
        "variables" => %{}
      }
  end
end

# Use in processing
rule_spec = load_client_rules("client_123")
result = process_payroll(timesheet_data, rule_spec)
```

## Creating Custom Rule Modules

When the built-in examples aren't enough, create your own rule modules:

### 1. Define Your Rule Module

```elixir
defmodule MyApp.CustomPayrollRules do
  @behaviour Presto.RuleBehaviour

  @impl Presto.RuleBehaviour
  def create_rules(rule_spec) do
    variables = Map.get(rule_spec, "variables", %{})
    requested_rules = Map.get(rule_spec, "rules_to_run", [])
    
    # Only create rules that were requested
    rules = []
    
    rules = if "holiday_pay" in requested_rules do
      [holiday_pay_rule(variables) | rules]
    else
      rules
    end
    
    rules = if "bonus_calculation" in requested_rules do
      [bonus_calculation_rule(variables) | rules]
    else
      rules
    end
    
    rules
  end

  @impl Presto.RuleBehaviour
  def valid_rule_spec?(%{"rules_to_run" => rules, "variables" => variables}) 
      when is_list(rules) and is_map(variables) do
    supported_rules = ["holiday_pay", "bonus_calculation"]
    Enum.any?(rules, &(&1 in supported_rules))
  end
  def valid_rule_spec?(_), do: false

  # Holiday pay rule: 2x rate for designated holidays
  defp holiday_pay_rule(variables) do
    holiday_multiplier = Map.get(variables, "holiday_multiplier", 2.0)
    
    %{
      name: :holiday_pay,
      pattern: fn facts ->
        # Find time entries on holidays
        facts
        |> Enum.filter(&holiday_time_entry?/1)
      end,
      action: fn holiday_entries ->
        # Create holiday pay entries
        Enum.map(holiday_entries, fn {:time_entry, id, data} ->
          holiday_hours = data.units
          holiday_pay = holiday_hours * holiday_multiplier
          
          {:holiday_pay, id, %{
            employee_id: data.employee_id,
            date: DateTime.to_date(data.start_datetime),
            hours: holiday_hours,
            multiplier: holiday_multiplier,
            total_pay_hours: holiday_pay
          }}
        end)
      end
    }
  end
  
  # Bonus calculation rule: performance bonuses
  defp bonus_calculation_rule(variables) do
    performance_threshold = Map.get(variables, "performance_threshold", 90)
    bonus_rate = Map.get(variables, "bonus_rate", 0.1)
    
    %{
      name: :bonus_calculation,
      pattern: fn facts ->
        # Find performance records above threshold
        facts
        |> Enum.filter(fn
          {:performance_record, _, %{score: score}} when score >= performance_threshold -> true
          _ -> false
        end)
      end,
      action: fn performance_records ->
        Enum.map(performance_records, fn {:performance_record, id, data} ->
          bonus_amount = data.base_salary * bonus_rate * (data.score / 100)
          
          {:bonus_award, id, %{
            employee_id: data.employee_id,
            performance_score: data.score,
            base_salary: data.base_salary,
            bonus_rate: bonus_rate,
            bonus_amount: bonus_amount,
            awarded_at: DateTime.utc_now()
          }}
        end)
      end
    }
  end
  
  defp holiday_time_entry?({:time_entry, _, %{start_datetime: start_dt}}) do
    date = DateTime.to_date(start_dt)
    is_holiday?(date)
  end
  defp holiday_time_entry?(_), do: false
  
  defp is_holiday?(date) do
    # Simplified holiday detection
    holidays = [
      ~D[2024-01-01],  # New Year's Day
      ~D[2024-07-04],  # Independence Day
      ~D[2024-12-25]   # Christmas
    ]
    
    date in holidays
  end
end
```

### 2. Register Your Rules

```elixir
# config/config.exs
config :presto, :rule_registry,
  rules: %{
    # Built-in rules
    "time_calculation" => Presto.Examples.PayrollRules,
    "overtime_check" => Presto.Examples.PayrollRules,
    
    # Your custom rules
    "holiday_pay" => MyApp.CustomPayrollRules,
    "bonus_calculation" => MyApp.CustomPayrollRules
  }
```

### 3. Use Your Custom Rules

```elixir
# Process holiday timesheet
holiday_timesheet = [
  {:time_entry, "holiday_1", %{
    employee_id: "emp_001",
    start_datetime: ~U[2024-07-04 09:00:00Z],  # July 4th
    finish_datetime: ~U[2024-07-04 17:00:00Z],
    minutes: nil,
    units: nil
  }}
]

rule_spec = %{
  "rules_to_run" => ["time_calculation", "holiday_pay"],
  "variables" => %{
    "holiday_multiplier" => 2.5  # 2.5x pay on holidays
  }
}

# Process with custom rules
{:ok, rule_module} = Presto.RuleRegistry.get_rule_module("holiday_pay")
rules = rule_module.create_rules(rule_spec)

# Execute rules (simplified - in practice you'd use the RETE engine)
calculated_entries = apply_time_calculation(holiday_timesheet)
holiday_pay_entries = apply_holiday_rules(calculated_entries, rule_spec)

IO.inspect(holiday_pay_entries)
# [%{
#   employee_id: "emp_001",
#   date: ~D[2024-07-04],
#   hours: 8.0,
#   multiplier: 2.5,
#   total_pay_hours: 20.0  # 8 hours * 2.5 multiplier
# }]
```

## Advanced Configuration

### Environment-Specific Rules

```elixir
# config/dev.exs
config :presto, :rule_registry,
  rules: %{
    "time_calculation" => Presto.Examples.PayrollRules,
    "overtime_check" => Presto.Examples.PayrollRules,
    "debug_logging" => MyApp.DebugRules
  }

# config/prod.exs  
config :presto, :rule_registry,
  rules: %{
    "time_calculation" => MyApp.OptimizedPayrollRules,
    "overtime_check" => MyApp.OptimizedPayrollRules,
    "audit_trail" => MyApp.AuditRules,
    "fraud_detection" => MyApp.FraudRules
  }

# config/test.exs
config :presto, :rule_registry,
  rules: %{
    "time_calculation" => MyApp.TestPayrollRules,
    "mock_overtime" => MyApp.MockRules
  }
```

### Multi-Tenant Configuration

```elixir
defmodule MyApp.TenantRules do
  def get_rules_for_tenant(tenant_id) do
    case tenant_id do
      "enterprise_client" ->
        %{
          "time_calculation" => MyApp.EnterprisePayrollRules,
          "overtime_check" => MyApp.EnterprisePayrollRules,
          "complex_compliance" => MyApp.EnterpriseComplianceRules
        }
      
      "small_business" ->
        %{
          "time_calculation" => Presto.Examples.PayrollRules,
          "overtime_check" => Presto.Examples.PayrollRules
        }
      
      _ ->
        %{
          "time_calculation" => MyApp.StandardPayrollRules
        }
    end
  end
end

# Usage in your application
def process_tenant_payroll(tenant_id, timesheet_data) do
  # Temporarily register tenant-specific rules
  tenant_rules = MyApp.TenantRules.get_rules_for_tenant(tenant_id)
  
  Enum.each(tenant_rules, fn {rule_name, rule_module} ->
    Presto.RuleRegistry.register_rule(rule_name, rule_module)
  end)
  
  # Process with tenant rules
  rule_spec = build_rule_spec_for_tenant(tenant_id)
  result = process_payroll(timesheet_data, rule_spec)
  
  result
end
```

## Troubleshooting

### Common Issues

**"No rules configured" Error**
```elixir
# Problem: Empty or missing configuration
# Solution: Check your config/config.exs

case Presto.RuleRegistry.validate_configuration() do
  :ok -> 
    IO.puts("Configuration is valid")
  {:error, errors} -> 
    IO.inspect(errors)
    # Add missing configuration
end
```

**"Module doesn't implement behavior" Error**
```elixir
# Problem: Your rule module is missing @behaviour declaration
# Solution: Add behavior implementation

defmodule MyApp.MyRules do
  @behaviour Presto.RuleBehaviour  # ← Add this!
  
  @impl Presto.RuleBehaviour
  def create_rules(rule_spec), do: # ...
  
  @impl Presto.RuleBehaviour  
  def valid_rule_spec?(rule_spec), do: # ...
end
```

**Rule Validation Failures**
```elixir
# Check rule specification validity
rule_spec = %{
  "rules_to_run" => ["invalid_rule"],  # ← Problem: rule doesn't exist
  "variables" => %{}
}

case Presto.RuleRegistry.valid_rule_spec?(rule_spec) do
  true -> IO.puts("Valid!")
  false -> 
    IO.puts("Invalid rule spec")
    IO.inspect(Presto.RuleRegistry.list_rule_names(), label: "Available rules")
end
```

**Performance Issues**
```elixir
# For large datasets, consider batching
def process_large_dataset(large_timesheet) do
  large_timesheet
  |> Enum.chunk_every(1000)  # Process 1000 entries at a time
  |> Enum.map(&process_batch/1)
  |> Enum.reduce(&merge_results/2)
end

# Monitor processing time
def process_with_timing(timesheet_data, rule_spec) do
  start_time = System.monotonic_time(:millisecond)
  
  result = process_payroll(timesheet_data, rule_spec)
  
  end_time = System.monotonic_time(:millisecond)
  processing_time = end_time - start_time
  
  {result, processing_time}
end
```

### Performance Optimization

**Efficient Fact Structures**
```elixir
# Good: Structured facts with clear types
{:time_entry, id, %{employee_id: "emp_001", hours: 8.0}}

# Avoid: Unstructured data
%{data: %{employee: %{id: "emp_001", time: %{hours: 8.0}}}}
```

**Rule Optimization**
```elixir
# Good: Specific pattern matching
def efficient_rule do
  %{
    name: :overtime_check,
    pattern: fn facts ->
      # Filter first, then process
      facts
      |> Enum.filter(&is_processed_time_entry?/1)
      |> Enum.group_by(&get_employee_id/1)
    end,
    action: fn grouped_entries ->
      # Efficient processing
    end
  }
end
```

## Best Practices

### 1. Rule Design

- **Single Responsibility**: Each rule should do one thing well
- **Immutable Facts**: Don't modify existing facts, create new ones
- **Clear Naming**: Use descriptive rule names and fact types
- **Validation**: Always implement `valid_rule_spec?/1`

### 2. Performance

- **Batch Processing**: Process related facts together
- **Efficient Patterns**: Use specific pattern matching
- **Monitor Memory**: Watch fact accumulation in long-running processes
- **Profile Rules**: Measure rule execution time

### 3. Testing

```elixir
defmodule MyApp.PayrollRulesTest do
  use ExUnit.Case
  
  test "calculates overtime correctly" do
    # Use dynamic registration for testing
    Presto.RuleRegistry.register_rule("test_overtime", MyApp.PayrollRules)
    
    timesheet = create_overtime_timesheet()
    rule_spec = %{
      "rules_to_run" => ["test_overtime"],
      "variables" => %{"overtime_threshold" => 40.0}
    }
    
    result = process_payroll(timesheet, rule_spec)
    
    assert result.summary.total_overtime_hours > 0
  end
  
  defp create_overtime_timesheet do
    # Test data creation helpers
  end
end
```

### 4. Production Deployment

- **Configuration Validation**: Validate rules on application startup
- **Error Handling**: Graceful degradation when rules fail
- **Monitoring**: Track rule execution metrics
- **Rollback Strategy**: Keep previous rule versions for rollback

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    # Validate rule configuration at startup
    case Presto.RuleRegistry.validate_configuration() do
      :ok -> 
        IO.puts("All rules configured correctly")
      {:error, errors} ->
        IO.inspect(errors, label: "Rule configuration errors")
        # Decide: fail fast or use default rules
    end
    
    # Start your supervision tree
    children = [
      # ... your processes
    ]
    
    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

## Architecture Overview

Presto implements the **RETE algorithm** for efficient rule processing:

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│    Facts    │───▶│ Alpha Network │───▶│ Beta Network│
│ (Your Data) │    │ (Filtering)   │    │ (Joining)   │
└─────────────┘    └──────────────┘    └─────────────┘
                                              │
                                              ▼
                   ┌─────────────┐    ┌──────────────┐
                   │   Results   │◀───│ Rule Actions │
                   │(New Facts)  │    │ (Processing) │
                   └─────────────┘    └──────────────┘
```

**When to use Presto:**
- ✅ Complex business rules (>10 conditions)
- ✅ Rules that change frequently  
- ✅ Large datasets (>1000 facts)
- ✅ Need for rule auditing
- ✅ Multiple rule sets (multi-tenant)

**When NOT to use Presto:**
- ❌ Simple if/then logic (< 5 rules)
- ❌ One-time data processing
- ❌ Static rules that never change
- ❌ Very simple applications

## Example Rule Modules

Presto includes production-ready examples:

- **`Presto.Examples.PayrollRules`** - Time calculation, overtime processing
- **`Presto.Examples.ComplianceRules`** - Weekly hour compliance, violation tracking
- **`Presto.Examples.CaliforniaSpikeBreakRules`** - Complex jurisdiction-aware break rules

Study these examples in `lib/presto/examples/` for patterns and best practices.

## Migration Guide

### From Hardcoded Rules

1. **Identify Current Rules**: List all business logic that could become rules
2. **Group by Domain**: Payroll rules, compliance rules, etc.
3. **Create Rule Modules**: Implement `Presto.RuleBehaviour`
4. **Add Configuration**: Define rules in `config/config.exs`
5. **Migrate Gradually**: Replace hardcoded logic incrementally

### From Other Rules Engines

- **Drools**: Presto uses Elixir instead of Java, focus on functional patterns
- **Business Rules Management Systems**: Presto is code-based, not GUI-based
- **Custom Rule Systems**: Presto provides the RETE algorithm and configuration management

## Contributing

Found a bug or want to contribute? Check out the project on GitHub!

## License

This library is designed to be flexible and extensible. The example rule modules are provided as templates - you're encouraged to create your own rule implementations that fit your specific business needs.