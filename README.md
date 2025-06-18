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
- **Configurable Rule Ordering**: Control execution sequence through JSON specifications
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
    {:ex_presto, "~> 0.1.0"}
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

# Process through Presto RETE engine
result = Presto.Examples.PayrollRules.process_with_engine(timesheet_data, rule_spec)

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

### Quick Start Workflow

```mermaid
flowchart TD
    A[Install Presto] --> B[Configure Rules]
    B --> C[Create Timesheet Data]
    C --> D[Define Rule Specification]
    D --> E[Process with RETE Engine]
    E --> F[Get Results]
    
    B1["config :presto, :rule_registry"] --> B
    C1["time_entry facts"] --> C
    D1["rules_to_run + variables"] --> D
    E1["Presto.Examples.PayrollRules.process_with_engine"] --> E
    F1["overtime calculations + summaries"] --> F
    
    style A fill:#e1f5fe
    style F fill:#c8e6c9
    style E fill:#fff3e0
```

## How It Works

Presto uses the **RETE algorithm** to efficiently process rules:

```mermaid
flowchart LR
    A[Facts<br/>Your Data] --> B[Rules Engine<br/>RETE Algorithm]
    B --> C[Results<br/>Processed Output]
    
    A1["📊 timesheet entries"] --> B1["⚙️ payroll rules"]
    B1 --> C1["💰 calculated hours + overtime"]
    
    A2["📋 compliance data"] --> B2["🔍 audit rules"]
    B2 --> C2["⚠️ violations + penalties"]
    
    A3["💼 pricing inputs"] --> B3["📈 business rules"]
    B3 --> C3["💲 final prices + discounts"]
    
    style A fill:#e3f2fd
    style B fill:#fff3e0
    style C fill:#e8f5e8
    
    style A1 fill:#e3f2fd
    style B1 fill:#fff3e0
    style C1 fill:#e8f5e8
    
    style A2 fill:#e3f2fd
    style B2 fill:#fff3e0
    style C2 fill:#e8f5e8
    
    style A3 fill:#e3f2fd
    style B3 fill:#fff3e0
    style C3 fill:#e8f5e8
```

### Core Components

1. **Facts**: Your data (employee records, transactions, etc.)
2. **Rules**: Business logic defined with RETE conditions and actions
3. **Rule Specs**: JSON configuration that specifies which rules to run
4. **Working Memory**: RETE engine that efficiently matches facts to rules

### RETE Engine Architecture

```mermaid
flowchart TD
    Facts["📥 Input Facts<br/>(timesheet entries)"] --> Alpha["🔍 Alpha Network<br/>(Pattern Matching)"]
    Alpha --> Beta["🔗 Beta Network<br/>(Join Operations)"]
    Beta --> Agenda["📋 Rule Agenda<br/>(Priority Ordering)"]
    Agenda --> Actions["⚡ Rule Actions<br/>(Concurrent Execution)"]
    Actions --> Results["📤 Output Facts<br/>(overtime calculations)"]
    
    WM[("💾 Working Memory<br/>(ETS Storage)")]
    
    Facts --> WM
    WM --> Alpha
    Beta --> WM
    Actions --> WM
    WM --> Results
    
    Alpha --> AM1["α₁ time_entry patterns"]
    Alpha --> AM2["α₂ employee patterns"]
    
    Beta --> BM1["β₁ joined time+employee"]
    Beta --> BM2["β₂ aggregated hours"]
    
    style Facts fill:#e3f2fd
    style Alpha fill:#f3e5f5
    style Beta fill:#e8f5e8
    style Agenda fill:#fff3e0
    style Actions fill:#fce4ec
    style Results fill:#e0f2f1
    style WM fill:#f5f5f5
```

Presto's RETE engine provides:

- **Incremental Processing**: Only processes changes (fact deltas)
- **Pattern Matching**: Efficient condition evaluation using Elixir's pattern matching
- **Join Operations**: Complex multi-fact relationships with indexed lookups  
- **Rule Priorities**: Configurable execution order for rule dependencies
- **Concurrent Execution**: Parallel rule firing with automatic conflict resolution
- **Memory Efficiency**: ETS-based storage optimised for read/write patterns

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
    
    # Process through Presto RETE engine
    result = Presto.Examples.PayrollRules.process_with_engine(timesheet_entries, rule_spec)
    
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

```mermaid
flowchart TD
    subgraph "Regular Scenario (40 hours)"
        R1["📊 4 days × 10 hours = 40 hours"] --> R2["⚙️ RETE Engine Processing"]
        R2 --> R3["✅ No overtime detected"]
        R3 --> R4["📈 total_overtime_hours: 0.0"]
    end
    
    subgraph "Overtime Scenario (50 hours)"
        O1["📊 5 days × 10 hours = 50 hours"] --> O2["⚙️ RETE Engine Processing"]
        O2 --> O3["⚠️ 10 hours over threshold"]
        O3 --> O4["📈 total_overtime_hours: 10.0"]
        O4 --> O5["💰 overtime_rate: 1.5x"]
    end
    
    subgraph "Processing Pipeline"
        P1["timesheet_entries"] --> P2["process_weekly_payroll()"]
        P2 --> P3["RETE rule execution"]
        P3 --> P4["formatted results"]
    end
    
    style R3 fill:#c8e6c9
    style O3 fill:#ffcdd2
    style P3 fill:#fff3e0
```

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
    
    # First process hours with RETE engine
    payroll_result = Presto.Examples.PayrollRules.process_with_engine(timesheet_entries)
    
    # Then check compliance using the RETE engine
    compliance_result = Presto.Examples.ComplianceRules.process_with_engine(
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

### API Usage Sequence

```mermaid
sequenceDiagram
    participant App as Application
    participant Presto as Presto Engine
    participant ETS as ETS Memory
    participant Rules as Rule Execution
    
    App->>Presto: start_link(rules)
    Presto->>ETS: Initialise working memory
    ETS-->>Presto: Memory ready
    Presto-->>App: {:ok, engine}
    
    loop For each fact
        App->>Presto: assert_fact(engine, fact)
        Presto->>ETS: Store fact
        Presto->>Rules: Check rule conditions
        alt Rule conditions met
            Rules->>Rules: Execute rule action
            Rules->>ETS: Assert new facts
            Rules-->>Presto: Rule fired
        end
    end
    
    App->>Presto: get_facts(engine)
    Presto->>ETS: Retrieve all facts
    ETS-->>Presto: Facts list
    Presto-->>App: All facts
    
    App->>Presto: stop(engine)
    Presto->>ETS: Cleanup memory
    ETS-->>Presto: Cleaned up
    Presto-->>App: :ok
```

```elixir
defmodule MyApp.DirectEngineExample do
  def process_with_direct_engine() do
    # Define rules directly
    rules = [
      %Presto.Rule{
        name: "adult_discount",
        conditions: [
          {:person, :name, :age},
          {:order, :name, :total}
        ],
        guards: [
          {:>=, :age, 18},
          {:>, :total, 100}
        ],
        action: {:apply_discount, 0.1}
      }
    ]
    
    # Start engine with rules
    {:ok, engine} = Presto.start_link(rules)
    
    # Assert facts
    :ok = Presto.assert_fact(engine, {:person, "John", 25})
    :ok = Presto.assert_fact(engine, {:order, "John", 150})
    
    # For manual execution mode:
    # {:ok, engine} = Presto.start_link(rules, execution_mode: :manual)
    # {fired_rules, new_facts} = Presto.run_cycle(engine)
    
    # Get results
    facts = Presto.get_facts(engine)
    
    # Clean up
    Presto.stop(engine)
    
    facts
  end
  
  def advanced_engine_usage() do
    rules = create_complex_rules()
    
    # Start with configuration
    {:ok, engine} = Presto.start_link(rules, 
      execution_mode: :automatic,
      max_rule_executions: 1000,
      name: MyRulesEngine
    )
    
    # Batch operations for performance
    facts = [
      {:person, "John", 25},
      {:person, "Jane", 30},
      {:order, "John", 150},
      {:order, "Jane", 200}
    ]
    
    :ok = Presto.assert_facts(engine, facts)
    
    # Monitor execution
    :ok = Presto.subscribe(engine, self())
    
    # Receive notifications:
    # {:rule_fired, %{rule: "adult_discount", bindings: %{name: "John", age: 25}}}
    # {:fact_asserted, {:person, "John", 25}}
    
    # Get statistics
    stats = Presto.get_statistics(engine)
    
    Presto.stop(engine)
  end
end
```

## JSON Rule Specifications

Presto uses simple, declarative JSON to specify which rules to run and in what order:

### Basic Rule Specification
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

### Advanced Rule Ordering and Configuration
```json
{
  "rule_execution_order": ["time_calculation", "pay_aggregation", "overtime_processing"],
  "overtime_rules": [
    {
      "name": "overtime_basic_priority_1",
      "priority": 1,
      "threshold": 15,
      "filter_pay_code": "basic_pay",
      "pay_code": "overtime_basic_pay"
    },
    {
      "name": "overtime_special_priority_2", 
      "priority": 2,
      "threshold": 15,
      "filter_pay_code": "special_pay",
      "pay_code": "overtime_special_pay"
    },
    {
      "name": "overtime_general_priority_3",
      "priority": 3,
      "threshold": 5,
      "filter_pay_code": null,
      "pay_code": "overtime_rest"
    }
  ],
  "variables": {
    "max_overtime_hours": 20,
    "calculation_precision": 2
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
        "rule_execution_order" => ["time_calculation", "pay_aggregation", "overtime_processing"],
        "overtime_rules" => [
          %{
            "name" => "client_overtime",
            "priority" => 1,
            "threshold" => threshold,
            "filter_pay_code" => "basic_pay",
            "pay_code" => "overtime_pay"
          }
        ],
        "variables" => %{
          "overtime_threshold" => threshold,
          "holiday_multiplier" => holiday_rate
        }
      }
    
    nil ->
      # Fallback to default rules
      %{
        "rule_execution_order" => ["time_calculation"],
        "variables" => %{}
      }
  end
end

# Use in processing
rule_spec = load_client_rules("client_123")
result = process_payroll(timesheet_data, rule_spec)
```

## Creating Custom Rules

Define your own rules using the direct Rule struct approach:

### 1. Define Custom Rules

```elixir
defmodule MyApp.CustomRules do
  def holiday_pay_rules(variables \\ %{}) do
    holiday_multiplier = Map.get(variables, "holiday_multiplier", 2.0)
    
    [
      %Presto.Rule{
        name: "holiday_pay_calculation",
        conditions: [
          {:time_entry, :id, :employee_id, :date, :hours},
          {:holiday, :date}  # Join on date field
        ],
        guards: [
          {:>, :hours, 0}
        ],
        action: fn bindings ->
          holiday_pay = bindings.hours * holiday_multiplier
          {:holiday_pay, bindings.id, %{
            employee_id: bindings.employee_id,
            date: bindings.date,
            regular_hours: bindings.hours,
            holiday_multiplier: holiday_multiplier,
            total_pay_hours: holiday_pay
          }}
        end
      }
    ]
  end
  
  def bonus_calculation_rules(variables \\ %{}) do
    performance_threshold = Map.get(variables, "performance_threshold", 90)
    bonus_rate = Map.get(variables, "bonus_rate", 0.1)
    
    [
      %Presto.Rule{
        name: "performance_bonus",
        conditions: [
          {:employee, :id, :base_salary},
          {:performance_review, :id, :score}
        ],
        guards: [
          {:>=, :score, performance_threshold}
        ],
        action: fn bindings ->
          bonus_amount = bindings.base_salary * bonus_rate * (bindings.score / 100)
          {:bonus_award, bindings.id, %{
            employee_id: bindings.id,
            performance_score: bindings.score,
            base_salary: bindings.base_salary,
            bonus_rate: bonus_rate,
            bonus_amount: bonus_amount,
            awarded_at: DateTime.utc_now()
          }}
        end,
        priority: 10
      }
    ]
  end
  
  def complex_eligibility_rules() do
    [
      %Presto.Rule{
        name: "senior_discount_eligibility",
        conditions: [
          {:person, :name, :age, :membership_level},
          {:order, :name, :total, :items}
        ],
        guards: [
          {:>=, :age, 65},
          {:==, :membership_level, :premium},
          {:>, :total, 50},
          {:>, {:length, :items}, 2}
        ],
        action: {:apply_senior_discount, [:name, :total]},
        priority: 20
      },
      
      %Presto.Rule{
        name: "bulk_discount",
        conditions: [
          {:order, :customer, :total, :items}
        ],
        guards: [
          {:>, {:length, :items}, 10}
        ],
        action: {MyApp.DiscountProcessor, :apply_bulk_discount, [:customer, :total]},
        priority: 15
      }
    ]
  end
end
```

### 2. Use Your Custom Rules

```elixir
defmodule MyApp.PayrollProcessor do
  def process_with_custom_rules() do
    # Create rule sets
    holiday_rules = MyApp.CustomRules.holiday_pay_rules(%{"holiday_multiplier" => 2.5})
    bonus_rules = MyApp.CustomRules.bonus_calculation_rules(%{"performance_threshold" => 85})
    eligibility_rules = MyApp.CustomRules.complex_eligibility_rules()
    
    # Combine all rules
    all_rules = holiday_rules ++ bonus_rules ++ eligibility_rules
    
    # Start engine with custom rules
    {:ok, engine} = Presto.start_link(all_rules)
    
    # Assert facts
    facts = [
      {:time_entry, "entry_1", "emp_001", ~D[2024-07-04], 8.0},
      {:holiday, ~D[2024-07-04]},
      {:employee, "emp_001", 75_000},
      {:performance_review, "emp_001", 92}
    ]
    
    :ok = Presto.assert_facts(engine, facts)
    
    # Get results
    results = Presto.get_facts(engine)
    
    Presto.stop(engine)
    
    results
  end
  
  def process_discount_eligibility() do
    rules = MyApp.CustomRules.complex_eligibility_rules()
    {:ok, engine} = Presto.start_link(rules)
    
    # Assert customer and order facts
    :ok = Presto.assert_fact(engine, {:person, "John", 67, :premium})
    :ok = Presto.assert_fact(engine, {:order, "John", 150, ["item1", "item2", "item3"]})
    
    # Rules will automatically fire and create discount facts
    results = Presto.get_facts(engine)
    
    Presto.stop(engine)
    results
  end
end
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

**"Module doesn't implement behaviour" Error**
```elixir
# Problem: Your rule module is missing @behaviour declaration
# Solution: Add behaviour implementation

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

Presto includes comprehensive performance optimisations based on advanced RETE implementations:

#### ETS Optimization
```elixir
# Optimized ETS configuration for different access patterns
config :presto, :engine,
  memory_config: %{
    working_memory: [:set, :public, {:read_concurrency, true}],
    alpha_memories: [:bag, :public, {:read_concurrency, true}],
    beta_memories: [:bag, :public, {:write_concurrency, true}]
  }
```

#### Pattern Compilation
```elixir
# Compile-time pattern optimisation
defmodule MyApp.OptimizedRules do
  # Patterns are compiled into efficient matching functions
  %Presto.Rule{
    name: "optimised_pattern",
    conditions: [
      {:person, :name, :age},  # Compiled to efficient matcher
      {:order, :name, :total}  # Indexed joins for performance
    ],
    guards: [
      {:>=, :age, 18}  # Guard optimisation with selectivity analysis
    ]
  }
end
```

#### Performance Monitoring
```elixir
# Built-in performance monitoring
defmodule MyApp.MonitoredEngine do
  def start_with_monitoring() do
    {:ok, engine} = Presto.start_link(rules, 
      optimisations: %{
        indexing: %{enabled: true, strategy: :hash},
        compilation: %{enabled: true, optimisation_level: :aggressive},
        memory: %{enabled: true, cache_optimisation: true}
      }
    )
    
    # Get performance statistics
    stats = Presto.get_statistics(engine)
    # %{
    #   total_rule_executions: 1250,
    #   average_cycle_time: 15.7, # milliseconds
    #   memory_usage: %{working_memory: 2048}, # KB
    #   rule_statistics: %{"adult_discount" => %{executions: 45, avg_time: 2.1}}
    # }
    
    engine
  end
end
```

#### Optimization Layers
```elixir
# Advanced optimisation configuration
config :presto, :optimisations,
  # Indexing optimisations for join performance
  indexing: %{
    enabled: true,
    join_indexing: %{strategy: :hash, rebuild_threshold: 1000},
    type_discrimination: %{enabled: true, cache_size: 10000}
  },
  
  # Pattern compilation optimisations
  compilation: %{
    enabled: true,
    pattern_compilation: %{compile_at_startup: true, optimisation_level: :aggressive},
    guard_optimisation: %{enabled: true, reorder_guards: true}
  },
  
  # Memory optimisations
  memory: %{
    enabled: true,
    cache_optimisation: %{enabled: true, prefetch_strategy: :sequential},
    gc_optimisation: %{cleanup_interval: 60_000}
  }
```

**Performance Targets:**
- **Fact Assertion**: 10,000+ facts/second for simple patterns
- **Rule Execution**: 1,000+ rule fires/second with complex conditions  
- **Memory Efficiency**: <100MB for 10,000 facts with 100 rules
- **Latency**: <1ms for simple rule activation, <10ms for complex joins

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

- **Engine Supervision**: Proper supervision tree integration
- **Error Handling**: Graceful degradation when rules fail
- **Monitoring**: Track rule execution metrics with built-in statistics
- **Configuration Management**: Environment-specific rule optimisation

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    # Define rules for this environment
    rules = load_production_rules()
    
    # Start your supervision tree
    children = [
      # Presto engine as supervised child
      {Presto, [rules, [name: MyApp.RulesEngine, 
                        optimisations: production_optimisations()]]}
      
      # Your other processes
      MyApp.DataProcessor,
      MyApp.APIServer
    ]
    
    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
  
  defp production_optimisations() do
    %{
      indexing: %{enabled: true, strategy: :hash},
      compilation: %{enabled: true, optimisation_level: :aggressive},
      memory: %{enabled: true, cache_optimisation: true},
      execution: %{enabled: true, parallel_rules: true}
    }
  end
  
  defp load_production_rules() do
    # Load rules based on environment configuration
    Application.get_env(:my_app, :rule_modules, [])
    |> Enum.flat_map(& &1.create_rules())
  end
end
```

## Architecture Overview

Presto implements the **RETE algorithm** with a simplified, integrated architecture optimised for Elixir:

```mermaid
flowchart TB
    subgraph "Input Layer"
        Facts["📥 Facts<br/>(Your Data)"]
    end
    
    subgraph "RETE Network"
        Alpha["🔍 Alpha Network<br/>(Pattern Filtering)"]
        Beta["🔗 Beta Network<br/>(Fact Joining)"]
    end
    
    subgraph "Execution Layer"
        Actions["⚡ Rule Actions<br/>(Business Processing)"]
    end
    
    subgraph "Output Layer"
        Results["📤 Results<br/>(New Facts)"]
    end
    
    subgraph "Memory Management"
        WM[("💾 Working Memory<br/>(ETS Tables)")]
        AM[("🗃️ Alpha Memory<br/>(Pattern Cache)")]
        BM[("🔗 Beta Memory<br/>(Join Cache)")]
    end
    
    Facts --> Alpha
    Alpha --> Beta
    Beta --> Actions
    Actions --> Results
    
    Facts -.-> WM
    Alpha -.-> AM
    Beta -.-> BM
    Actions -.-> WM
    
    style Facts fill:#e3f2fd
    style Alpha fill:#f3e5f5
    style Beta fill:#e8f5e8
    style Actions fill:#fff3e0
    style Results fill:#e0f2f1
    style WM fill:#f5f5f5
    style AM fill:#f5f5f5
    style BM fill:#f5f5f5
```

### Core Components

**Engine (GenServer)**: Central coordinator managing:
- Network state and rule management
- Fact lifecycle and working memory (ETS)
- Alpha/Beta memory management (integrated)
- Rule execution coordination
- Memory cleanup and optimisation

**Alpha Network**: Pattern matching using Elixir's native pattern matching + ETS storage

**Beta Network**: Join operations with ETS-based partial match storage

**Task Supervision**: Parallel rule execution with fault isolation

### Memory Architecture

```elixir
# Working Memory - all facts
:working_memory          # ETS table: {fact_id, fact}

# Alpha Memories - facts by pattern  
:alpha_memory_1          # ETS table: {pattern_id, fact}

# Beta Memories - partial matches
:beta_memory_1           # ETS table: {token_id, token}

# All managed by single Engine process for consistency
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

Presto includes comprehensive production-ready examples that demonstrate the full power of the RETE engine:

### Core Examples

- **`Presto.Examples.PayrollRules`** - Time calculation, overtime processing with RETE engine
- **`Presto.Examples.ComplianceRules`** - Weekly hour compliance, violation detection and reporting
- **`Presto.Examples.CaliforniaSpikeBreakRules`** - Multi-jurisdictional break rules with complex industry requirements
- **`Presto.Examples.OvertimeRules`** - Advanced overtime calculation with configurable rule ordering
- **`Presto.Examples.TroncRules`** - TRONC (Tips, Gratuities & Service Charges) distribution system

### RETE Engine Integration

All examples now showcase the actual Presto RETE engine with:

```elixir
# Each example provides both RETE engine and direct processing approaches

# RETE Engine Processing (Recommended)
result = Presto.Examples.PayrollRules.process_with_engine(time_entries, rule_spec)

# Direct Processing (For comparison/migration)
result = Presto.Examples.PayrollRules.process_time_entries(time_entries, rule_spec)
```

### Advanced Features Demonstrated

**Multi-Stage RETE Workflows:**
```elixir
# Compliance Rules Example - 3-stage RETE processing
result = Presto.Examples.ComplianceRules.run_example()
# 1. Time processing rule (calculate durations)
# 2. Weekly aggregation rule (group by employee/week) 
# 3. Compliance checking rule (detect violations)
```

**Multi-Jurisdictional Processing:**
```elixir
# California Spike Break Rules - Industry-specific processing
Presto.Examples.CaliforniaSpikeBreakRules.run_multi_jurisdiction_example()
# 1. Work session analysis rule
# 2. Spike break detection rule (jurisdiction-aware)
# 3. Compliance checking rule
# 4. Penalty calculation rule
```

**Complex Business Logic:**
```elixir
# TRONC Distribution - Multi-factor allocation rules  
result = Presto.Examples.TroncRules.run_custom_allocation_example()
# 1. Pool collection rule (aggregate tips/service charges)
# 2. Admin deduction rule (apply cost deductions) 
# 3. Role allocation rule (distribute by weighted hours)
# 4. Staff distribution rule (individual payments)
```

### Rule Ordering and Priority

The examples demonstrate sophisticated rule execution control:

```elixir
# Overtime Rules - Configurable execution order
rule_spec = Presto.Examples.OvertimeRules.generate_example_rule_spec()
result = Presto.Examples.OvertimeRules.run_custom_order_example()

# Custom rule ordering with priorities
%{
  "rule_execution_order" => ["time_calculation", "pay_aggregation", "overtime_processing"],
  "overtime_rules" => [
    %{"name" => "overtime_basic_priority_1", "priority" => 1, "threshold" => 15},
    %{"name" => "overtime_special_priority_2", "priority" => 2, "threshold" => 15},
    %{"name" => "overtime_general_priority_3", "priority" => 3, "threshold" => 5}
  ]
}
```

### Key RETE Engine Features Showcased

- **Incremental Processing**: Facts trigger rules as they're asserted
- **Pattern Matching**: Sophisticated condition matching on fact structures
- **Working Memory**: Efficient fact storage and retrieval
- **Rule Priorities**: Control execution order within the RETE network
- **Concurrent Execution**: Parallel rule firing with `concurrent: true`
- **Complex Joins**: Multi-fact pattern matching across different data types

### Example Usage Patterns

```elixir
# Start RETE engine and process facts
{:ok, engine} = Presto.start_engine()

# Add rules to engine
rules = Presto.Examples.ComplianceRules.create_rules(rule_spec)
Enum.each(rules, &Presto.add_rule(engine, &1))

# Assert facts into working memory
Enum.each(time_entries, fn {:time_entry, id, data} ->
  Presto.assert_fact(engine, {:time_entry, id, data})
end)

# Fire rules and get results
results = Presto.fire_rules(engine, concurrent: true)
Presto.stop_engine(engine)
```

Study these examples in `lib/presto/examples/` for RETE engine patterns and best practices.

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

## 📦 Publishing & Releases

This project uses automated publishing to Hex.pm with GitHub Actions:

- **Automated Publishing**: Push a version tag (e.g., `v0.2.0`) to trigger automatic publishing
- **Release Preparation**: Use the "Prepare Release" GitHub Action to update versions and changelog
- **Quality Gates**: All tests, formatting, and code quality checks must pass before publishing

See [docs/PUBLISHING.md](docs/PUBLISHING.md) for detailed setup and usage instructions.

### Quick Release Process

1. Run "Prepare Release" workflow in GitHub Actions
2. Review and merge the generated PR
3. Create and push version tag: `git tag v0.2.0 && git push origin v0.2.0`
4. Automated publishing handles the rest! 🚀