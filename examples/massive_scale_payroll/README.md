# Massive Scale Payroll Processing Example

This example demonstrates how to use Presto as a generic rules engine to build a massive scale payroll processing system capable of handling:

- **10,000 employees** with 50 shifts per week each
- **2.15 million shifts** per month total
- **2,000 complex payroll rules** (1,000 compiled + 1,000 runtime)
- **Complex time-based calculations** with variable pay rates within shifts
- **Overtime rules** that sum across shifts and mark segments as paid
- **Parallel processing** with multiple Presto rule engines

## Architecture Overview

```mermaid
graph TB
    subgraph "Massive Scale Payroll System"
        PC[PayrollCoordinator<br/>Orchestration & Batching]
        SSP[ShiftSegmentProcessor<br/>Time-based Segmentation]
        PM[PerformanceMonitor<br/>Real-time Metrics]
        OA[OvertimeAggregator<br/>Cross-shift Processing]
        
        subgraph "Worker Pool (8-16 Workers)"
            EW1[EmployeeWorker 1<br/>+ Presto Engine]
            EW2[EmployeeWorker 2<br/>+ Presto Engine]
            EW3[EmployeeWorker 3<br/>+ Presto Engine]
            EWN[EmployeeWorker N<br/>+ Presto Engine]
        end
        
        subgraph "Presto Rule Engines"
            PE1[Presto Engine 1<br/>2,000 Rules]
            PE2[Presto Engine 2<br/>2,000 Rules]
            PE3[Presto Engine 3<br/>2,000 Rules]
            PEN[Presto Engine N<br/>2,000 Rules]
        end
    end
    
    PC --> SSP
    PC --> EW1
    PC --> EW2
    PC --> EW3
    PC --> EWN
    
    EW1 --> PE1
    EW2 --> PE2
    EW3 --> PE3
    EWN --> PEN
    
    EW1 --> OA
    EW2 --> OA
    EW3 --> OA
    EWN --> OA
    
    PM -.-> PC
    PM -.-> EW1
    PM -.-> EW2
    PM -.-> EW3
    PM -.-> EWN
    
    classDef coordinator fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef worker fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef presto fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    classDef monitor fill:#fff3e0,stroke:#e65100,stroke-width:2px
    
    class PC coordinator
    class EW1,EW2,EW3,EWN worker
    class PE1,PE2,PE3,PEN presto
    class PM,SSP,OA monitor
```

## Key Features

### Presto Integration
- **Generic Rules Engine**: Presto handles all pattern matching and rule execution
- **2,000 Rules**: Organized by category and priority, loaded into Presto engines
- **Parallel Processing**: Multiple Presto instances for concurrent processing
- **Domain-Specific Logic**: Payroll rules implemented using Presto's rule DSL

### Complex Time-Based Processing
- **Shift Segmentation**: Christmas Eve → Christmas Day scenarios with different pay rates
- **Variable Pay Rates**: Different rates for holidays, weekends, night shifts
- **Overtime Calculations**: Weekly/daily overtime with segment payment marking
- **Time Period Boundaries**: Automatic detection and segmentation

### Scalability Features
- **Batch Processing**: Configurable batch sizes for memory management
- **Parallel Workers**: 8-16 concurrent workers with dedicated Presto engines
- **Memory Management**: Controlled memory usage with streaming processing
- **Performance Monitoring**: Real-time bottleneck detection and optimization

```mermaid
graph LR
    subgraph "Data Flow Pipeline"
        subgraph "Input Layer"
            TS[Time Sheets<br/>10K employees<br/>2.15M shifts/month]
        end
        
        subgraph "Processing Layer"
            PC[PayrollCoordinator<br/>Batch Management]
            
            subgraph "Parallel Processing"
                B1[Batch 1<br/>125 employees]
                B2[Batch 2<br/>125 employees]
                B3[Batch 3<br/>125 employees]
                BN[Batch N<br/>125 employees]
            end
            
            subgraph "Worker Pool"
                W1[Worker 1 + Presto]
                W2[Worker 2 + Presto]
                W3[Worker 3 + Presto]
                WN[Worker N + Presto]
            end
        end
        
        subgraph "Output Layer"
            OA[OvertimeAggregator<br/>Cross-shift Analysis]
            PR[Payroll Results<br/>Detailed Calculations]
        end
    end
    
    TS --> PC
    PC --> B1
    PC --> B2
    PC --> B3
    PC --> BN
    
    B1 --> W1
    B2 --> W2
    B3 --> W3
    BN --> WN
    
    W1 --> OA
    W2 --> OA
    W3 --> OA
    WN --> OA
    
    OA --> PR
    
    classDef input fill:#e3f2fd,stroke:#0277bd,stroke-width:2px
    classDef batch fill:#f1f8e9,stroke:#33691e,stroke-width:2px
    classDef worker fill:#fce4ec,stroke:#ad1457,stroke-width:2px
    classDef output fill:#fff8e1,stroke:#ff8f00,stroke-width:2px
    
    class TS input
    class PC,B1,B2,B3,BN batch
    class W1,W2,W3,WN worker
    class OA,PR output
```

## Files

- `massive_payroll_demo.exs` - Main demo script (run this!)
- `scalable_payroll_system.ex` - Primary API and orchestration
- `payroll_coordinator.ex` - Batch processing and worker coordination
- `shift_segment_processor.ex` - Complex time-based shift segmentation
- `employee_worker.ex` - Individual worker with dedicated Presto engine
- `overtime_aggregator.ex` - Cross-shift overtime and payment marking
- `massive_payroll_rules.ex` - 2,000 payroll rules for Presto
- `performance_monitor.ex` - Real-time monitoring and bottleneck analysis

## Running the Demo

```bash
# From the presto project root
elixir examples/massive_scale_payroll/massive_payroll_demo.exs
```

## Demo Output

The demo will show:

1. **System Initialization**: Multiple Presto engines starting up
2. **Christmas Shift Example**: Complex time-based segmentation demonstration  
3. **Massive Dataset Generation**: 1,000 employees with 20,000 total shifts
4. **Parallel Processing**: Real-time progress via multiple Presto engines
5. **Comprehensive Results**: Overtime analysis, performance metrics, bottlenecks
6. **Presto Integration Stats**: Rule execution statistics and engine performance

## Example Output

```
=== Massive Scale Payroll Processing Demo ===
Demo Configuration:
  Employees: 1,000 (scaled down from 10,000)
  Shifts per employee: 20 (scaled down from 200+ per month)
  Total shifts: 20,000
  Payroll rules: 2,000 (via Presto rule engines)
  Parallel workers: 8 (each with dedicated Presto engine)

✓ Massive Scale Payroll System started
  - Multiple Presto rule engines initialized
  - PayrollCoordinator ready for batch processing

--- Christmas Eve → Christmas Day Shift Segmentation ---
Segments created:
  emp_001_shift_christmas_seg_1:
    Time: 2024-12-24 22:00:00Z → 2024-12-25 00:00:00Z
    Duration: 120 minutes
    Pay period: christmas_eve
    Rate multiplier: 1.5x

  emp_001_shift_christmas_seg_2:
    Time: 2024-12-25 00:00:00Z → 2024-12-25 06:00:00Z
    Duration: 360 minutes
    Pay period: christmas_day
    Rate multiplier: 2.0x

✓ Massive payroll processing completed in 2,847ms

--- Presto Rules Engine Integration ---
Presto Integration Statistics:
  Total Presto engines used: 8
  Total rules loaded: 2,000
  Rules executed successfully: true

PRESTO ARCHITECTURE HIGHLIGHTS:
  ✓ Presto serves as a generic, reusable rules engine
  ✓ Each worker has its own dedicated Presto engine instance
  ✓ 2,000 rules loaded and executed via Presto's RETE network
  ✓ Domain-specific logic in rules, not hardcoded in application
```

## Scaling Patterns

```mermaid
graph TB
    subgraph "Horizontal Scaling Architecture"
        subgraph "Small Scale (1K employees)"
            S1[2-4 Workers]
            S2[2-4 Presto Engines]
            S3[Batch Size: 250]
        end
        
        subgraph "Medium Scale (5K employees)"
            M1[4-8 Workers]
            M2[4-8 Presto Engines]
            M3[Batch Size: 125]
        end
        
        subgraph "Enterprise Scale (10K+ employees)"
            E1[8-16 Workers]
            E2[8-16 Presto Engines]
            E3[Batch Size: 125]
        end
        
        subgraph "Performance Characteristics"
            P1[Linear Throughput Scaling]
            P2[Constant Memory Usage]
            P3[Independent Engine Performance]
        end
    end
    
    S1 --> M1
    M1 --> E1
    S2 --> M2
    M2 --> E2
    S3 --> M3
    M3 --> E3
    
    E1 --> P1
    E2 --> P2
    E3 --> P3
    
    classDef small fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef medium fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef enterprise fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    classDef performance fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
    
    class S1,S2,S3 small
    class M1,M2,M3 medium
    class E1,E2,E3 enterprise
    class P1,P2,P3 performance
```

## Christmas Eve → Christmas Day Example

The system automatically handles complex scenarios like shifts that span across different pay periods:

```elixir
# Original shift
{:time_entry, "emp_001_shift_christmas", %{
  start_datetime: ~U[2024-12-24 22:00:00Z],  # Christmas Eve 10 PM
  finish_datetime: ~U[2024-12-25 06:00:00Z], # Christmas Day 6 AM
}}

# Automatically becomes two segments
{:shift_segment, "emp_001_shift_christmas_seg_1", %{
  start_datetime: ~U[2024-12-24 22:00:00Z],
  finish_datetime: ~U[2024-12-25 00:00:00Z],  # Midnight boundary
  pay_period: :christmas_eve,
  pay_rate_multiplier: 1.5
}}

{:shift_segment, "emp_001_shift_christmas_seg_2", %{
  start_datetime: ~U[2024-12-25 00:00:00Z],  # Midnight boundary
  finish_datetime: ~U[2024-12-25 06:00:00Z],
  pay_period: :christmas_day,
  pay_rate_multiplier: 2.0
}}
```

## Presto Rule Examples

The system uses 2,000 rules implemented in Presto's rule DSL:

```elixir
# Segmentation rule
rule "basic_shift_segmentation", %{priority: 120} do
  when: {:time_entry, entry_id, %{start_datetime: start_dt, finish_datetime: finish_dt} = data}
        and not Map.has_key?(data, :segmented)
  then: 
    segments = ShiftSegmentProcessor.segment_single_shift(data)
    Enum.each(segments, fn segment ->
      assert {:shift_segment, segment.segment_id, segment}
    end)
    retract {:time_entry, entry_id, data}
end

# Pay calculation rule
rule "calculate_base_pay", %{priority: 97} do
  when: {:shift_segment, segment_id, %{units: units, pay_rate_multiplier: multiplier} = data}
        and {:employee_info, employee_id, %{base_hourly_rate: base_rate}}
        and data.employee_id == employee_id
  then:
    base_pay = Float.round(units * base_rate * multiplier, 2)
    assert {:shift_segment, segment_id, Map.put(data, :base_pay_amount, base_pay)}
end

# Overtime eligibility rule
rule "weekly_overtime_eligibility", %{priority: 59} do
  when: {:shift_segment, segment_id, %{employee_id: emp_id, units: units} = data}
        and weekly_hours_for_employee(emp_id) > 40.0
  then:
    assert {:overtime_eligible, segment_id, %{
      employee_id: emp_id,
      overtime_type: :weekly,
      hours: units
    }}
end
```

## Performance Characteristics

**Expected Performance (full 10,000 employees):**
- Processing time: ~20-25 minutes
- Memory usage: <4GB peak
- Throughput: ~400-500 employees/minute
- Rule execution rate: ~1,000+ rules/second per engine

**Scalability:**
- Linear scaling with additional workers
- Memory usage controlled by batch processing
- Independent Presto engines prevent cross-worker interference
- Real-time bottleneck detection for optimization

```mermaid
flowchart TD
    subgraph "Performance Monitoring System"
        subgraph "Real-time Metrics Collection"
            PM[PerformanceMonitor]
            
            subgraph "Worker Metrics"
                WM1[Worker 1 Metrics<br/>Processing Rate<br/>Memory Usage<br/>Queue Depth]
                WM2[Worker 2 Metrics<br/>Processing Rate<br/>Memory Usage<br/>Queue Depth]
                WMN[Worker N Metrics<br/>Processing Rate<br/>Memory Usage<br/>Queue Depth]
            end
            
            subgraph "Presto Engine Metrics"
                PEM1[Engine 1 Stats<br/>Rules Executed<br/>Execution Time<br/>Memory Usage]
                PEM2[Engine 2 Stats<br/>Rules Executed<br/>Execution Time<br/>Memory Usage]
                PEMN[Engine N Stats<br/>Rules Executed<br/>Execution Time<br/>Memory Usage]
            end
            
            subgraph "System Metrics"
                SM[Overall Throughput<br/>Total Memory Usage<br/>Processing Time<br/>Bottleneck Detection]
            end
        end
        
        subgraph "Performance Analysis"
            BA[Bottleneck Analysis]
            OPT[Optimization Recommendations]
            ALERT[Performance Alerts]
        end
        
        subgraph "Adaptive Scaling"
            AS[Auto-scaling Decisions]
            BC[Batch Size Adjustment]
            WA[Worker Allocation]
        end
    end
    
    WM1 --> PM
    WM2 --> PM
    WMN --> PM
    
    PEM1 --> PM
    PEM2 --> PM
    PEMN --> PM
    
    PM --> SM
    SM --> BA
    BA --> OPT
    BA --> ALERT
    
    OPT --> AS
    AS --> BC
    AS --> WA
    
    classDef metrics fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef analysis fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef scaling fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    classDef monitor fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
    
    class WM1,WM2,WMN,PEM1,PEM2,PEMN metrics
    class BA,OPT,ALERT analysis
    class AS,BC,WA scaling
    class PM,SM monitor
```

## Design Principles

1. **Presto as Generic Engine**: All business logic implemented as Presto rules
2. **Domain-Specific Coordination**: Payroll system handles orchestration and data flow
3. **Parallel Processing**: Multiple independent Presto engines for concurrency
4. **Memory Management**: Batch processing prevents memory exhaustion
5. **Real-time Monitoring**: Performance tracking and bottleneck identification
6. **Testable Architecture**: Each component can be tested independently

This example demonstrates how Presto can serve as the foundation for massive scale, enterprise-grade applications while maintaining clean separation between generic rule processing and domain-specific business logic.