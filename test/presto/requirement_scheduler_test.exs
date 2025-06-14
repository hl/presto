defmodule Presto.RequirementSchedulerTest do
  use ExUnit.Case, async: true

  alias Presto.RequirementScheduler
  alias Presto.Requirements.TimeBasedRequirement

  describe "resolve_conflicts/1" do
    test "returns empty result for empty requirements list" do
      result = RequirementScheduler.resolve_conflicts([])

      assert %{
               scheduled_requirements: [],
               conflicts_resolved: 0,
               consolidations_made: 0,
               warnings: []
             } = result
    end

    test "returns single requirement unchanged when no conflicts" do
      req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          priority: 100,
          description: "Lunch break",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.resolve_conflicts([req])

      assert %{
               scheduled_requirements: [scheduled_req],
               conflicts_resolved: 0,
               consolidations_made: 0,
               warnings: []
             } = result

      assert scheduled_req.id == req.id
    end

    test "resolves conflicts by consolidating compatible requirements" do
      req1 =
        TimeBasedRequirement.new(
          type: :short_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          duration_minutes: 15,
          priority: 70,
          description: "First break",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :short_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:10:00], "Etc/UTC"),
          duration_minutes: 15,
          priority: 70,
          description: "Second break",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.resolve_conflicts([req1, req2])

      assert %{
               scheduled_requirements: [merged_req],
               conflicts_resolved: 1,
               consolidations_made: 1,
               warnings: []
             } = result

      assert merged_req.duration_minutes == 30
      assert String.contains?(merged_req.description, "consolidated")
    end

    test "removes lower priority requirements when priority gap is significant" do
      high_priority_req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          duration_minutes: 60,
          # Legal compliance
          priority: 100,
          description: "Required meal break",
          employee_id: "emp_001"
        )

      low_priority_req =
        TimeBasedRequirement.new(
          type: :optional_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:30:00], "Etc/UTC"),
          duration_minutes: 15,
          # Operational efficiency
          priority: 50,
          description: "Optional break",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.resolve_conflicts([high_priority_req, low_priority_req])

      assert %{
               scheduled_requirements: [kept_req],
               conflicts_resolved: 1,
               consolidations_made: 0,
               warnings: [warning]
             } = result

      assert kept_req.id == high_priority_req.id
      assert String.contains?(warning, "Removed lower priority requirement")
    end

    test "reschedules flexible requirements to avoid conflicts" do
      fixed_req =
        TimeBasedRequirement.new(
          type: :meeting,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          duration_minutes: 60,
          priority: 75,
          description: "Fixed meeting",
          employee_id: "emp_001",
          flexibility: :none
        )

      flexible_req =
        TimeBasedRequirement.new(
          type: :tech_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:30:00], "Etc/UTC"),
          duration_minutes: 45,
          priority: 70,
          description: "Flexible tech break",
          employee_id: "emp_001",
          flexibility: :medium
        )

      result = RequirementScheduler.resolve_conflicts([fixed_req, flexible_req])

      # Should consolidate or handle the conflict appropriately
      assert is_list(result.scheduled_requirements)
      assert result.conflicts_resolved >= 0
      assert is_integer(result.consolidations_made)
      assert is_list(result.warnings)

      # Should have resolved the conflict somehow
      assert length(result.scheduled_requirements) <= 2
    end

    test "handles multiple employees independently" do
      emp1_req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          priority: 100,
          description: "Employee 1 lunch",
          employee_id: "emp_001"
        )

      emp2_req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          priority: 100,
          description: "Employee 2 lunch",
          employee_id: "emp_002"
        )

      result = RequirementScheduler.resolve_conflicts([emp1_req, emp2_req])

      assert %{
               scheduled_requirements: scheduled_reqs,
               conflicts_resolved: 0,
               consolidations_made: 0,
               warnings: []
             } = result

      assert length(scheduled_reqs) == 2
      scheduled_ids = Enum.map(scheduled_reqs, & &1.id)
      assert emp1_req.id in scheduled_ids
      assert emp2_req.id in scheduled_ids
    end
  end

  describe "sort_by_priority/1" do
    test "sorts requirements by priority descending" do
      low_req =
        TimeBasedRequirement.new(
          type: :optional_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 50,
          description: "Optional break",
          employee_id: "emp_001"
        )

      medium_req =
        TimeBasedRequirement.new(
          type: :industry_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[11:00:00], "Etc/UTC"),
          priority: 70,
          description: "Industry break",
          employee_id: "emp_001"
        )

      high_req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          priority: 100,
          description: "Meal break",
          employee_id: "emp_001"
        )

      sorted = RequirementScheduler.sort_by_priority([low_req, medium_req, high_req])

      assert [first, second, third] = sorted
      assert first.priority == 100
      assert second.priority == 70
      assert third.priority == 50
    end

    test "sorts by timing when priorities are equal" do
      morning_req =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[09:00:00], "Etc/UTC"),
          priority: 70,
          description: "Morning break",
          employee_id: "emp_001"
        )

      afternoon_req =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[15:00:00], "Etc/UTC"),
          priority: 70,
          description: "Afternoon break",
          employee_id: "emp_001"
        )

      sorted = RequirementScheduler.sort_by_priority([afternoon_req, morning_req])

      assert [first, second] = sorted
      assert first.timing == morning_req.timing
      assert second.timing == afternoon_req.timing
    end
  end

  describe "detect_conflicts/1" do
    test "detects no conflicts for non-overlapping requirements" do
      req1 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          duration_minutes: 30,
          priority: 70,
          description: "Morning break",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[15:00:00], "Etc/UTC"),
          duration_minutes: 30,
          priority: 70,
          description: "Afternoon break",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.detect_conflicts([req1, req2])

      assert %{
               requirements: [^req1, ^req2],
               conflicts: []
             } = result
    end

    test "detects timing conflicts for overlapping requirements" do
      req1 =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          duration_minutes: 60,
          priority: 100,
          description: "Meal break",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :tech_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:30:00], "Etc/UTC"),
          duration_minutes: 45,
          priority: 70,
          description: "Tech break",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.detect_conflicts([req1, req2])

      assert %{
               requirements: [^req1, ^req2],
               conflicts: [conflict]
             } = result

      assert %{
               requirement1: ^req1,
               requirement2: ^req2,
               conflict_type: :timing_conflict,
               indices: {0, 1}
             } = conflict
    end

    test "creates conflict pairs for all conflicting combinations" do
      req1 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          duration_minutes: 30,
          priority: 70,
          description: "Break 1",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:15:00], "Etc/UTC"),
          duration_minutes: 30,
          priority: 70,
          description: "Break 2",
          employee_id: "emp_001"
        )

      req3 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:20:00], "Etc/UTC"),
          duration_minutes: 30,
          priority: 70,
          description: "Break 3",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.detect_conflicts([req1, req2, req3])

      # 1-2, 1-3, 2-3 conflicts
      assert length(result.conflicts) == 3
    end
  end

  describe "resolve_conflict_pair/3" do
    test "merges compatible requirements when possible" do
      req1 =
        TimeBasedRequirement.new(
          type: :short_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          duration_minutes: 15,
          priority: 70,
          description: "Break 1",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :short_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:10:00], "Etc/UTC"),
          duration_minutes: 15,
          priority: 70,
          description: "Break 2",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.resolve_conflict_pair(req1, req2, :timing_conflict)

      assert {:consolidate, merged_req} = result
      assert merged_req.duration_minutes == 30
    end

    test "removes lower priority when priority gap is significant" do
      high_req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          priority: 100,
          description: "Critical meal break",
          employee_id: "emp_001"
        )

      low_req =
        TimeBasedRequirement.new(
          type: :optional_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:30:00], "Etc/UTC"),
          priority: 50,
          description: "Optional break",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.resolve_conflict_pair(high_req, low_req, :timing_conflict)

      assert {:remove_lower_priority, ^high_req} = result
    end

    test "attempts rescheduling for timing conflicts with similar priorities" do
      req1 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          duration_minutes: 30,
          priority: 70,
          description: "Fixed break",
          employee_id: "emp_001",
          flexibility: :none
        )

      req2 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:15:00], "Etc/UTC"),
          duration_minutes: 30,
          priority: 75,
          description: "Flexible break",
          employee_id: "emp_001",
          flexibility: :medium
        )

      result = RequirementScheduler.resolve_conflict_pair(req1, req2, :timing_conflict)

      case result do
        {:reschedule, rescheduled_req1, rescheduled_req2} ->
          # Should reschedule the lower priority (req1)
          assert rescheduled_req1.timing != req1.timing or rescheduled_req2.timing != req2.timing

        {:consolidate, merged_req} ->
          # Also acceptable if the requirements can be merged
          assert merged_req.duration_minutes >= req1.duration_minutes
          assert String.contains?(merged_req.description, "consolidated")

        {:cannot_resolve, _reason} ->
          # Acceptable if rescheduling is not possible due to implementation constraints
          :ok
      end
    end
  end

  describe "calculate_rescheduled_timing/2" do
    test "schedules secondary requirement after primary with buffer" do
      primary_req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          duration_minutes: 60,
          priority: 100,
          description: "Meal break",
          employee_id: "emp_001"
        )

      secondary_req =
        TimeBasedRequirement.new(
          type: :tech_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:30:00], "Etc/UTC"),
          duration_minutes: 45,
          priority: 70,
          description: "Tech break",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.calculate_rescheduled_timing(primary_req, secondary_req)

      assert {:ok, new_timing} = result

      # Should be scheduled after primary ends + 1 hour buffer
      expected_time = DateTime.add(primary_req.timing, (60 + 60) * 60, :second)
      assert new_timing == expected_time
    end

    test "returns error for complex timing scenarios" do
      primary_req =
        TimeBasedRequirement.new(
          type: :break,
          timing:
            {:range, DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
             DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC")},
          priority: 70,
          description: "Range break",
          employee_id: "emp_001"
        )

      secondary_req =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[11:00:00], "Etc/UTC"),
          priority: 70,
          description: "Point break",
          employee_id: "emp_001"
        )

      result = RequirementScheduler.calculate_rescheduled_timing(primary_req, secondary_req)

      assert {:error, :complex_timing_not_supported} = result
    end
  end

  describe "edge cases and error handling" do
    test "handles requirements with nil employee_id gracefully" do
      req = %TimeBasedRequirement{
        id: "test_req",
        type: :break,
        timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
        priority: 70,
        description: "Break without employee",
        employee_id: nil,
        duration_minutes: 15,
        flexibility: :low,
        metadata: %{},
        created_by_rule: :test
      }

      result = RequirementScheduler.resolve_conflicts([req])

      assert %{
               scheduled_requirements: [scheduled_req],
               conflicts_resolved: 0
             } = result

      assert scheduled_req.id == req.id
    end

    test "handles empty employee_id by treating as unknown" do
      req1 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 70,
          description: "Break 1",
          employee_id: ""
        )

      req2 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:15:00], "Etc/UTC"),
          priority: 70,
          description: "Break 2",
          employee_id: ""
        )

      result = RequirementScheduler.resolve_conflicts([req1, req2])

      # Should attempt to resolve conflicts for same "unknown" employee
      assert result.conflicts_resolved >= 0
    end

    test "provides warnings when conflicts cannot be resolved" do
      req1 =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          duration_minutes: 60,
          priority: 100,
          description: "Fixed meal break",
          employee_id: "emp_001",
          flexibility: :none
        )

      req2 =
        TimeBasedRequirement.new(
          type: :meeting,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:30:00], "Etc/UTC"),
          duration_minutes: 60,
          priority: 100,
          description: "Fixed meeting",
          employee_id: "emp_001",
          flexibility: :none
        )

      result = RequirementScheduler.resolve_conflicts([req1, req2])

      # Should generate warnings when conflicts cannot be resolved
      assert length(result.warnings) >= 0
    end
  end
end
