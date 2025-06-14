defmodule Presto.Requirements.TimeBasedRequirementTest do
  use ExUnit.Case, async: true

  alias Presto.Requirements.TimeBasedRequirement

  describe "new/1" do
    test "creates a new time-based requirement with required fields" do
      timing = DateTime.new!(~D[2024-01-15], ~T[14:00:00], "Etc/UTC")

      req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: timing,
          priority: 100,
          description: "30-minute meal break",
          employee_id: "emp_001"
        )

      assert %TimeBasedRequirement{
               type: :meal_break,
               timing: ^timing,
               duration_minutes: 15,
               priority: 100,
               description: "30-minute meal break",
               employee_id: "emp_001",
               flexibility: :low,
               metadata: %{},
               created_by_rule: :unknown
             } = req
    end

    test "creates requirement with custom duration and flexibility" do
      timing = DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC")

      req =
        TimeBasedRequirement.new(
          type: :tech_break,
          timing: timing,
          duration_minutes: 45,
          priority: 70,
          description: "Tech crunch break",
          employee_id: "emp_002",
          flexibility: :medium,
          metadata: %{crunch_level: :high},
          created_by_rule: :bay_area_tech_crunch
        )

      assert req.duration_minutes == 45
      assert req.flexibility == :medium
      assert req.metadata == %{crunch_level: :high}
      assert req.created_by_rule == :bay_area_tech_crunch
    end

    test "generates unique IDs for each requirement" do
      timing = DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC")

      req1 =
        TimeBasedRequirement.new(
          type: :break,
          timing: timing,
          priority: 50,
          description: "Break 1",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :break,
          timing: timing,
          priority: 50,
          description: "Break 2",
          employee_id: "emp_001"
        )

      assert req1.id != req2.id
      assert String.starts_with?(req1.id, "req_")
      assert String.starts_with?(req2.id, "req_")
    end
  end

  describe "priority/1" do
    test "returns the priority of the requirement" do
      req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          priority: 100,
          description: "Critical meal break",
          employee_id: "emp_001"
        )

      assert TimeBasedRequirement.priority(req) == 100
    end
  end

  describe "type/1" do
    test "returns the type of the requirement" do
      req =
        TimeBasedRequirement.new(
          type: :consecutive_work_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 90,
          description: "Consecutive work break",
          employee_id: "emp_001"
        )

      assert TimeBasedRequirement.type(req) == :consecutive_work_break
    end
  end

  describe "conflicts_with?/2" do
    test "returns no conflict for different employees" do
      timing = DateTime.new!(~D[2024-01-15], ~T[14:00:00], "Etc/UTC")

      req1 =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: timing,
          priority: 100,
          description: "Meal break",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: timing,
          priority: 100,
          description: "Meal break",
          employee_id: "emp_002"
        )

      assert TimeBasedRequirement.conflicts_with?(req1, req2) == :no_conflict
    end

    test "returns no conflict for non-overlapping times" do
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

      assert TimeBasedRequirement.conflicts_with?(req1, req2) == :no_conflict
    end

    test "returns timing conflict for overlapping times" do
      req1 =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          duration_minutes: 60,
          priority: 100,
          description: "Lunch break",
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

      assert TimeBasedRequirement.conflicts_with?(req1, req2) == :timing_conflict
    end

    test "returns no conflict for non-TimeBasedRequirement" do
      req =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 70,
          description: "Break",
          employee_id: "emp_001"
        )

      other_requirement = %{some: :other_type}

      assert TimeBasedRequirement.conflicts_with?(req, other_requirement) == :no_conflict
    end
  end

  describe "merge_with/2" do
    test "merges compatible requirements of same type and close timing" do
      req1 =
        TimeBasedRequirement.new(
          type: :short_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          duration_minutes: 15,
          priority: 70,
          description: "Morning break",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :short_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:15:00], "Etc/UTC"),
          duration_minutes: 15,
          priority: 70,
          description: "Extended break",
          employee_id: "emp_001"
        )

      {:ok, merged} = TimeBasedRequirement.merge_with(req1, req2)

      assert merged.duration_minutes == 30
      assert merged.timing == req1.timing
      assert merged.priority == 70
      assert String.contains?(merged.description, "consolidated")
      assert merged.metadata.consolidated_from == [req1.id, req2.id]
    end

    test "consolidates when higher priority requirement can satisfy lower priority" do
      req1 =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          duration_minutes: 60,
          priority: 85,
          description: "Meal break",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :short_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:30:00], "Etc/UTC"),
          duration_minutes: 15,
          priority: 70,
          description: "Short break",
          employee_id: "emp_001"
        )

      {:ok, consolidated} = TimeBasedRequirement.merge_with(req1, req2)

      assert consolidated.id == req1.id
      assert consolidated.duration_minutes == 60
      assert consolidated.priority == 85
      assert String.contains?(consolidated.description, "satisfies")
      assert consolidated.metadata.satisfies_requirements == [req2.id]
    end

    test "returns error for different employees" do
      req1 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 70,
          description: "Break",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 70,
          description: "Break",
          employee_id: "emp_002"
        )

      assert TimeBasedRequirement.merge_with(req1, req2) == {:error, :incompatible}
    end

    test "returns error for incompatible timing" do
      req1 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 70,
          description: "Morning break",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[15:00:00], "Etc/UTC"),
          priority: 70,
          description: "Afternoon break",
          employee_id: "emp_001"
        )

      assert TimeBasedRequirement.merge_with(req1, req2) == {:error, :incompatible}
    end

    test "returns error for non-TimeBasedRequirement" do
      req =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 70,
          description: "Break",
          employee_id: "emp_001"
        )

      other_requirement = %{some: :other_type}

      assert TimeBasedRequirement.merge_with(req, other_requirement) == {:error, :incompatible}
    end
  end

  describe "reschedule/2" do
    test "reschedules flexible requirement to new timing" do
      req =
        TimeBasedRequirement.new(
          type: :break,
          timing: DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
          priority: 70,
          description: "Flexible break",
          employee_id: "emp_001",
          flexibility: :high
        )

      new_timing = DateTime.new!(~D[2024-01-15], ~T[14:00:00], "Etc/UTC")

      {:ok, rescheduled} = TimeBasedRequirement.reschedule(req, new_timing)

      assert rescheduled.timing == new_timing
      assert rescheduled.type == req.type
      assert rescheduled.priority == req.priority
    end

    test "refuses to reschedule inflexible requirement" do
      req =
        TimeBasedRequirement.new(
          type: :meal_break,
          timing: DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC"),
          priority: 100,
          description: "Fixed meal break",
          employee_id: "emp_001",
          flexibility: :none
        )

      new_timing = DateTime.new!(~D[2024-01-15], ~T[13:00:00], "Etc/UTC")

      assert TimeBasedRequirement.reschedule(req, new_timing) == {:error, :cannot_reschedule}
    end
  end

  describe "range timing support" do
    test "creates requirement with time range" do
      start_time = DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC")
      end_time = DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC")

      req =
        TimeBasedRequirement.new(
          type: :flexible_break,
          timing: {:range, start_time, end_time},
          priority: 70,
          description: "Flexible morning break",
          employee_id: "emp_001"
        )

      assert req.timing == {:range, start_time, end_time}
    end

    test "detects conflict with overlapping ranges" do
      req1 =
        TimeBasedRequirement.new(
          type: :break,
          timing:
            {:range, DateTime.new!(~D[2024-01-15], ~T[10:00:00], "Etc/UTC"),
             DateTime.new!(~D[2024-01-15], ~T[12:00:00], "Etc/UTC")},
          priority: 70,
          description: "Morning break window",
          employee_id: "emp_001"
        )

      req2 =
        TimeBasedRequirement.new(
          type: :break,
          timing:
            {:range, DateTime.new!(~D[2024-01-15], ~T[11:00:00], "Etc/UTC"),
             DateTime.new!(~D[2024-01-15], ~T[13:00:00], "Etc/UTC")},
          priority: 70,
          description: "Overlapping break window",
          employee_id: "emp_001"
        )

      assert TimeBasedRequirement.conflicts_with?(req1, req2) == :timing_conflict
    end
  end
end
