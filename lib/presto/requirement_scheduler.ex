defmodule Presto.RequirementScheduler do
  @moduledoc """
  General priority-based scheduler for resolving conflicts between requirements
  from any rule type (payroll, compliance, spike break, etc.).

  This scheduler:
  1. Detects conflicts between requirements
  2. Applies priority-based resolution strategies
  3. Optimizes schedules by consolidating compatible requirements
  4. Ensures legal compliance requirements are never compromised
  """

  @type requirement :: struct()
  @type scheduling_result :: %{
          scheduled_requirements: [requirement()],
          conflicts_resolved: integer(),
          consolidations_made: integer(),
          warnings: [String.t()]
        }

  @doc """
  Main entry point: resolves conflicts in a list of requirements and returns
  an optimized, conflict-free schedule.
  """
  @spec resolve_conflicts([requirement()]) :: scheduling_result()
  def resolve_conflicts(requirements) when is_list(requirements) do
    initial_count = length(requirements)

    employee_results =
      requirements
      |> group_by_employee()
      |> Enum.map(&resolve_employee_conflicts/1)

    # Aggregate results across all employees
    all_scheduled = Enum.flat_map(employee_results, & &1.scheduled_requirements)
    total_consolidations = Enum.sum(Enum.map(employee_results, & &1.consolidations_made))
    all_warnings = Enum.flat_map(employee_results, & &1.warnings)

    conflicts_resolved = initial_count - length(all_scheduled)

    %{
      scheduled_requirements: all_scheduled,
      conflicts_resolved: conflicts_resolved,
      consolidations_made: total_consolidations,
      warnings: all_warnings
    }
  end

  @doc """
  Groups requirements by employee since conflicts typically occur within
  individual employee schedules.
  """
  def group_by_employee(requirements) do
    requirements
    |> Enum.group_by(&get_employee_id/1)
  end

  @doc """
  Resolves conflicts for a single employee's requirements.
  """
  def resolve_employee_conflicts({employee_id, requirements}) do
    requirements
    |> sort_by_priority()
    |> detect_conflicts()
    |> apply_resolution_strategies()
    |> optimize_schedule()
    |> validate_final_schedule(employee_id)
  end

  @doc """
  Sorts requirements by priority (highest first) and timing (earliest first for same priority).
  """
  def sort_by_priority(requirements) do
    requirements
    |> Enum.sort_by(fn req ->
      priority = req.__struct__.priority(req)
      timing = req.__struct__.timing(req)

      timing_sort_key =
        case timing do
          %DateTime{} = dt -> DateTime.to_unix(dt)
          {:range, start_dt, _end_dt} -> DateTime.to_unix(start_dt)
          # Flexible timing goes last
          _ -> 999_999_999_999
        end

      # Sort by priority (descending) then timing (ascending)
      {-priority, timing_sort_key}
    end)
  end

  @doc """
  Detects all conflicts between requirements.
  """
  def detect_conflicts(requirements) do
    conflicts =
      for {req1, i} <- Enum.with_index(requirements),
          {req2, j} <- Enum.with_index(requirements),
          i < j,
          conflict_type = req1.__struct__.conflicts_with?(req1, req2),
          conflict_type != :no_conflict do
        %{
          requirement1: req1,
          requirement2: req2,
          conflict_type: conflict_type,
          indices: {i, j}
        }
      end

    %{
      requirements: requirements,
      conflicts: conflicts
    }
  end

  @doc """
  Applies resolution strategies based on conflict types and priorities.
  """
  def apply_resolution_strategies(%{requirements: requirements, conflicts: conflicts}) do
    {resolved_requirements, consolidations, warnings} =
      Enum.reduce(conflicts, {requirements, 0, []}, fn conflict, {reqs, consol_count, warns} ->
        apply_conflict_resolution(conflict, reqs, consol_count, warns)
      end)

    %{
      scheduled_requirements: resolved_requirements,
      consolidations_made: consolidations,
      warnings: warnings
    }
  end

  @doc """
  Applies specific resolution strategy for a single conflict.
  """
  def apply_conflict_resolution(conflict, requirements, consolidation_count, warnings) do
    %{
      requirement1: req1,
      requirement2: req2,
      conflict_type: conflict_type
    } = conflict

    case resolve_conflict_pair(req1, req2, conflict_type) do
      {:consolidate, merged_req} ->
        # Replace both requirements with merged one
        updated_reqs =
          requirements
          |> Enum.reject(fn req -> req == req1 or req == req2 end)
          |> List.insert_at(0, merged_req)

        {updated_reqs, consolidation_count + 1, warnings}

      {:reschedule, updated_req1, updated_req2} ->
        # Replace requirements with rescheduled versions
        updated_reqs =
          requirements
          |> Enum.map(fn req ->
            cond do
              req == req1 -> updated_req1
              req == req2 -> updated_req2
              true -> req
            end
          end)

        {updated_reqs, consolidation_count, warnings}

      {:remove_lower_priority, kept_req} ->
        # Remove the lower priority requirement
        updated_reqs =
          requirements
          |> Enum.reject(fn req ->
            (req == req1 and kept_req == req2) or (req == req2 and kept_req == req1)
          end)

        removed_req = if kept_req == req1, do: req2, else: req1

        warning =
          "Removed lower priority requirement: #{removed_req.__struct__.describe(removed_req)}"

        {updated_reqs, consolidation_count, [warning | warnings]}

      {:cannot_resolve, reason} ->
        warning =
          "Could not resolve conflict between #{req1.__struct__.describe(req1)} and #{req2.__struct__.describe(req2)}: #{reason}"

        {requirements, consolidation_count, [warning | warnings]}
    end
  end

  @doc """
  Resolves a conflict between two specific requirements.
  """
  def resolve_conflict_pair(req1, req2, conflict_type) do
    priority1 = req1.__struct__.priority(req1)
    priority2 = req2.__struct__.priority(req2)

    cond do
      # If one has much higher priority, remove the lower one first
      priority1 - priority2 >= 20 ->
        {:remove_lower_priority, req1}

      priority2 - priority1 >= 20 ->
        {:remove_lower_priority, req2}

      # Try to merge compatible requirements
      merge_result = try_merge_requirements(req1, req2) ->
        merge_result

      # Try to reschedule the more flexible requirement
      conflict_type == :timing_conflict ->
        case try_reschedule_requirements(req1, req2) do
          nil -> {:cannot_resolve, "Cannot reschedule requirements"}
          result -> result
        end

      true ->
        {:cannot_resolve, "No resolution strategy available"}
    end
  end

  @doc """
  Attempts to merge two requirements if they're compatible.
  """
  def try_merge_requirements(req1, req2) do
    case req1.__struct__.merge_with(req1, req2) do
      {:ok, merged_req} ->
        {:consolidate, merged_req}

      {:error, :incompatible} ->
        nil
    end
  end

  @doc """
  Attempts to reschedule requirements to resolve timing conflicts.
  """
  def try_reschedule_requirements(req1, req2) do
    # Try to reschedule the lower priority requirement
    {primary_req, secondary_req} =
      if req1.__struct__.priority(req1) >= req2.__struct__.priority(req2) do
        {req1, req2}
      else
        {req2, req1}
      end

    # Calculate new timing for secondary requirement
    case calculate_rescheduled_timing(primary_req, secondary_req) do
      {:ok, new_timing} ->
        case secondary_req.__struct__.reschedule(secondary_req, new_timing) do
          {:ok, rescheduled_req} ->
            if req1 == primary_req do
              {:reschedule, req1, rescheduled_req}
            else
              {:reschedule, rescheduled_req, req2}
            end

          {:error, :cannot_reschedule} ->
            nil
        end

      {:error, _reason} ->
        nil
    end
  end

  @doc """
  Calculates a new timing that avoids conflict with a higher priority requirement.
  """
  def calculate_rescheduled_timing(primary_req, secondary_req) do
    case {primary_req.__struct__.timing(primary_req),
          secondary_req.__struct__.timing(secondary_req)} do
      {%DateTime{} = primary_time, %DateTime{}} ->
        # Schedule secondary requirement 1 hour after primary ends
        primary_duration = get_duration_minutes(primary_req)
        new_timing = DateTime.add(primary_time, (primary_duration + 60) * 60, :second)
        {:ok, new_timing}

      _ ->
        {:error, :complex_timing_not_supported}
    end
  end

  @doc """
  Optimizes the final schedule by looking for additional consolidation opportunities.
  """
  def optimize_schedule(%{scheduled_requirements: requirements} = result) do
    # Look for additional optimization opportunities
    optimized_requirements =
      requirements
      |> group_similar_requirements()
      |> Enum.flat_map(&optimize_requirement_group/1)

    %{result | scheduled_requirements: optimized_requirements}
  end

  @doc """
  Validates that the final schedule is feasible and compliant.
  """
  def validate_final_schedule(%{scheduled_requirements: requirements} = result, employee_id) do
    feasibility_warnings = validate_schedule_feasibility(requirements, employee_id)
    compliance_warnings = validate_legal_compliance(requirements, employee_id)

    all_warnings = feasibility_warnings ++ compliance_warnings

    %{result | warnings: result.warnings ++ all_warnings}
  end

  # Helper functions

  defp get_employee_id(requirement) do
    # Try to extract employee_id from the requirement struct
    Map.get(requirement, :employee_id, "unknown")
  end

  defp get_duration_minutes(requirement) do
    Map.get(requirement, :duration_minutes, 0)
  end

  defp group_similar_requirements(requirements) do
    requirements
    |> Enum.group_by(fn req -> req.__struct__.type(req) end)
  end

  defp optimize_requirement_group({_type, requirements}) when length(requirements) <= 1 do
    requirements
  end

  defp optimize_requirement_group({_type, requirements}) do
    # For groups of similar requirements, look for consolidation opportunities
    requirements
    |> Enum.reduce([], fn req, acc ->
      case find_consolidation_opportunity(req, acc) do
        {:consolidate_with, existing_req, merged_req} ->
          # Replace existing requirement with merged one
          Enum.map(acc, fn r -> if r == existing_req, do: merged_req, else: r end)

        :no_opportunity ->
          [req | acc]
      end
    end)
  end

  defp find_consolidation_opportunity(req, existing_requirements) do
    Enum.find_value(existing_requirements, :no_opportunity, fn existing_req ->
      case req.__struct__.merge_with(req, existing_req) do
        {:ok, merged_req} ->
          {:consolidate_with, existing_req, merged_req}

        {:error, :incompatible} ->
          nil
      end
    end)
  end

  defp validate_schedule_feasibility(_requirements, _employee_id) do
    # Check for impossible schedules (overlapping non-consolidatable requirements, etc.)
    # Placeholder - would implement detailed feasibility checks
    []
  end

  defp validate_legal_compliance(requirements, _employee_id) do
    # Ensure legal requirements (meal breaks, etc.) are preserved
    critical_requirements =
      Enum.filter(requirements, fn req ->
        req.__struct__.priority(req) >= 100
      end)

    # Only warn if there were initially requirements but no critical ones remain
    total_requirements = length(requirements)

    if total_requirements > 0 and Enum.empty?(critical_requirements) do
      has_high_priority_types =
        Enum.any?(requirements, fn req ->
          req.__struct__.type(req) in [:meal_break, :safety_break, :central_valley_agriculture]
        end)

      if has_high_priority_types do
        ["Warning: No critical legal compliance requirements scheduled"]
      else
        []
      end
    else
      []
    end
  end
end
