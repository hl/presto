defmodule Presto.Requirements.TimeBasedRequirement do
  @moduledoc """
  A requirement that must occur at a specific time or within a time range.
  Used for breaks, meetings, deadlines, processing windows, etc.
  """

  @behaviour Presto.RequirementBehaviour

  defstruct [
    :id,
    :type,
    :timing,
    :duration_minutes,
    :priority,
    :description,
    :employee_id,
    :flexibility,
    :metadata,
    :created_by_rule
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          type: atom(),
          timing: DateTime.t() | {:range, DateTime.t(), DateTime.t()},
          duration_minutes: integer(),
          priority: integer(),
          description: String.t(),
          employee_id: String.t(),
          flexibility: :none | :low | :medium | :high,
          metadata: map(),
          created_by_rule: atom()
        }

  @impl Presto.RequirementBehaviour
  def priority(%__MODULE__{priority: priority}), do: priority

  @impl Presto.RequirementBehaviour
  def type(%__MODULE__{type: type}), do: type

  @impl Presto.RequirementBehaviour
  def timing(%__MODULE__{timing: timing}), do: timing

  @impl Presto.RequirementBehaviour
  def describe(%__MODULE__{description: description}), do: description

  @impl Presto.RequirementBehaviour
  def metadata(%__MODULE__{metadata: metadata}), do: metadata

  @impl Presto.RequirementBehaviour
  def conflicts_with?(%__MODULE__{} = req1, %__MODULE__{} = req2) do
    # Check if requirements are for the same employee
    if req1.employee_id != req2.employee_id do
      :no_conflict
    else
      check_timing_conflict(req1, req2)
    end
  end

  def conflicts_with?(%__MODULE__{}, _other), do: :no_conflict

  @impl Presto.RequirementBehaviour
  def merge_with(%__MODULE__{} = req1, %__MODULE__{} = req2) do
    priority_gap = abs(req1.priority - req2.priority)
    
    cond do
      # Can't merge requirements for different employees
      req1.employee_id != req2.employee_id ->
        {:error, :incompatible}
      
      # Don't merge if priority gap is too large (>=20) - should be handled by removal
      priority_gap >= 20 ->
        {:error, :incompatible}

      # Can merge if they're the same type and compatible timing
      req1.type == req2.type and compatible_timing?(req1, req2) ->
        merge_compatible_requirements(req1, req2)

      # Can merge if one can satisfy the other's requirements (with small priority gap)
      can_consolidate?(req1, req2) ->
        consolidate_requirements(req1, req2)

      true ->
        {:error, :incompatible}
    end
  end

  def merge_with(%__MODULE__{}, _other), do: {:error, :incompatible}

  @impl Presto.RequirementBehaviour
  def reschedule(%__MODULE__{flexibility: :none}, _new_timing) do
    {:error, :cannot_reschedule}
  end

  def reschedule(%__MODULE__{} = req, new_timing) do
    {:ok, %{req | timing: new_timing}}
  end

  # Helper functions

  defp check_timing_conflict(req1, req2) do
    case {get_time_range(req1), get_time_range(req2)} do
      {{start1, end1}, {start2, end2}} ->
        if time_ranges_overlap?(start1, end1, start2, end2) do
          :timing_conflict
        else
          :no_conflict
        end

      _ ->
        :no_conflict
    end
  end

  defp get_time_range(%__MODULE__{timing: timing, duration_minutes: duration}) do
    case timing do
      %DateTime{} = start_time ->
        end_time = DateTime.add(start_time, duration * 60, :second)
        {start_time, end_time}

      {:range, start_time, end_time} ->
        {start_time, end_time}

      _ ->
        nil
    end
  end

  defp time_ranges_overlap?(start1, end1, start2, end2) do
    not (DateTime.compare(end1, start2) == :lt or DateTime.compare(end2, start1) == :lt)
  end

  defp compatible_timing?(req1, req2) do
    # Requirements are compatible if they can be merged into a single time slot
    case {req1.timing, req2.timing} do
      {%DateTime{} = time1, %DateTime{} = time2} ->
        # If times are within 30 minutes, they can potentially be merged
        abs(DateTime.diff(time1, time2, :minute)) <= 30

      _ ->
        false
    end
  end

  defp merge_compatible_requirements(req1, req2) do
    # Use the higher priority requirement as base
    base_req = if req1.priority >= req2.priority, do: req1, else: req2
    other_req = if req1.priority >= req2.priority, do: req2, else: req1

    # Combine durations and use earlier timing
    combined_duration = req1.duration_minutes + req2.duration_minutes
    earliest_timing = earliest_time(req1.timing, req2.timing)

    merged_req = %{
      base_req
      | duration_minutes: combined_duration,
        timing: earliest_timing,
        description: "#{base_req.description} (consolidated with #{other_req.description})",
        metadata: Map.merge(base_req.metadata, %{consolidated_from: [req1.id, req2.id]})
    }

    {:ok, merged_req}
  end

  defp can_consolidate?(req1, req2) do
    # A longer, higher-priority break can satisfy a shorter, lower-priority break
    cond do
      req1.priority > req2.priority and req1.duration_minutes >= req2.duration_minutes ->
        true

      req2.priority > req1.priority and req2.duration_minutes >= req1.duration_minutes ->
        true

      true ->
        false
    end
  end

  defp consolidate_requirements(req1, req2) do
    # Use the requirement that can satisfy both
    primary_req = if req1.priority >= req2.priority, do: req1, else: req2
    secondary_req = if req1.priority >= req2.priority, do: req2, else: req1

    consolidated_req = %{
      primary_req
      | description: "#{primary_req.description} (satisfies #{secondary_req.description})",
        metadata:
          Map.merge(primary_req.metadata, %{
            satisfies_requirements: [secondary_req.id],
            consolidated_from: [req1.id, req2.id]
          })
    }

    {:ok, consolidated_req}
  end

  defp earliest_time(%DateTime{} = time1, %DateTime{} = time2) do
    if DateTime.compare(time1, time2) == :lt, do: time1, else: time2
  end

  defp earliest_time(time1, _time2), do: time1

  @doc """
  Creates a new time-based requirement.
  """
  def new(opts) do
    %__MODULE__{
      id: Keyword.get(opts, :id, generate_id()),
      type: Keyword.fetch!(opts, :type),
      timing: Keyword.fetch!(opts, :timing),
      duration_minutes: Keyword.get(opts, :duration_minutes, 15),
      priority: Keyword.fetch!(opts, :priority),
      description: Keyword.fetch!(opts, :description),
      employee_id: Keyword.fetch!(opts, :employee_id),
      flexibility: Keyword.get(opts, :flexibility, :low),
      metadata: Keyword.get(opts, :metadata, %{}),
      created_by_rule: Keyword.get(opts, :created_by_rule, :unknown)
    }
  end

  defp generate_id do
    "req_" <> (System.system_time(:nanosecond) |> Integer.to_string())
  end
end