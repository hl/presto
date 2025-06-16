defmodule Presto.RequirementBehaviour do
  @moduledoc """
  Behaviour for all rule requirements that can participate in priority-based scheduling.

  This enables conflict detection and resolution across all rule types (payroll,
  compliance, spike break, etc.) in a unified way.
  """

  @type requirement :: struct()
  @type conflict_result ::
          :no_conflict | :timing_conflict | :resource_conflict | :logical_conflict
  @type merge_result :: {:ok, requirement()} | {:error, :incompatible}

  @doc """
  Returns the priority level of this requirement.
  Higher numbers indicate higher priority.

  Standard priority levels:
  - 100: Critical legal compliance (meal breaks, safety requirements)
  - 90: Regulatory compliance (weekly hour limits, overtime calculations)
  - 70: Industry standards and best practices
  - 50: Operational efficiency and enhancements
  """
  @callback priority(requirement()) :: integer()

  @doc """
  Returns the category/type of this requirement for grouping and analysis.
  """
  @callback type(requirement()) :: atom()

  @doc """
  Detects if this requirement conflicts with another requirement.
  """
  @callback conflicts_with?(requirement(), requirement()) :: conflict_result()

  @doc """
  Attempts to merge this requirement with another compatible requirement.
  Returns merged requirement or error if incompatible.
  """
  @callback merge_with(requirement(), requirement()) :: merge_result()

  @doc """
  Reschedules this requirement to a new time, if possible.
  Returns updated requirement or error if rescheduling is not allowed.
  """
  @callback reschedule(requirement(), new_timing :: DateTime.t()) ::
              {:ok, requirement()} | {:error, :cannot_reschedule}

  @doc """
  Returns a human-readable description of the requirement.
  """
  @callback describe(requirement()) :: String.t()

  @doc """
  Returns the timing constraint for this requirement (when it must occur).
  """
  @callback timing(requirement()) ::
              DateTime.t() | {:range, DateTime.t(), DateTime.t()} | :flexible

  @doc """
  Returns metadata about the requirement for logging and analysis.
  """
  @callback metadata(requirement()) :: map()
end
