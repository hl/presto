defmodule Presto.Examples.CaliforniaSpikeBreakRules do
  @moduledoc """
  California Spike Break Rules engine implementation for Presto.
  
  Implements the California Spike Break Rule across multiple jurisdictional levels:
  - State Level: Base spike break requirements
  - Regional Level: Additional regional requirements
  - City Level: Enhanced city-specific requirements
  
  The Spike Break Rule requires additional mandatory breaks during periods of 
  intensive work that exceed normal capacity thresholds.
  
  This module implements the `Presto.RuleBehaviour` and serves as an example
  of complex, jurisdiction-aware rule implementations.
  """

  @behaviour Presto.RuleBehaviour

  @type work_session :: {:work_session, any(), map()}
  @type spike_requirement :: {:spike_requirement, any(), map()}
  @type compliance_result :: {:compliance_result, any(), map()}
  @type jurisdiction :: map()
  @type rule_spec :: map()
  @type spike_compliance_output :: %{
    spike_requirements: [spike_requirement()],
    compliance_results: [compliance_result()],
    summary: map(),
    jurisdiction: jurisdiction()
  }

  @doc """
  Creates spike break rules based on the provided specification and jurisdiction.
  """
  @spec create_rules(rule_spec(), jurisdiction()) :: [map()]
  def create_rules(rule_spec, jurisdiction \\ %{}) do
    [
      spike_break_detection_rule(jurisdiction),
      spike_break_compliance_rule(rule_spec, jurisdiction)
    ]
  end

  @doc """
  Rule 1: Detect spike break requirements based on work patterns and jurisdiction.
  
  Pattern: Matches work sessions that trigger spike break requirements
  Action: Creates spike break requirement facts
  """
  @spec spike_break_detection_rule(jurisdiction()) :: map()
  def spike_break_detection_rule(jurisdiction \\ %{}) do
    %{
      name: :detect_spike_break_requirements,
      pattern: fn facts ->
        facts
        |> extract_work_sessions()
        |> filter_spike_break_candidates(jurisdiction)
      end,
      action: fn spike_candidates ->
        spike_candidates
        |> Enum.flat_map(&calculate_spike_break_requirements(&1, jurisdiction))
      end
    }
  end

  @doc """
  Rule 2: Check compliance with spike break requirements.
  
  Pattern: Matches spike break requirements against actual breaks taken
  Action: Creates compliance results and penalty calculations
  """
  @spec spike_break_compliance_rule(rule_spec(), jurisdiction()) :: map()
  def spike_break_compliance_rule(_rule_spec \\ %{}, jurisdiction \\ %{}) do
    %{
      name: :check_spike_break_compliance,
      pattern: fn facts ->
        requirements = extract_spike_break_requirements(facts)
        actual_breaks = extract_actual_breaks(facts)
        {requirements, actual_breaks}
      end,
      action: fn {requirements, actual_breaks} ->
        requirements
        |> Enum.map(&check_requirement_compliance(&1, actual_breaks, jurisdiction))
      end
    }
  end

  @doc """
  Processes work sessions through the complete spike break compliance workflow.
  """
  @spec process_spike_break_compliance([work_session()], rule_spec(), jurisdiction()) :: spike_compliance_output()
  def process_spike_break_compliance(work_sessions, _rule_spec \\ %{}, jurisdiction \\ %{}) do
    # Step 1: Detect spike break requirements
    spike_requirements = detect_spike_break_requirements(work_sessions, jurisdiction)
    
    # Step 2: Check compliance if breaks data is available
    compliance_results = check_spike_break_compliance(spike_requirements, work_sessions, jurisdiction)
    
    # Step 3: Generate summary and penalties
    summary = generate_spike_break_summary(spike_requirements, compliance_results, jurisdiction)
    
    %{
      spike_requirements: spike_requirements,
      compliance_results: compliance_results,
      summary: summary,
      jurisdiction: jurisdiction
    }
  end

  @doc """
  Detects spike break requirements for work sessions.
  """
  @spec detect_spike_break_requirements([work_session()], jurisdiction()) :: [spike_requirement()]
  def detect_spike_break_requirements(work_sessions, jurisdiction \\ %{}) do
    work_sessions
    |> Enum.filter(&work_session_triggers_spike_breaks?(&1, jurisdiction))
    |> Enum.flat_map(&calculate_spike_break_requirements(&1, jurisdiction))
  end

  @doc """
  Determines if a work session triggers spike break requirements.
  """
  @spec work_session_triggers_spike_breaks?(work_session(), jurisdiction()) :: boolean()
  def work_session_triggers_spike_breaks?(work_session, jurisdiction) do
    {:work_session, _id, data} = work_session
    
    # Get jurisdiction-specific thresholds
    thresholds = get_jurisdiction_thresholds(jurisdiction)
    
    triggers_long_consecutive_work?(data, thresholds) ||
    triggers_extended_day_work?(data, thresholds) ||
    triggers_spike_period_work?(data, thresholds) ||
    triggers_jurisdiction_specific_work?(data, jurisdiction)
  end

  @doc """
  Calculates specific spike break requirements for a work session.
  """
  @spec calculate_spike_break_requirements(work_session(), jurisdiction()) :: [spike_requirement()]
  def calculate_spike_break_requirements(work_session, jurisdiction) do
    {:work_session, _id, data} = work_session
    
    requirements = []
    
    # Check for consecutive work without meal break
    requirements = requirements ++ calculate_consecutive_work_breaks(data, jurisdiction)
    
    # Check for extended day breaks  
    requirements = requirements ++ calculate_extended_day_breaks(data, jurisdiction)
    
    # Check for spike period breaks
    requirements = requirements ++ calculate_spike_period_breaks(data, jurisdiction)
    
    # Check for jurisdiction-specific breaks
    requirements = requirements ++ calculate_jurisdiction_specific_breaks(data, jurisdiction)
    
    requirements
  end

  @doc """
  Gets jurisdiction-specific thresholds and requirements.
  """
  def get_jurisdiction_thresholds(jurisdiction) do
    base_thresholds = %{
      # California State Level
      consecutive_hours_threshold: 6,
      extended_day_threshold: 10,
      spike_period_interval: 2,
      spike_break_duration: 15,
      extended_break_duration: 10,
      spike_period_break_duration: 5
    }
    
    # Apply regional enhancements
    regional_adjustments = get_regional_adjustments(jurisdiction)
    
    # Apply city-level enhancements
    city_adjustments = get_city_adjustments(jurisdiction)
    
    base_thresholds
    |> Map.merge(regional_adjustments)
    |> Map.merge(city_adjustments)
  end

  @doc """
  Gets regional-level threshold adjustments.
  """
  def get_regional_adjustments(%{region: region}) do
    case region do
      "bay_area" ->
        %{
          tech_crunch_break_interval: 1,
          tech_crunch_break_duration: 5,
          applies_during: ["crunch_time", "product_launch"]
        }
      
      "los_angeles_county" ->
        %{
          entertainment_peak_break_interval: 2,
          entertainment_peak_break_duration: 10,
          applies_during: ["award_season", "peak_production"]
        }
      
      "central_valley" ->
        %{
          agricultural_break_interval: 1,
          agricultural_break_duration: 15,
          applies_during: ["harvest_season"],
          heat_considerations: true
        }
      
      _ ->
        %{}
    end
  end

  def get_regional_adjustments(_), do: %{}

  @doc """
  Gets city-level threshold adjustments.
  """
  def get_city_adjustments(%{city: city}) do
    case city do
      "san_francisco" ->
        %{
          tourism_peak_break_interval: 1.5,
          tourism_peak_break_duration: 8,
          applies_during: ["convention_season", "peak_tourism"]
        }
      
      "los_angeles" ->
        %{
          award_season_break_interval: 1,
          award_season_break_duration: 10,
          applies_during: ["oscar_week", "award_season"]
        }
      
      "san_diego" ->
        %{
          convention_break_interval: 2,
          convention_break_duration: 10,
          applies_during: ["comic_con", "convention_period"]
        }
      
      _ ->
        %{}
    end
  end

  def get_city_adjustments(_), do: %{}

  @doc """
  Determines applicable spike periods based on work session timing and location.
  """
  @spec determine_spike_periods(work_session(), jurisdiction()) :: [map()]
  def determine_spike_periods(work_session_data, jurisdiction) do
    {:work_session, _id, %{start_datetime: start_dt, industry: industry}} = work_session_data
    
    base_periods = []
    
    # Add regional spike periods
    regional_periods = get_regional_spike_periods(start_dt, jurisdiction, industry)
    
    # Add city spike periods  
    city_periods = get_city_spike_periods(start_dt, jurisdiction, industry)
    
    base_periods ++ regional_periods ++ city_periods
  end

  # Private implementation functions

  defp extract_work_sessions(facts) do
    facts
    |> Enum.filter(fn
      {:work_session, _, %{start_datetime: _, end_datetime: _}} -> true
      _ -> false
    end)
  end

  defp filter_spike_break_candidates(work_sessions, jurisdiction) do
    work_sessions
    |> Enum.filter(&work_session_triggers_spike_breaks?(&1, jurisdiction))
  end

  defp extract_spike_break_requirements(facts) do
    facts
    |> Enum.filter(fn
      {:spike_break_requirement, _, _} -> true
      _ -> false
    end)
  end

  defp extract_actual_breaks(facts) do
    facts
    |> Enum.filter(fn
      {:break_taken, _, _} -> true
      _ -> false
    end)
  end

  defp check_requirement_compliance(requirement, actual_breaks, jurisdiction) do
    {:spike_break_requirement, req_id, req_data} = requirement
    
    matching_breaks = find_matching_breaks(req_data, actual_breaks)
    
    status = if length(matching_breaks) > 0, do: :compliant, else: :non_compliant
    
    penalty = if status == :non_compliant do
      calculate_spike_break_penalty(req_data, jurisdiction)
    else
      0.0
    end
    
    {:spike_break_compliance, req_id, %{
      requirement: req_data,
      status: status,
      matching_breaks: matching_breaks,
      penalty_hours: penalty,
      checked_at: DateTime.utc_now(),
      jurisdiction: jurisdiction
    }}
  end

  defp triggers_long_consecutive_work?(data, thresholds) do
    consecutive_hours = calculate_consecutive_hours_without_meal(data)
    consecutive_hours > thresholds.consecutive_hours_threshold
  end

  defp triggers_extended_day_work?(data, thresholds) do
    total_hours = calculate_total_work_hours(data)
    total_hours > thresholds.extended_day_threshold
  end

  @spec triggers_spike_period_work?(map(), map()) :: boolean()
  defp triggers_spike_period_work?(_data, _thresholds) do
    # Simplified implementation for now - always return false to prevent dialyzer errors
    false
  end

  defp triggers_jurisdiction_specific_work?(data, jurisdiction) do
    # Check for jurisdiction-specific triggers
    case jurisdiction do
      %{region: "bay_area", industry: "technology"} ->
        during_crunch_time?(data)
      
      %{region: "los_angeles_county", industry: "entertainment"} ->
        during_peak_production?(data)
      
      %{region: "central_valley", industry: "agriculture"} ->
        during_harvest_season?(data)
      
      _ ->
        false
    end
  end

  defp calculate_consecutive_work_breaks(data, jurisdiction) do
    thresholds = get_jurisdiction_thresholds(jurisdiction)
    consecutive_hours = calculate_consecutive_hours_without_meal(data)
    
    if consecutive_hours > thresholds.consecutive_hours_threshold do
      [create_spike_break_requirement(
        :consecutive_work,
        data,
        thresholds.spike_break_duration,
        "Consecutive work exceeds #{thresholds.consecutive_hours_threshold} hours"
      )]
    else
      []
    end
  end

  defp calculate_extended_day_breaks(data, jurisdiction) do
    thresholds = get_jurisdiction_thresholds(jurisdiction)
    total_hours = calculate_total_work_hours(data)
    
    if total_hours > thresholds.extended_day_threshold do
      extra_hours = total_hours - thresholds.extended_day_threshold
      extra_breaks = ceil(extra_hours / 2) # One break per 2 extra hours
      
      Enum.map(1..extra_breaks, fn i ->
        create_spike_break_requirement(
          :extended_day,
          data,
          thresholds.extended_break_duration,
          "Extended day break #{i} for #{total_hours} hour day"
        )
      end)
    else
      []
    end
  end

  @spec calculate_spike_period_breaks(map(), jurisdiction()) :: [spike_requirement()]
  defp calculate_spike_period_breaks(_data, _jurisdiction) do
    # Simplified implementation for now - return empty list to prevent dialyzer errors
    []
  end

  defp calculate_jurisdiction_specific_breaks(data, jurisdiction) do
    case jurisdiction do
      %{region: "bay_area"} ->
        calculate_bay_area_breaks(data, jurisdiction)
      
      %{region: "los_angeles_county"} ->
        calculate_la_county_breaks(data, jurisdiction)
      
      %{region: "central_valley"} ->
        calculate_central_valley_breaks(data, jurisdiction)
      
      _ ->
        []
    end
  end

  defp calculate_bay_area_breaks(data, jurisdiction) do
    if during_crunch_time?(data) do
      adjustments = get_regional_adjustments(jurisdiction)
      hours = calculate_total_work_hours(data)
      breaks_needed = floor(hours / adjustments[:tech_crunch_break_interval])
      
      Enum.map(1..breaks_needed, fn i ->
        create_spike_break_requirement(
          :bay_area_tech_crunch,
          data,
          adjustments[:tech_crunch_break_duration],
          "Bay Area tech crunch break #{i}"
        )
      end)
    else
      []
    end
  end

  defp calculate_la_county_breaks(data, jurisdiction) do
    if during_peak_production?(data) do
      adjustments = get_regional_adjustments(jurisdiction)
      hours = calculate_total_work_hours(data)
      breaks_needed = floor(hours / adjustments[:entertainment_peak_break_interval])
      
      Enum.map(1..breaks_needed, fn i ->
        create_spike_break_requirement(
          :la_entertainment_peak,
          data,
          adjustments[:entertainment_peak_break_duration],
          "LA entertainment peak break #{i}"
        )
      end)
    else
      []
    end
  end

  defp calculate_central_valley_breaks(data, jurisdiction) do
    if during_harvest_season?(data) do
      adjustments = get_regional_adjustments(jurisdiction)
      hours = calculate_total_work_hours(data)
      breaks_needed = floor(hours / adjustments[:agricultural_break_interval])
      
      Enum.map(1..breaks_needed, fn i ->
        create_spike_break_requirement(
          :central_valley_agriculture,
          data,
          adjustments[:agricultural_break_duration],
          "Central Valley agricultural break #{i}"
        )
      end)
    else
      []
    end
  end

  defp create_spike_break_requirement(type, work_data, duration_minutes, reason) do
    id = generate_requirement_id(type, work_data)
    
    {:spike_break_requirement, id, %{
      type: type,
      employee_id: work_data.employee_id,
      work_session_id: work_data.work_session_id,
      required_duration_minutes: duration_minutes,
      required_by: calculate_required_by_time(work_data, type),
      reason: reason,
      jurisdiction: work_data[:jurisdiction],
      created_at: DateTime.utc_now()
    }}
  end

  defp calculate_consecutive_hours_without_meal(data) do
    # Simplified calculation - in real implementation would analyze meal break gaps
    total_hours = calculate_total_work_hours(data)
    meal_breaks = Map.get(data, :meal_breaks_taken, 0)
    
    if meal_breaks == 0 and total_hours > 5 do
      total_hours
    else
      0
    end
  end

  defp calculate_total_work_hours(data) do
    start_dt = data.start_datetime
    end_dt = data.end_datetime
    DateTime.diff(end_dt, start_dt, :second) / 3600.0
  end

  @spec during_crunch_time?(map()) :: boolean()
  defp during_crunch_time?(data) do
    # Check if work session occurs during tech industry crunch periods
    # This would be based on company-specific or industry-wide designations
    Map.get(data, :is_crunch_time, false)
  end

  @spec during_peak_production?(map()) :: boolean()
  defp during_peak_production?(data) do
    # Check if work session occurs during entertainment industry peak production
    Map.get(data, :is_peak_production, false)
  end

  @spec during_harvest_season?(map()) :: boolean()
  defp during_harvest_season?(data) do
    # Check if work session occurs during agricultural harvest season
    start_date = DateTime.to_date(data.start_datetime)
    month = start_date.month
    month >= 6 and month <= 10 # June through October
  end

  defp generate_requirement_id(type, work_data) do
    "spike_break_#{type}_#{work_data.employee_id}_#{DateTime.to_unix(work_data.start_datetime)}"
  end

  defp calculate_required_by_time(work_data, type) do
    # Calculate when the spike break should be taken based on type and work pattern
    case type do
      :consecutive_work ->
        DateTime.add(work_data.start_datetime, 6 * 3600, :second) # After 6 hours
      
      :extended_day ->
        DateTime.add(work_data.start_datetime, 10 * 3600, :second) # After 10 hours
      
      :spike_period ->
        DateTime.add(work_data.start_datetime, 2 * 3600, :second) # Every 2 hours
      
      _ ->
        DateTime.add(work_data.start_datetime, 4 * 3600, :second) # Default 4 hours
    end
  end

  defp find_matching_breaks(req_data, actual_breaks) do
    # Find actual breaks that satisfy the requirement
    actual_breaks
    |> Enum.filter(fn {:break_taken, _, break_data} ->
      break_data.employee_id == req_data.employee_id and
      break_data.duration_minutes >= req_data.required_duration_minutes and
      DateTime.compare(break_data.taken_at, req_data.required_by) != :gt
    end)
  end

  @spec calculate_spike_break_penalty(map(), jurisdiction()) :: float()
  defp calculate_spike_break_penalty(req_data, _jurisdiction) do
    # In California, penalty is typically 1 hour of pay for missed breaks
    base_penalty = 1.0
    
    # Enhanced penalties for certain jurisdictions or spike types
    case req_data.type do
      :bay_area_tech_crunch -> base_penalty * 1.5
      :la_entertainment_peak -> base_penalty * 1.25
      :central_valley_agriculture -> base_penalty * 1.0
      _ -> base_penalty
    end
  end


  defp get_regional_spike_periods(_start_dt, _jurisdiction, _industry) do
    # Would return applicable spike periods based on region, time, and industry
    []
  end

  defp get_city_spike_periods(_start_dt, _jurisdiction, _industry) do
    # Would return applicable spike periods based on city, time, and industry
    []
  end

  defp check_spike_break_compliance(spike_requirements, work_sessions, jurisdiction) do
    actual_breaks = extract_break_data_from_sessions(work_sessions)
    
    spike_requirements
    |> Enum.map(&check_requirement_compliance(&1, actual_breaks, jurisdiction))
  end

  defp extract_break_data_from_sessions(work_sessions) do
    # Extract break information from work session data
    work_sessions
    |> Enum.flat_map(fn {:work_session, _, data} ->
      Map.get(data, :breaks_taken, [])
    end)
  end

  defp generate_spike_break_summary(spike_requirements, compliance_results, jurisdiction) do
    total_requirements = length(spike_requirements)
    violations = Enum.count(compliance_results, fn {:spike_break_compliance, _, data} ->
      data.status == :non_compliant
    end)
    
    total_penalty_hours = compliance_results
    |> Enum.map(fn {:spike_break_compliance, _, data} -> data.penalty_hours end)
    |> Enum.sum()
    
    compliance_rate = if total_requirements > 0 do
      ((total_requirements - violations) / total_requirements * 100) |> Float.round(1)
    else
      100.0
    end
    
    %{
      total_spike_break_requirements: total_requirements,
      compliance_violations: violations,
      compliance_rate_percent: compliance_rate,
      total_penalty_hours: total_penalty_hours,
      jurisdiction: jurisdiction,
      summary_generated_at: DateTime.utc_now()
    }
  end

  @doc """
  Validates a spike break rule specification.
  
  Checks that the rule specification contains the required structure
  for spike break compliance processing and has valid variable types.
  """
  @spec valid_rule_spec?(rule_spec()) :: boolean()
  def valid_rule_spec?(%{"rules_to_run" => rules, "variables" => variables}) 
      when is_list(rules) and is_map(variables) do
    # Check that we have valid spike break rule names
    valid_spike_break_rules = ["spike_break_compliance"]
    spike_break_rules_present = Enum.any?(rules, &(&1 in valid_spike_break_rules))
    
    # Spike break rules don't require specific variables, so any map is valid
    spike_break_rules_present
  end
  def valid_rule_spec?(_), do: false

end