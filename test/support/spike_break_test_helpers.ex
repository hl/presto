defmodule Presto.SpikeBreakTestHelpers do
  @moduledoc """
  Test helpers specific to California Spike Break Rules testing.
  Supports multi-jurisdictional testing scenarios.
  """

  alias Presto.Factories

  @doc """
  Creates a standard work session without spike break triggers.
  """
  def create_standard_work_session(employee_id, date, hours \\ 8) do
    start_dt = DateTime.new!(date, ~T[09:00:00], "Etc/UTC")
    end_dt = DateTime.add(start_dt, trunc(hours * 3600), :second)

    {:work_session, "session_#{employee_id}_#{Date.to_string(date)}", %{
      employee_id: employee_id,
      work_session_id: "session_#{employee_id}_#{Date.to_string(date)}",
      start_datetime: start_dt,
      end_datetime: end_dt,
      total_hours: hours,
      meal_breaks_taken: if(hours > 5, do: 1, else: 0),
      breaks_taken: []
    }}
  end

  @doc """
  Creates a work session that triggers consecutive work spike breaks.
  """
  def create_consecutive_work_session(employee_id, date, hours_without_meal \\ 7) do
    start_dt = DateTime.new!(date, ~T[09:00:00], "Etc/UTC")
    end_dt = DateTime.add(start_dt, trunc(hours_without_meal * 3600), :second)

    {:work_session, "consecutive_#{employee_id}_#{Date.to_string(date)}", %{
      employee_id: employee_id,
      work_session_id: "consecutive_#{employee_id}_#{Date.to_string(date)}",
      start_datetime: start_dt,
      end_datetime: end_dt,
      total_hours: hours_without_meal,
      meal_breaks_taken: 0, # No meal breaks - triggers spike break
      breaks_taken: []
    }}
  end

  @doc """
  Creates a work session that triggers extended day spike breaks.
  """
  def create_extended_day_session(employee_id, date, total_hours \\ 12) do
    start_dt = DateTime.new!(date, ~T[07:00:00], "Etc/UTC")
    end_dt = DateTime.add(start_dt, trunc(total_hours * 3600), :second)

    {:work_session, "extended_#{employee_id}_#{Date.to_string(date)}", %{
      employee_id: employee_id,
      work_session_id: "extended_#{employee_id}_#{Date.to_string(date)}",
      start_datetime: start_dt,
      end_datetime: end_dt,
      total_hours: total_hours,
      meal_breaks_taken: if(total_hours > 5, do: 1, else: 0),
      breaks_taken: []
    }}
  end

  @doc """
  Creates a work session during spike periods (Bay Area tech crunch).
  """
  def create_bay_area_crunch_session(employee_id, date, hours \\ 10) do
    start_dt = DateTime.new!(date, ~T[09:00:00], "Etc/UTC")
    end_dt = DateTime.add(start_dt, trunc(hours * 3600), :second)

    {:work_session, "crunch_#{employee_id}_#{Date.to_string(date)}", %{
      employee_id: employee_id,
      work_session_id: "crunch_#{employee_id}_#{Date.to_string(date)}",
      start_datetime: start_dt,
      end_datetime: end_dt,
      total_hours: hours,
      meal_breaks_taken: 1,
      breaks_taken: [],
      is_crunch_time: true,
      industry: "technology",
      jurisdiction: %{state: "california", region: "bay_area", city: "san_francisco"}
    }}
  end

  @doc """
  Creates a work session for LA entertainment peak production.
  """
  def create_la_entertainment_session(employee_id, date, hours \\ 14) do
    start_dt = DateTime.new!(date, ~T[06:00:00], "Etc/UTC")
    end_dt = DateTime.add(start_dt, trunc(hours * 3600), :second)

    {:work_session, "entertainment_#{employee_id}_#{Date.to_string(date)}", %{
      employee_id: employee_id,
      work_session_id: "entertainment_#{employee_id}_#{Date.to_string(date)}",
      start_datetime: start_dt,
      end_datetime: end_dt,
      total_hours: hours,
      meal_breaks_taken: 2,
      breaks_taken: [],
      is_peak_production: true,
      industry: "entertainment",
      jurisdiction: %{state: "california", region: "los_angeles_county", city: "los_angeles"}
    }}
  end

  @doc """
  Creates a work session for Central Valley agricultural work during harvest.
  """
  def create_central_valley_agriculture_session(employee_id, date, hours \\ 10) do
    start_dt = DateTime.new!(date, ~T[05:00:00], "Etc/UTC") # Early start for agriculture
    end_dt = DateTime.add(start_dt, trunc(hours * 3600), :second)

    {:work_session, "agriculture_#{employee_id}_#{Date.to_string(date)}", %{
      employee_id: employee_id,
      work_session_id: "agriculture_#{employee_id}_#{Date.to_string(date)}",
      start_datetime: start_dt,
      end_datetime: end_dt,
      total_hours: hours,
      meal_breaks_taken: 1,
      breaks_taken: [],
      industry: "agriculture",
      jurisdiction: %{state: "california", region: "central_valley"}
    }}
  end

  @doc """
  Creates a work session with actual breaks taken for compliance testing.
  """
  def create_work_session_with_breaks(employee_id, date, total_hours, breaks_taken) do
    start_dt = DateTime.new!(date, ~T[08:00:00], "Etc/UTC")
    end_dt = DateTime.add(start_dt, trunc(total_hours * 3600), :second)

    {:work_session, "with_breaks_#{employee_id}_#{Date.to_string(date)}", %{
      employee_id: employee_id,
      work_session_id: "with_breaks_#{employee_id}_#{Date.to_string(date)}",
      start_datetime: start_dt,
      end_datetime: end_dt,
      total_hours: total_hours,
      meal_breaks_taken: 1,
      breaks_taken: breaks_taken
    }}
  end

  @doc """
  Creates a break taken fact for compliance testing.
  """
  def create_break_taken(employee_id, taken_at, duration_minutes, break_type \\ :spike_break) do
    break_id = "break_#{employee_id}_#{DateTime.to_unix(taken_at)}"

    {:break_taken, break_id, %{
      employee_id: employee_id,
      taken_at: taken_at,
      duration_minutes: duration_minutes,
      break_type: break_type,
      paid: true
    }}
  end

  @doc """
  Creates jurisdiction configurations for testing different levels.
  """
  def create_jurisdiction_configs do
    %{
      state_only: %{
        state: "california"
      },

      bay_area_tech: %{
        state: "california",
        region: "bay_area",
        city: "san_francisco",
        industry: "technology"
      },

      la_entertainment: %{
        state: "california",
        region: "los_angeles_county",
        city: "los_angeles",
        industry: "entertainment"
      },

      central_valley_agriculture: %{
        state: "california",
        region: "central_valley",
        industry: "agriculture"
      },

      san_diego_hospitality: %{
        state: "california",
        region: "san_diego_county",
        city: "san_diego",
        industry: "hospitality"
      }
    }
  end

  @doc """
  Creates a complex multi-jurisdiction scenario.
  """
  def create_multi_jurisdiction_scenario(base_date) do
    jurisdictions = create_jurisdiction_configs()

    %{
      # Bay Area tech worker during crunch
      bay_area_sessions: [
        create_bay_area_crunch_session("emp_tech_001", base_date, 12),
        create_bay_area_crunch_session("emp_tech_002", base_date, 14),
        create_extended_day_session("emp_tech_003", base_date, 16)
      ],

      # LA entertainment workers during award season
      la_sessions: [
        create_la_entertainment_session("emp_ent_001", base_date, 16),
        create_la_entertainment_session("emp_ent_002", base_date, 18)
      ],

      # Central Valley agricultural workers during harvest
      central_valley_sessions: [
        create_central_valley_agriculture_session("emp_ag_001", base_date, 10),
        create_central_valley_agriculture_session("emp_ag_002", base_date, 12)
      ],

      # Standard California workers (state-level only)
      standard_sessions: [
        create_consecutive_work_session("emp_std_001", base_date, 7),
        create_extended_day_session("emp_std_002", base_date, 11)
      ]
    }
  end

  @doc """
  Creates time-based test scenarios for different periods.
  """
  def create_temporal_scenarios do
    %{
      # Harvest season (triggers agricultural rules)
      harvest_season: ~D[2024-08-15],

      # Award season (triggers entertainment rules)
      award_season: ~D[2024-02-15],

      # Convention season (triggers hospitality rules)
      convention_season: ~D[2024-07-20],

      # Regular work period
      regular_period: ~D[2024-04-15]
    }
  end

  @doc """
  Extracts spike break requirements from result data.
  """
  def extract_spike_break_requirements(requirements) do
    Enum.filter(requirements, fn
      {:spike_break_requirement, _, _} -> true
      _ -> false
    end)
  end

  @doc """
  Returns compliance results statistics.
  """
  def get_compliance_stats(compliance_results) do
    violations = Enum.count(compliance_results, fn {:spike_break_compliance, _, data} ->
      data.status == :non_compliant
    end)

    total_penalties = compliance_results
    |> Enum.map(fn {:spike_break_compliance, _, data} -> data.penalty_hours end)
    |> Enum.sum()

    %{
      violations: violations,
      total_penalty_hours: total_penalties,
      total_results: length(compliance_results)
    }
  end

  @doc """
  Creates a comprehensive spike break test scenario.
  """
  def create_comprehensive_test_scenario(employee_id, date, scenario_type) do
    case scenario_type do
      :consecutive_work_violation ->
        %{
          work_session: create_consecutive_work_session(employee_id, date, 8),
          expected_requirements: 1,
          expected_types: [:consecutive_work],
          jurisdiction: %{state: "california"}
        }

      :extended_day_violation ->
        %{
          work_session: create_extended_day_session(employee_id, date, 14),
          expected_requirements: 2, # One for consecutive work + one for extended day
          expected_types: [:extended_day],
          jurisdiction: %{state: "california"}
        }

      :bay_area_tech_crunch ->
        %{
          work_session: create_bay_area_crunch_session(employee_id, date, 12),
          expected_requirements: 12, # Hourly breaks during crunch
          expected_types: [:bay_area_tech_crunch],
          jurisdiction: %{state: "california", region: "bay_area", city: "san_francisco"}
        }

      :multi_violation ->
        %{
          work_session: create_extended_day_session(employee_id, date, 16),
          expected_requirements: 3, # Consecutive + multiple extended day breaks
          expected_types: [:consecutive_work, :extended_day],
          jurisdiction: %{state: "california"}
        }

      :compliant_standard ->
        %{
          work_session: create_standard_work_session(employee_id, date, 8),
          expected_requirements: 0,
          expected_types: [],
          jurisdiction: %{state: "california"}
        }
    end
  end

  @doc """
  Creates work sessions spanning multiple jurisdictions for boundary testing.
  """
  def create_cross_jurisdiction_scenario(employee_id, date) do
    # Worker travels between jurisdictions in same day
    morning_session = {:work_session, "morning_#{employee_id}", %{
      employee_id: employee_id,
      work_session_id: "morning_#{employee_id}",
      start_datetime: DateTime.new!(date, ~T[06:00:00], "Etc/UTC"),
      end_datetime: DateTime.new!(date, ~T[12:00:00], "Etc/UTC"),
      total_hours: 6,
      meal_breaks_taken: 0,
      breaks_taken: [],
      jurisdiction: %{state: "california", region: "bay_area", city: "san_francisco"}
    }}

    afternoon_session = {:work_session, "afternoon_#{employee_id}", %{
      employee_id: employee_id,
      work_session_id: "afternoon_#{employee_id}",
      start_datetime: DateTime.new!(date, ~T[13:00:00], "Etc/UTC"),
      end_datetime: DateTime.new!(date, ~T[20:00:00], "Etc/UTC"),
      total_hours: 7,
      meal_breaks_taken: 1,
      breaks_taken: [],
      jurisdiction: %{state: "california", region: "los_angeles_county", city: "los_angeles"}
    }}

    [morning_session, afternoon_session]
  end


  @doc """
  Extracts compliance results from a list of facts.
  """
  def extract_compliance_results(facts) do
    facts
    |> Enum.filter(fn
      {:spike_break_compliance, _, _} -> true
      _ -> false
    end)
  end

  @doc """
  Groups results by jurisdiction for analysis.
  """
  def group_by_jurisdiction(results) do
    results
    |> Enum.group_by(fn {_, _, data} ->
      Map.get(data, :jurisdiction, %{state: "california"})
    end)
  end

  @doc """
  Calculates expected penalty hours based on violation count and jurisdiction.
  """
  def calculate_expected_penalties(violations, jurisdiction) do
    base_penalty = 1.0

    multiplier = case jurisdiction do
      %{region: "bay_area"} -> 1.5
      %{region: "los_angeles_county"} -> 1.25
      _ -> 1.0
    end

    violations * base_penalty * multiplier
  end

  @doc """
  Creates realistic break compliance scenarios.
  """
  def create_break_compliance_scenarios(employee_id, date) do
    %{
      # Fully compliant - all required breaks taken
      fully_compliant: %{
        work_session: create_extended_day_session(employee_id, date, 12),
        breaks_taken: [
          create_break_taken(employee_id, DateTime.new!(date, ~T[15:00:00], "Etc/UTC"), 15),
          create_break_taken(employee_id, DateTime.new!(date, ~T[17:00:00], "Etc/UTC"), 10)
        ],
        expected_violations: 0
      },

      # Partially compliant - some breaks taken
      partially_compliant: %{
        work_session: create_extended_day_session(employee_id, date, 12),
        breaks_taken: [
          create_break_taken(employee_id, DateTime.new!(date, ~T[15:00:00], "Etc/UTC"), 15)
        ],
        expected_violations: 1
      },

      # Non-compliant - no breaks taken
      non_compliant: %{
        work_session: create_extended_day_session(employee_id, date, 12),
        breaks_taken: [],
        expected_violations: 2
      }
    }
  end
end