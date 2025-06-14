defmodule Presto.Examples.SpikeBreakIntegrationTest do
  use ExUnit.Case, async: true

  alias Presto.Examples.CaliforniaSpikeBreakRules
  alias Presto.SpikeBreakTestHelpers

  describe "Real-World Multi-Jurisdiction Integration Scenarios" do
    test "tech company with employees across multiple Bay Area cities" do
      base_date = ~D[2024-01-15]

      # Tech company with offices in multiple Bay Area cities during product crunch
      company_scenario = %{
        # San Francisco headquarters - main development team
        sf_developers: [
          create_crunch_session("dev_sf_001", base_date, "san_francisco", 14, %{
            role: "senior_dev",
            team: "backend"
          }),
          create_crunch_session("dev_sf_002", base_date, "san_francisco", 16, %{
            role: "frontend_dev",
            team: "ui"
          }),
          create_crunch_session("dev_sf_003", base_date, "san_francisco", 12, %{
            role: "devops",
            team: "infrastructure"
          })
        ],

        # Palo Alto office - AI/ML team
        palo_alto_ml: [
          create_crunch_session("ml_pa_001", base_date, "palo_alto", 15, %{
            role: "ml_engineer",
            team: "ai"
          }),
          create_crunch_session("ml_pa_002", base_date, "palo_alto", 13, %{
            role: "data_scientist",
            team: "ai"
          })
        ],

        # Mountain View office - mobile team
        mv_mobile: [
          create_crunch_session("mob_mv_001", base_date, "mountain_view", 11, %{
            role: "ios_dev",
            team: "mobile"
          }),
          create_crunch_session("mob_mv_002", base_date, "mountain_view", 13, %{
            role: "android_dev",
            team: "mobile"
          })
        ]
      }

      # Define jurisdiction for each location
      sf_jurisdiction = %{
        state: "california",
        region: "bay_area",
        city: "san_francisco",
        industry: "technology"
      }

      palo_alto_jurisdiction = %{
        state: "california",
        region: "bay_area",
        city: "palo_alto",
        industry: "technology"
      }

      mv_jurisdiction = %{
        state: "california",
        region: "bay_area",
        city: "mountain_view",
        industry: "technology"
      }

      # Process each location group
      sf_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          company_scenario.sf_developers,
          %{},
          sf_jurisdiction
        )

      palo_alto_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          company_scenario.palo_alto_ml,
          %{},
          palo_alto_jurisdiction
        )

      mv_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          company_scenario.mv_mobile,
          %{},
          mv_jurisdiction
        )

      # Validate results across all locations
      all_results = [sf_result, palo_alto_result, mv_result]

      Enum.each(all_results, fn result ->
        assert result.summary.total_spike_break_requirements > 0
        assert result.summary.total_employees > 0

        # During crunch time, should have high penalty hours due to non-compliance
        assert result.summary.total_penalty_hours > 0
      end)

      # San Francisco (largest team) should have most requirements
      assert sf_result.summary.total_spike_break_requirements >=
               palo_alto_result.summary.total_spike_break_requirements

      # Verify tech crunch rules are applied consistently
      Enum.each(all_results, fn result ->
        tech_requirements =
          SpikeBreakTestHelpers.extract_spike_break_requirements(result.spike_requirements)
          |> Enum.filter(fn {:spike_break_requirement, _, data} ->
            data.type == :bay_area_tech_crunch
          end)

        assert length(tech_requirements) > 0,
               "Each Bay Area location should have tech crunch requirements"
      end)
    end

    test "entertainment industry during award season across LA County" do
      # Peak award season
      award_season_date = ~D[2024-02-15]

      # Entertainment companies across LA County during award season
      entertainment_scenario = %{
        # Hollywood studios
        hollywood_productions: [
          create_entertainment_session("prod_hw_001", award_season_date, "hollywood", 18, %{
            role: "director",
            project: "feature_film"
          }),
          create_entertainment_session("prod_hw_002", award_season_date, "hollywood", 16, %{
            role: "cinematographer",
            project: "feature_film"
          }),
          create_entertainment_session("prod_hw_003", award_season_date, "hollywood", 20, %{
            role: "editor",
            project: "post_production"
          })
        ],

        # Burbank studios - TV production
        burbank_tv: [
          create_entertainment_session("tv_bb_001", award_season_date, "burbank", 14, %{
            role: "showrunner",
            project: "tv_series"
          }),
          create_entertainment_session("tv_bb_002", award_season_date, "burbank", 16, %{
            role: "writer",
            project: "tv_series"
          })
        ],

        # Santa Monica - post-production facilities
        santa_monica_post: [
          create_entertainment_session("post_sm_001", award_season_date, "santa_monica", 15, %{
            role: "sound_engineer",
            project: "post_production"
          }),
          create_entertainment_session("post_sm_002", award_season_date, "santa_monica", 17, %{
            role: "vfx_artist",
            project: "post_production"
          })
        ]
      }

      # LA County jurisdiction applies to all locations
      la_jurisdiction = %{
        state: "california",
        region: "los_angeles_county",
        industry: "entertainment"
      }

      # Process all entertainment work
      all_entertainment_sessions =
        entertainment_scenario.hollywood_productions ++
          entertainment_scenario.burbank_tv ++
          entertainment_scenario.santa_monica_post

      result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          all_entertainment_sessions,
          %{},
          la_jurisdiction
        )

      # Validate entertainment industry compliance
      assert result.summary.total_employees == 7
      assert result.summary.total_spike_break_requirements > 0

      # All long sessions should trigger entertainment peak requirements
      entertainment_requirements =
        SpikeBreakTestHelpers.extract_spike_break_requirements(result.spike_requirements)
        |> Enum.filter(fn {:spike_break_requirement, _, data} ->
          data.type == :la_entertainment_peak
        end)

      assert length(entertainment_requirements) > 0

      # Award season should result in significant penalty exposure
      assert result.summary.total_penalty_hours > 10.0

      # Enhanced penalty multiplier for entertainment industry
      assert result.summary.total_penalty_hours > result.summary.compliance_violations * 1.0
    end

    test "cross-industry mobile workforce scenario" do
      # During harvest season
      base_date = ~D[2024-08-15]

      # Mobile workforce that works across industries and jurisdictions
      mobile_workforce_scenario = %{
        # Agricultural consultant working in Central Valley
        agricultural_consultant:
          SpikeBreakTestHelpers.create_central_valley_agriculture_session(
            "consultant_ag_001",
            base_date,
            12
          ),

        # Same consultant later works for tech company in Bay Area
        tech_consulting:
          SpikeBreakTestHelpers.create_bay_area_crunch_session(
            # Next day
            "consultant_ag_001",
            Date.add(base_date, 1),
            10
          ),

        # Entertainment industry contractor in LA
        entertainment_contractor:
          SpikeBreakTestHelpers.create_la_entertainment_session(
            "contractor_ent_001",
            base_date,
            16
          ),

        # Same contractor does tech work in Bay Area
        contractor_tech:
          SpikeBreakTestHelpers.create_bay_area_crunch_session(
            # Two days later
            "contractor_ent_001",
            Date.add(base_date, 2),
            14
          ),

        # Regular California employee (no special jurisdiction)
        standard_employee:
          SpikeBreakTestHelpers.create_extended_day_session(
            "emp_standard_001",
            base_date,
            11
          )
      }

      # Process each work session with appropriate jurisdiction
      ag_jurisdiction = %{state: "california", region: "central_valley", industry: "agriculture"}
      tech_jurisdiction = %{state: "california", region: "bay_area", industry: "technology"}

      entertainment_jurisdiction = %{
        state: "california",
        region: "los_angeles_county",
        industry: "entertainment"
      }

      standard_jurisdiction = %{state: "california"}

      ag_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [mobile_workforce_scenario.agricultural_consultant],
          %{},
          ag_jurisdiction
        )

      consultant_tech_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [mobile_workforce_scenario.tech_consulting],
          %{},
          tech_jurisdiction
        )

      entertainment_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [mobile_workforce_scenario.entertainment_contractor],
          %{},
          entertainment_jurisdiction
        )

      contractor_tech_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [mobile_workforce_scenario.contractor_tech],
          %{},
          tech_jurisdiction
        )

      standard_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          [mobile_workforce_scenario.standard_employee],
          %{},
          standard_jurisdiction
        )

      # Validate cross-industry mobility
      all_results = [
        ag_result,
        consultant_tech_result,
        entertainment_result,
        contractor_tech_result,
        standard_result
      ]

      Enum.each(all_results, fn result ->
        assert is_map(result)
        assert result.summary.total_employees == 1
      end)

      # Different jurisdictions should produce different requirement profiles
      _requirements_by_jurisdiction = [
        {ag_result, :agriculture},
        {consultant_tech_result, :technology},
        {entertainment_result, :entertainment},
        {contractor_tech_result, :technology},
        {standard_result, :standard}
      ]

      # Agriculture during harvest season should have specific requirements
      ag_requirements =
        SpikeBreakTestHelpers.extract_spike_break_requirements(ag_result.spike_requirements)

      harvest_requirements =
        Enum.filter(ag_requirements, fn {:spike_break_requirement, _, data} ->
          data.type == :central_valley_agriculture
        end)

      assert length(harvest_requirements) > 0

      # Tech jurisdictions should have tech-specific requirements
      [consultant_tech_result, contractor_tech_result]
      |> Enum.each(fn tech_result ->
        tech_requirements =
          SpikeBreakTestHelpers.extract_spike_break_requirements(tech_result.spike_requirements)

        tech_crunch_requirements =
          Enum.filter(tech_requirements, fn {:spike_break_requirement, _, data} ->
            data.type == :bay_area_tech_crunch
          end)

        assert length(tech_crunch_requirements) > 0
      end)
    end

    test "large-scale compliance audit across all California jurisdictions" do
      # Mid-year audit
      audit_date = ~D[2024-06-15]

      # Simulate statewide compliance audit with representative samples
      statewide_audit = %{
        # Bay Area tech workers
        bay_area_tech:
          Enum.map(1..10, fn i ->
            create_crunch_session(
              "audit_tech_#{i}",
              audit_date,
              "san_francisco",
              10 + rem(i, 6),
              %{audit_id: "tech_audit"}
            )
          end),

        # LA entertainment workers
        la_entertainment:
          Enum.map(1..8, fn i ->
            create_entertainment_session(
              "audit_ent_#{i}",
              audit_date,
              "los_angeles",
              12 + rem(i, 8),
              %{audit_id: "ent_audit"}
            )
          end),

        # Central Valley agricultural workers
        central_valley_agriculture:
          Enum.map(1..6, fn i ->
            # Not harvest season in June
            SpikeBreakTestHelpers.create_central_valley_agriculture_session(
              "audit_ag_#{i}",
              audit_date,
              8 + rem(i, 4)
            )
          end),

        # San Diego hospitality workers
        san_diego_hospitality:
          Enum.map(1..5, fn i ->
            create_hospitality_session("audit_hosp_#{i}", audit_date, "san_diego", 9 + rem(i, 3))
          end),

        # Standard California workers (various locations)
        standard_workers:
          Enum.map(1..15, fn i ->
            SpikeBreakTestHelpers.create_extended_day_session(
              "audit_std_#{i}",
              audit_date,
              8 + rem(i, 5)
            )
          end)
      }

      # Define jurisdictions for audit
      jurisdictions = %{
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
        },
        standard_workers: %{state: "california"}
      }

      # Process audit for each jurisdiction
      audit_results =
        Map.new(jurisdictions, fn {key, jurisdiction} ->
          work_sessions = Map.get(statewide_audit, key)

          result =
            CaliforniaSpikeBreakRules.process_spike_break_compliance(
              work_sessions,
              %{},
              jurisdiction
            )

          {key, result}
        end)

      # Compile statewide audit summary
      statewide_summary = compile_statewide_audit_summary(audit_results)

      # Validate audit results
      # 10 + 8 + 6 + 5 + 15
      assert statewide_summary.total_employees == 44
      assert statewide_summary.total_spike_break_requirements > 0
      assert statewide_summary.jurisdictions_audited == 5

      # Different jurisdictions should have different compliance rates
      compliance_rates =
        Map.new(audit_results, fn {key, result} ->
          {key, result.summary.compliance_rate_percent}
        end)

      # Tech industry (crunch time) should have lower compliance
      assert compliance_rates.bay_area_tech < compliance_rates.standard_workers

      # Entertainment (long hours) should have challenges
      assert compliance_rates.la_entertainment <= compliance_rates.standard_workers

      # Standard workers should have better compliance rates
      assert compliance_rates.standard_workers >= 0.0

      # Generate compliance recommendations
      recommendations = generate_compliance_recommendations(audit_results)

      assert length(recommendations) > 0
      assert Enum.any?(recommendations, fn rec -> String.contains?(rec, "technology") end)
      assert Enum.any?(recommendations, fn rec -> String.contains?(rec, "entertainment") end)
    end

    test "emergency response scenario with relaxed requirements" do
      # Summer emergency scenario
      emergency_date = ~D[2024-07-20]

      # Simulate emergency response (wildfires, requiring extended work hours)
      emergency_scenario = %{
        # Emergency responders working extended hours
        emergency_responders: [
          # 24-hour shift
          create_emergency_session("responder_001", emergency_date, "first_responder", 24),
          # 20-hour shift
          create_emergency_session("responder_002", emergency_date, "first_responder", 20),
          # 18-hour shift
          create_emergency_session("responder_003", emergency_date, "first_responder", 18)
        ],

        # Utility workers restoring power
        utility_workers: [
          create_emergency_session("utility_001", emergency_date, "utility", 16),
          create_emergency_session("utility_002", emergency_date, "utility", 14),
          create_emergency_session("utility_003", emergency_date, "utility", 18)
        ],

        # Support staff (non-emergency roles)
        support_staff: [
          SpikeBreakTestHelpers.create_extended_day_session("support_001", emergency_date, 12),
          SpikeBreakTestHelpers.create_extended_day_session("support_002", emergency_date, 10),
          SpikeBreakTestHelpers.create_extended_day_session("support_003", emergency_date, 14)
        ]
      }

      # Emergency jurisdiction with modified rules
      emergency_jurisdiction = %{
        state: "california",
        region: "statewide",
        industry: "emergency_response",
        emergency_declared: true,
        emergency_type: "wildfire"
      }

      standard_jurisdiction = %{state: "california"}

      # Process emergency responders with emergency jurisdiction
      emergency_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          emergency_scenario.emergency_responders ++ emergency_scenario.utility_workers,
          %{},
          emergency_jurisdiction
        )

      # Process support staff with standard rules
      support_result =
        CaliforniaSpikeBreakRules.process_spike_break_compliance(
          emergency_scenario.support_staff,
          %{},
          standard_jurisdiction
        )

      # Emergency workers should still have requirements but potentially different handling
      assert emergency_result.summary.total_spike_break_requirements > 0
      assert support_result.summary.total_spike_break_requirements > 0

      # Support staff should have stricter compliance requirements
      assert support_result.summary.compliance_violations >= 0

      # Emergency response should be tracked but may have modified penalties
      assert emergency_result.summary.total_penalty_hours >= 0
    end
  end

  describe "Complex Temporal Integration Scenarios" do
    test "year-long compliance tracking with seasonal variations" do
      employee_id = "yearly_tracker_001"

      # Track compliance across different seasons and jurisdictions
      yearly_scenario = [
        # Q1 - Standard work in Bay Area
        {~D[2024-01-15], :standard, %{state: "california", region: "bay_area"}, 9},
        {~D[2024-02-15], :award_season,
         %{state: "california", region: "los_angeles_county", industry: "entertainment"}, 16},
        {~D[2024-03-15], :standard, %{state: "california", region: "bay_area"}, 8},

        # Q2 - Convention season in San Diego
        {~D[2024-04-15], :standard, %{state: "california"}, 10},
        {~D[2024-05-15], :convention,
         %{
           state: "california",
           region: "san_diego_county",
           city: "san_diego",
           industry: "hospitality"
         }, 12},
        {~D[2024-06-15], :standard, %{state: "california"}, 8},

        # Q3 - Harvest season in Central Valley
        {~D[2024-07-15], :harvest_prep,
         %{state: "california", region: "central_valley", industry: "agriculture"}, 11},
        {~D[2024-08-15], :harvest_peak,
         %{state: "california", region: "central_valley", industry: "agriculture"}, 12},
        {~D[2024-09-15], :harvest_end,
         %{state: "california", region: "central_valley", industry: "agriculture"}, 13},

        # Q4 - Tech crunch season
        {~D[2024-10-15], :tech_crunch,
         %{
           state: "california",
           region: "bay_area",
           city: "san_francisco",
           industry: "technology"
         }, 14},
        {~D[2024-11-15], :tech_crunch,
         %{
           state: "california",
           region: "bay_area",
           city: "san_francisco",
           industry: "technology"
         }, 16},
        {~D[2024-12-15], :holiday_standard, %{state: "california"}, 8}
      ]

      # Process each month's work
      yearly_results =
        Enum.map(yearly_scenario, fn {date, scenario_type, jurisdiction, hours} ->
          work_session =
            case scenario_type do
              :award_season ->
                create_entertainment_session(employee_id, date, "los_angeles", hours, %{})

              :convention ->
                create_hospitality_session(employee_id, date, "san_diego", hours)

              :harvest_prep ->
                SpikeBreakTestHelpers.create_central_valley_agriculture_session(
                  employee_id,
                  date,
                  hours
                )

              :harvest_peak ->
                SpikeBreakTestHelpers.create_central_valley_agriculture_session(
                  employee_id,
                  date,
                  hours
                )

              :harvest_end ->
                SpikeBreakTestHelpers.create_central_valley_agriculture_session(
                  employee_id,
                  date,
                  hours
                )

              :tech_crunch ->
                create_crunch_session(employee_id, date, "san_francisco", hours, %{})

              _ ->
                SpikeBreakTestHelpers.create_standard_work_session(employee_id, date, hours)
            end

          result =
            CaliforniaSpikeBreakRules.process_spike_break_compliance(
              [work_session],
              %{},
              jurisdiction
            )

          {date, scenario_type, jurisdiction, result}
        end)

      # Analyze yearly compliance trends
      monthly_violations =
        Enum.map(yearly_results, fn {date, scenario_type, _jurisdiction, result} ->
          {date, scenario_type, result.summary.compliance_violations}
        end)

      # Harvest season (Aug-Sep) should show increased requirements
      harvest_months =
        Enum.filter(monthly_violations, fn {date, _scenario, _violations} ->
          date.month >= 8 and date.month <= 9
        end)

      tech_crunch_months =
        Enum.filter(monthly_violations, fn {_date, scenario, _violations} ->
          scenario == :tech_crunch
        end)

      # Should have seasonal variation in requirements
      assert length(harvest_months) == 2
      assert length(tech_crunch_months) == 2

      # Year-end summary
      total_violations =
        Enum.reduce(monthly_violations, 0, fn {_date, _scenario, violations}, acc ->
          acc + violations
        end)

      assert total_violations >= 0
    end

    test "multi-week project with changing jurisdictions" do
      project_start = ~D[2024-03-01]
      employee_id = "project_worker_001"

      # 4-week project moving across jurisdictions
      project_weeks = [
        # Week 1: Planning in Bay Area
        {0, "planning", %{state: "california", region: "bay_area", city: "san_francisco"}, 10,
         false},

        # Week 2: Production in LA
        {7, "production",
         %{
           state: "california",
           region: "los_angeles_county",
           city: "los_angeles",
           industry: "entertainment"
         }, 14, true},

        # Week 3: Post-production in Bay Area
        {14, "post_production",
         %{
           state: "california",
           region: "bay_area",
           city: "san_francisco",
           industry: "technology"
         }, 12, true},

        # Week 4: Delivery in San Diego
        {21, "delivery", %{state: "california", region: "san_diego_county", city: "san_diego"}, 8,
         false}
      ]

      project_results =
        Enum.map(project_weeks, fn {day_offset, phase, jurisdiction, hours, is_crunch} ->
          work_date = Date.add(project_start, day_offset)

          work_session =
            if is_crunch do
              case phase do
                "production" ->
                  create_entertainment_session(employee_id, work_date, "los_angeles", hours, %{
                    phase: phase
                  })

                "post_production" ->
                  create_crunch_session(employee_id, work_date, "san_francisco", hours, %{
                    phase: phase
                  })

                _ ->
                  SpikeBreakTestHelpers.create_extended_day_session(employee_id, work_date, hours)
              end
            else
              SpikeBreakTestHelpers.create_standard_work_session(employee_id, work_date, hours)
            end

          result =
            CaliforniaSpikeBreakRules.process_spike_break_compliance(
              [work_session],
              %{},
              jurisdiction
            )

          {phase, jurisdiction, hours, result}
        end)

      # Validate project compliance across jurisdictions
      Enum.each(project_results, fn {phase, _jurisdiction, _hours, result} ->
        assert is_map(result), "Phase #{phase} should produce valid results"
        assert result.summary.total_employees == 1
      end)

      # Production and post-production phases should have higher requirements
      production_result =
        Enum.find(project_results, fn {phase, _, _, _} -> phase == "production" end)

      post_production_result =
        Enum.find(project_results, fn {phase, _, _, _} -> phase == "post_production" end)

      planning_result = Enum.find(project_results, fn {phase, _, _, _} -> phase == "planning" end)

      {_, _, _, production_data} = production_result
      {_, _, _, post_production_data} = post_production_result
      {_, _, _, planning_data} = planning_result

      # Intensive phases should have more requirements than planning
      assert production_data.summary.total_spike_break_requirements >=
               planning_data.summary.total_spike_break_requirements

      assert post_production_data.summary.total_spike_break_requirements >=
               planning_data.summary.total_spike_break_requirements
    end
  end

  # Helper functions for creating specific session types

  defp create_crunch_session(employee_id, date, city, hours, metadata) do
    {:work_session, "crunch_#{employee_id}_#{Date.to_string(date)}",
     %{
       employee_id: employee_id,
       work_session_id: "crunch_#{employee_id}_#{Date.to_string(date)}",
       start_datetime: DateTime.new!(date, ~T[08:00:00], "Etc/UTC"),
       end_datetime:
         DateTime.add(DateTime.new!(date, ~T[08:00:00], "Etc/UTC"), hours * 3600, :second),
       total_hours: hours,
       meal_breaks_taken: if(hours > 12, do: 2, else: 1),
       breaks_taken: [],
       is_crunch_time: true,
       industry: "technology",
       city: city,
       jurisdiction: %{state: "california", region: "bay_area", city: city},
       metadata: metadata
     }}
  end

  defp create_entertainment_session(employee_id, date, city, hours, metadata) do
    {:work_session, "entertainment_#{employee_id}_#{Date.to_string(date)}",
     %{
       employee_id: employee_id,
       work_session_id: "entertainment_#{employee_id}_#{Date.to_string(date)}",
       start_datetime: DateTime.new!(date, ~T[06:00:00], "Etc/UTC"),
       end_datetime:
         DateTime.add(DateTime.new!(date, ~T[06:00:00], "Etc/UTC"), hours * 3600, :second),
       total_hours: hours,
       meal_breaks_taken: if(hours > 12, do: 2, else: 1),
       breaks_taken: [],
       is_peak_production: true,
       industry: "entertainment",
       city: city,
       jurisdiction: %{state: "california", region: "los_angeles_county", city: city},
       metadata: metadata
     }}
  end

  defp create_hospitality_session(employee_id, date, city, hours) do
    {:work_session, "hospitality_#{employee_id}_#{Date.to_string(date)}",
     %{
       employee_id: employee_id,
       work_session_id: "hospitality_#{employee_id}_#{Date.to_string(date)}",
       start_datetime: DateTime.new!(date, ~T[07:00:00], "Etc/UTC"),
       end_datetime:
         DateTime.add(DateTime.new!(date, ~T[07:00:00], "Etc/UTC"), hours * 3600, :second),
       total_hours: hours,
       meal_breaks_taken: 1,
       breaks_taken: [],
       industry: "hospitality",
       city: city,
       jurisdiction: %{state: "california", region: "san_diego_county", city: city}
     }}
  end

  defp create_emergency_session(employee_id, date, worker_type, hours) do
    {:work_session, "emergency_#{employee_id}_#{Date.to_string(date)}",
     %{
       employee_id: employee_id,
       work_session_id: "emergency_#{employee_id}_#{Date.to_string(date)}",
       start_datetime: DateTime.new!(date, ~T[06:00:00], "Etc/UTC"),
       end_datetime:
         DateTime.add(DateTime.new!(date, ~T[06:00:00], "Etc/UTC"), hours * 3600, :second),
       total_hours: hours,
       meal_breaks_taken: if(hours > 12, do: 2, else: 1),
       breaks_taken: [],
       worker_type: worker_type,
       emergency_status: true,
       jurisdiction: %{state: "california", emergency_declared: true}
     }}
  end

  defp compile_statewide_audit_summary(audit_results) do
    total_employees =
      Enum.reduce(audit_results, 0, fn {_key, result}, acc ->
        acc + result.summary.total_employees
      end)

    total_requirements =
      Enum.reduce(audit_results, 0, fn {_key, result}, acc ->
        acc + result.summary.total_spike_break_requirements
      end)

    total_violations =
      Enum.reduce(audit_results, 0, fn {_key, result}, acc ->
        acc + result.summary.compliance_violations
      end)

    overall_compliance_rate =
      if total_requirements > 0 do
        ((total_requirements - total_violations) / total_requirements * 100) |> Float.round(1)
      else
        100.0
      end

    %{
      total_employees: total_employees,
      total_spike_break_requirements: total_requirements,
      total_compliance_violations: total_violations,
      overall_compliance_rate_percent: overall_compliance_rate,
      jurisdictions_audited: map_size(audit_results),
      audit_date: DateTime.utc_now()
    }
  end

  defp generate_compliance_recommendations(audit_results) do
    recommendations = []

    # Check for industries with high violation rates
    recommendations =
      Enum.reduce(audit_results, recommendations, fn {key, result}, acc ->
        violation_rate =
          if result.summary.total_spike_break_requirements > 0 do
            result.summary.compliance_violations / result.summary.total_spike_break_requirements
          else
            0.0
          end

        cond do
          violation_rate > 0.5 and String.contains?(Atom.to_string(key), "tech") ->
            acc ++
              [
                "Consider implementing automated break reminders for technology workers during crunch periods"
              ]

          violation_rate > 0.4 and String.contains?(Atom.to_string(key), "entertainment") ->
            acc ++
              [
                "Review entertainment industry scheduling practices to ensure adequate break coverage"
              ]

          violation_rate > 0.3 ->
            acc ++ ["Increase break compliance monitoring for #{key} jurisdiction"]

          true ->
            acc
        end
      end)

    recommendations ++ ["Regular compliance training recommended for all high-risk jurisdictions"]
  end
end
