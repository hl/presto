defmodule Presto.BetaNetworkTest do
  use ExUnit.Case, async: true

  alias Presto.AlphaNetwork
  alias Presto.BetaNetwork
  alias Presto.WorkingMemory

  setup do
    {:ok, wm} = WorkingMemory.start_link([])
    {:ok, alpha} = AlphaNetwork.start_link(working_memory: wm)
    {:ok, beta} = BetaNetwork.start_link(alpha_network: alpha)

    %{
      working_memory: wm,
      alpha_network: alpha,
      beta_network: beta
    }
  end

  describe "beta node creation and management" do
    test "can create beta nodes for joining two alpha nodes", ctx do
      # Create two alpha nodes to join
      {:ok, alpha1} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, [{:age, :>, 18}]}
        )

      {:ok, alpha2} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:employment, :name, :company, []}
        )

      # Create beta node to join them on :name
      join_condition = {:join, alpha1, alpha2, :name}
      {:ok, beta_id} = BetaNetwork.create_beta_node(ctx.beta_network, join_condition)

      assert is_binary(beta_id)

      nodes = BetaNetwork.get_beta_nodes(ctx.beta_network)
      assert Map.has_key?(nodes, beta_id)
    end

    test "can create beta nodes with multiple join conditions", ctx do
      {:ok, alpha1} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, :city, []}
        )

      {:ok, alpha2} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:job, :name, :city, :salary, []}
        )

      # Join on both name and city
      join_condition = {:join, alpha1, alpha2, [:name, :city]}
      {:ok, beta_id} = BetaNetwork.create_beta_node(ctx.beta_network, join_condition)

      node_info = BetaNetwork.get_beta_node_info(ctx.beta_network, beta_id)
      assert node_info.join_keys == [:name, :city]
    end

    test "can create nested beta nodes", ctx do
      # Create alpha nodes
      {:ok, alpha1} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, []}
        )

      {:ok, alpha2} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:employment, :name, :company, []}
        )

      {:ok, alpha3} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:salary, :name, :amount, []}
        )

      # Create first join (person + employment)
      {:ok, beta1} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, alpha1, alpha2, :name}
        )

      # Create second join (previous result + salary)
      {:ok, beta2} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, beta1, alpha3, :name}
        )

      nodes = BetaNetwork.get_beta_nodes(ctx.beta_network)
      assert Map.has_key?(nodes, beta1)
      assert Map.has_key?(nodes, beta2)
    end

    test "can remove beta nodes", ctx do
      {:ok, alpha1} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, []}
        )

      {:ok, alpha2} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:employment, :name, :company, []}
        )

      {:ok, beta_id} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, alpha1, alpha2, :name}
        )

      assert Map.has_key?(BetaNetwork.get_beta_nodes(ctx.beta_network), beta_id)

      :ok = BetaNetwork.remove_beta_node(ctx.beta_network, beta_id)

      refute Map.has_key?(BetaNetwork.get_beta_nodes(ctx.beta_network), beta_id)
    end
  end

  describe "fact joining" do
    test "joins facts from two alpha nodes", ctx do
      # Set up alpha nodes
      {:ok, person_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, [{:age, :>, 18}]}
        )

      {:ok, employment_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:employment, :name, :company, []}
        )

      # Set up beta node
      {:ok, beta_id} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, person_alpha, employment_alpha, :name}
        )

      # Add facts to working memory and process through alpha network
      person_facts = [
        {:person, "Alice", 25},
        {:person, "Bob", 30},
        # Too young, filtered by alpha
        {:person, "Charlie", 16}
      ]

      employment_facts = [
        {:employment, "Alice", "TechCorp"},
        {:employment, "Bob", "DataInc"},
        # No matching person
        {:employment, "Diana", "StartupCo"}
      ]

      for fact <- person_facts do
        WorkingMemory.assert_fact(ctx.working_memory, fact)
        AlphaNetwork.process_fact_assertion(ctx.alpha_network, fact)
      end

      for fact <- employment_facts do
        WorkingMemory.assert_fact(ctx.working_memory, fact)
        AlphaNetwork.process_fact_assertion(ctx.alpha_network, fact)
      end

      # Process joins
      BetaNetwork.process_alpha_changes(ctx.beta_network)

      # Get join results
      joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta_id)

      # Should have 2 joins: Alice and Bob (Charlie filtered, Diana unmatched)
      assert length(joins) == 2

      alice_join = Enum.find(joins, fn j -> j.name == "Alice" end)
      bob_join = Enum.find(joins, fn j -> j.name == "Bob" end)

      assert alice_join.age == 25
      assert alice_join.company == "TechCorp"
      assert bob_join.age == 30
      assert bob_join.company == "DataInc"
    end

    test "handles multiple join keys", ctx do
      {:ok, person_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :city, :age, []}
        )

      {:ok, job_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:job, :name, :city, :title, []}
        )

      {:ok, beta_id} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, person_alpha, job_alpha, [:name, :city]}
        )

      person_facts = [
        {:person, "Alice", "NYC", 25},
        {:person, "Bob", "SF", 30},
        # Different Alice in SF
        {:person, "Alice", "SF", 28}
      ]

      job_facts = [
        {:job, "Alice", "NYC", "Engineer"},
        {:job, "Alice", "SF", "Manager"},
        # Wrong city for Bob
        {:job, "Bob", "NYC", "Designer"}
      ]

      for fact <- person_facts ++ job_facts do
        WorkingMemory.assert_fact(ctx.working_memory, fact)
        AlphaNetwork.process_fact_assertion(ctx.alpha_network, fact)
      end

      BetaNetwork.process_alpha_changes(ctx.beta_network)

      joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta_id)

      # Should have 2 joins:
      # - Alice in NYC (Engineer)
      # - Alice in SF (Manager)
      # Bob in SF doesn't match Bob in NYC job
      assert length(joins) == 2

      nyc_alice = Enum.find(joins, fn j -> j.city == "NYC" end)
      sf_alice = Enum.find(joins, fn j -> j.city == "SF" end)

      assert nyc_alice.name == "Alice"
      assert nyc_alice.title == "Engineer"
      assert sf_alice.name == "Alice"
      assert sf_alice.title == "Manager"
    end

    test "handles fact retraction in joins", ctx do
      {:ok, person_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, []}
        )

      {:ok, employment_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:employment, :name, :company, []}
        )

      {:ok, beta_id} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, person_alpha, employment_alpha, :name}
        )

      # Add facts
      person_fact = {:person, "Alice", 25}
      employment_fact = {:employment, "Alice", "TechCorp"}

      WorkingMemory.assert_fact(ctx.working_memory, person_fact)
      WorkingMemory.assert_fact(ctx.working_memory, employment_fact)
      AlphaNetwork.process_fact_assertion(ctx.alpha_network, person_fact)
      AlphaNetwork.process_fact_assertion(ctx.alpha_network, employment_fact)

      BetaNetwork.process_alpha_changes(ctx.beta_network)

      # Should have one join
      joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta_id)
      assert length(joins) == 1

      # Retract person fact
      WorkingMemory.retract_fact(ctx.working_memory, person_fact)
      AlphaNetwork.process_fact_retraction(ctx.alpha_network, person_fact)
      BetaNetwork.process_alpha_changes(ctx.beta_network)

      # Join should be removed
      joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta_id)
      assert Enum.empty?(joins)
    end
  end

  describe "nested joins" do
    test "can join results from beta nodes with alpha nodes", ctx do
      # Create alpha nodes
      {:ok, person_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, []}
        )

      {:ok, employment_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:employment, :name, :company, []}
        )

      {:ok, salary_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:salary, :name, :amount, [{:amount, :>, 50_000}]}
        )

      # Create first beta node (person + employment)
      {:ok, beta1} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, person_alpha, employment_alpha, :name}
        )

      # Create second beta node (previous result + salary)
      {:ok, beta2} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, beta1, salary_alpha, :name}
        )

      # Add facts
      facts = [
        {:person, "Alice", 30},
        {:person, "Bob", 25},
        {:employment, "Alice", "TechCorp"},
        {:employment, "Bob", "StartupCo"},
        {:salary, "Alice", 75_000},
        # Below threshold
        {:salary, "Bob", 45_000}
      ]

      for fact <- facts do
        WorkingMemory.assert_fact(ctx.working_memory, fact)
        AlphaNetwork.process_fact_assertion(ctx.alpha_network, fact)
      end

      BetaNetwork.process_alpha_changes(ctx.beta_network)

      # First beta should have both Alice and Bob
      beta1_joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta1)
      assert length(beta1_joins) == 2

      # Second beta should only have Alice (Bob filtered by salary requirement)
      beta2_joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta2)
      assert length(beta2_joins) == 1

      alice_result = hd(beta2_joins)
      assert alice_result.name == "Alice"
      assert alice_result.age == 30
      assert alice_result.company == "TechCorp"
      assert alice_result.amount == 75_000
    end

    test "can join two beta node results", ctx do
      # Set up alpha nodes
      {:ok, person_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :department, []}
        )

      {:ok, skill_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:skill, :name, :technology, []}
        )

      {:ok, project_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:project, :department, :technology, :budget, []}
        )

      {:ok, assignment_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:assignment, :department, :budget, []}
        )

      # Create first beta: person + skill (by name)
      {:ok, beta1} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, person_alpha, skill_alpha, :name}
        )

      # Create second beta: project + assignment (by department)
      {:ok, beta2} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, project_alpha, assignment_alpha, :department}
        )

      # Create third beta: join beta1 and beta2 results
      {:ok, beta3} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, beta1, beta2, [:department, :technology]}
        )

      # Add facts
      facts = [
        {:person, "Alice", "Engineering"},
        {:skill, "Alice", "Elixir"},
        {:project, "Engineering", "Elixir", 100_000},
        {:assignment, "Engineering", 100_000}
      ]

      for fact <- facts do
        WorkingMemory.assert_fact(ctx.working_memory, fact)
        AlphaNetwork.process_fact_assertion(ctx.alpha_network, fact)
      end

      BetaNetwork.process_alpha_changes(ctx.beta_network)

      # Final result should combine all information
      final_joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta3)
      assert length(final_joins) == 1

      result = hd(final_joins)
      assert result.name == "Alice"
      assert result.department == "Engineering"
      assert result.technology == "Elixir"
      assert result.budget == 100_000
    end
  end

  describe "partial match storage" do
    test "stores partial matches efficiently", ctx do
      {:ok, person_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, []}
        )

      {:ok, employment_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:employment, :name, :company, []}
        )

      {:ok, beta_id} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, person_alpha, employment_alpha, :name}
        )

      # Add only person facts (no employment facts yet)
      person_facts = [
        {:person, "Alice", 25},
        {:person, "Bob", 30}
      ]

      for fact <- person_facts do
        WorkingMemory.assert_fact(ctx.working_memory, fact)
        AlphaNetwork.process_fact_assertion(ctx.alpha_network, fact)
      end

      BetaNetwork.process_alpha_changes(ctx.beta_network)

      # Should have partial matches stored but no complete joins yet
      joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta_id)
      assert Enum.empty?(joins)

      partial_matches = BetaNetwork.get_partial_matches(ctx.beta_network, beta_id)
      # Alice and Bob waiting for employment
      assert length(partial_matches) == 2

      # Now add employment fact for Alice
      employment_fact = {:employment, "Alice", "TechCorp"}
      WorkingMemory.assert_fact(ctx.working_memory, employment_fact)
      AlphaNetwork.process_fact_assertion(ctx.alpha_network, employment_fact)
      BetaNetwork.process_alpha_changes(ctx.beta_network)

      # Should now have one complete join
      joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta_id)
      assert length(joins) == 1
      assert hd(joins).name == "Alice"
    end

    test "handles incremental updates efficiently", ctx do
      {:ok, person_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :age, []}
        )

      {:ok, employment_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:employment, :name, :company, []}
        )

      {:ok, beta_id} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, person_alpha, employment_alpha, :name}
        )

      # Track processing calls to verify incremental behavior
      initial_process_count = BetaNetwork.get_process_count(ctx.beta_network)

      # Add facts one by one
      WorkingMemory.assert_fact(ctx.working_memory, {:person, "Alice", 25})
      AlphaNetwork.process_fact_assertion(ctx.alpha_network, {:person, "Alice", 25})
      BetaNetwork.process_alpha_changes(ctx.beta_network)

      first_process_count = BetaNetwork.get_process_count(ctx.beta_network)

      WorkingMemory.assert_fact(ctx.working_memory, {:employment, "Alice", "TechCorp"})
      AlphaNetwork.process_fact_assertion(ctx.alpha_network, {:employment, "Alice", "TechCorp"})
      BetaNetwork.process_alpha_changes(ctx.beta_network)

      second_process_count = BetaNetwork.get_process_count(ctx.beta_network)

      # Should show incremental processing (not reprocessing everything)
      assert first_process_count > initial_process_count
      assert second_process_count > first_process_count

      # But total processing should be less than naive O(n²) approach
      joins = BetaNetwork.get_beta_memory(ctx.beta_network, beta_id)
      assert length(joins) == 1
    end
  end

  describe "performance characteristics" do
    test "demonstrates linear scaling with RETE algorithm", ctx do
      # Create a scenario that would be O(n²) with naive approach
      # but O(n) with RETE due to incremental processing

      {:ok, person_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:person, :name, :department, []}
        )

      {:ok, project_alpha} =
        AlphaNetwork.create_alpha_node(
          ctx.alpha_network,
          {:project, :department, :budget, []}
        )

      {:ok, beta_id} =
        BetaNetwork.create_beta_node(
          ctx.beta_network,
          {:join, person_alpha, project_alpha, :department}
        )

      # Measure time for different data sizes
      sizes = [100, 500, 1000]

      times =
        for size <- sizes do
          # Clear previous data
          BetaNetwork.clear_beta_memory(ctx.beta_network, beta_id)

          # Add facts
          {time, _} =
            :timer.tc(fn ->
              for i <- 1..size do
                # 10 departments
                dept = "dept_#{rem(i, 10)}"
                WorkingMemory.assert_fact(ctx.working_memory, {:person, "person_#{i}", dept})
                WorkingMemory.assert_fact(ctx.working_memory, {:project, dept, i * 1000})

                AlphaNetwork.process_fact_assertion(
                  ctx.alpha_network,
                  {:person, "person_#{i}", dept}
                )

                AlphaNetwork.process_fact_assertion(ctx.alpha_network, {:project, dept, i * 1000})
              end

              BetaNetwork.process_alpha_changes(ctx.beta_network)
            end)

          time
        end

      [time1, _time2, time3] = times

      # With RETE, scaling should be much better than quadratic
      # This is a qualitative test - exact ratios depend on implementation
      # but growth should be sub-quadratic
      growth_ratio = time3 / time1
      # 100x for quadratic
      quadratic_growth = 1000 / 100 * (1000 / 100)

      # RETE should scale much better than quadratic
      assert growth_ratio < quadratic_growth / 2
    end
  end
end
