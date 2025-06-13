defmodule Presto.AlphaNetworkTest do
  use ExUnit.Case, async: true

  alias Presto.AlphaNetwork
  alias Presto.WorkingMemory

  setup do
    {:ok, wm} = WorkingMemory.start_link([])
    {:ok, alpha} = AlphaNetwork.start_link(working_memory: wm)
    %{alpha_network: alpha, working_memory: wm}
  end

  describe "alpha node creation and management" do
    test "can create alpha nodes for simple conditions", %{alpha_network: alpha} do
      # Create alpha node for {:person, _, age} where age > 18
      condition = {:person, :_, :age, [{:age, :>, 18}]}

      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      assert is_binary(node_id)

      nodes = AlphaNetwork.get_alpha_nodes(alpha)
      assert Map.has_key?(nodes, node_id)
    end

    test "can create multiple alpha nodes", %{alpha_network: alpha} do
      conditions = [
        {:person, :_, :age, [{:age, :>, 18}]},
        {:person, :name, :_, [{:name, :==, "John"}]},
        {:company, :_, :industry, [{:industry, :==, "Tech"}]}
      ]

      node_ids =
        for condition <- conditions do
          {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)
          node_id
        end

      assert length(node_ids) == 3
      # All unique
      assert length(Enum.uniq(node_ids)) == 3

      nodes = AlphaNetwork.get_alpha_nodes(alpha)
      assert map_size(nodes) == 3
    end

    test "can remove alpha nodes", %{alpha_network: alpha} do
      condition = {:person, :_, :age, [{:age, :>, 18}]}
      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      assert Map.has_key?(AlphaNetwork.get_alpha_nodes(alpha), node_id)

      :ok = AlphaNetwork.remove_alpha_node(alpha, node_id)

      refute Map.has_key?(AlphaNetwork.get_alpha_nodes(alpha), node_id)
    end
  end

  describe "fact filtering through alpha network" do
    test "filters facts based on simple conditions", %{alpha_network: alpha, working_memory: wm} do
      # Create alpha node for adults (age > 18)
      condition = {:person, :name, :age, [{:age, :>, 18}]}
      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      # Add facts to working memory
      WorkingMemory.assert_fact(wm, {:person, "John", 25})
      WorkingMemory.assert_fact(wm, {:person, "Jane", 16})
      WorkingMemory.assert_fact(wm, {:person, "Bob", 30})

      # Process facts through alpha network
      AlphaNetwork.process_fact_assertion(alpha, {:person, "John", 25})
      AlphaNetwork.process_fact_assertion(alpha, {:person, "Jane", 16})
      AlphaNetwork.process_fact_assertion(alpha, {:person, "Bob", 30})

      # Get matching facts from alpha node
      matches = AlphaNetwork.get_alpha_memory(alpha, node_id)

      # Only John and Bob should match (age > 18)
      assert length(matches) == 2
      assert %{name: "John", age: 25} in matches
      assert %{name: "Bob", age: 30} in matches
      refute %{name: "Jane", age: 16} in matches
    end

    test "handles multiple conditions", %{alpha_network: alpha, working_memory: wm} do
      # Create alpha node for senior tech workers (age > 30 AND industry == "Tech")
      condition =
        {:employee, :name, :age, :industry,
         [
           {:age, :>, 30},
           {:industry, :==, "Tech"}
         ]}

      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      # Add facts
      facts = [
        {:employee, "Alice", 35, "Tech"},
        {:employee, "Bob", 25, "Tech"},
        {:employee, "Carol", 40, "Finance"},
        {:employee, "Dave", 45, "Tech"}
      ]

      for fact <- facts do
        WorkingMemory.assert_fact(wm, fact)
        AlphaNetwork.process_fact_assertion(alpha, fact)
      end

      matches = AlphaNetwork.get_alpha_memory(alpha, node_id)

      # Only Alice and Dave match both conditions
      assert length(matches) == 2
      assert %{name: "Alice", age: 35, industry: "Tech"} in matches
      assert %{name: "Dave", age: 45, industry: "Tech"} in matches
    end

    test "handles fact retraction", %{alpha_network: alpha, working_memory: wm} do
      condition = {:person, :name, :age, [{:age, :>, 18}]}
      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      fact = {:person, "John", 25}
      WorkingMemory.assert_fact(wm, fact)
      AlphaNetwork.process_fact_assertion(alpha, fact)

      # Fact should be in alpha memory
      matches = AlphaNetwork.get_alpha_memory(alpha, node_id)
      assert [%{name: "John", age: 25}] = matches

      # Retract fact
      WorkingMemory.retract_fact(wm, fact)
      AlphaNetwork.process_fact_retraction(alpha, fact)

      # Fact should be removed from alpha memory
      matches = AlphaNetwork.get_alpha_memory(alpha, node_id)
      assert [] = matches
    end

    test "handles different comparison operators", %{alpha_network: alpha, working_memory: _wm} do
      conditions_and_expected = [
        # Greater than
        {[{:age, :>, 25}], [30, 35], [20, 25]},
        # Less than
        {[{:age, :<, 30}], [20, 25], [30, 35]},
        # Equal
        {[{:age, :==, 25}], [25], [20, 30, 35]},
        # Not equal
        {[{:age, :!=, 25}], [20, 30, 35], [25]},
        # Greater than or equal
        {[{:age, :>=, 25}], [25, 30, 35], [20]},
        # Less than or equal
        {[{:age, :<=, 25}], [20, 25], [30, 35]}
      ]

      for {conditions, should_match, should_not_match} <- conditions_and_expected do
        # Create fresh alpha node for each test
        {:ok, node_id} =
          AlphaNetwork.create_alpha_node(
            alpha,
            {:person, :name, :age, conditions}
          )

        # Test facts that should match
        for age <- should_match do
          fact = {:person, "Person#{age}", age}
          AlphaNetwork.process_fact_assertion(alpha, fact)
        end

        # Test facts that should not match
        for age <- should_not_match do
          fact = {:person, "Person#{age}", age}
          AlphaNetwork.process_fact_assertion(alpha, fact)
        end

        matches = AlphaNetwork.get_alpha_memory(alpha, node_id)
        matched_ages = for %{age: age} <- matches, do: age

        assert Enum.sort(matched_ages) == Enum.sort(should_match)

        # Clean up for next test
        AlphaNetwork.remove_alpha_node(alpha, node_id)
      end
    end
  end

  describe "pattern matching efficiency" do
    test "uses compiled patterns for fast matching", %{alpha_network: alpha} do
      # This test verifies that alpha nodes compile patterns for efficiency
      condition = {:person, :name, :age, [{:age, :>, 18}]}
      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      node_info = AlphaNetwork.get_alpha_node_info(alpha, node_id)

      # Should have compiled pattern and test function
      assert Map.has_key?(node_info, :pattern)
      assert Map.has_key?(node_info, :test_function)
      assert is_function(node_info.test_function)
    end

    test "efficient processing of large fact sets" do
      {:ok, wm} = WorkingMemory.start_link([])
      {:ok, alpha} = AlphaNetwork.start_link(working_memory: wm)

      # Create alpha node
      condition = {:number, :value, [{:value, :>, 5000}]}
      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      # Process 10,000 facts
      {process_time, _} =
        :timer.tc(fn ->
          for i <- 1..10_000 do
            fact = {:number, i}
            AlphaNetwork.process_fact_assertion(alpha, fact)
          end
        end)

      matches = AlphaNetwork.get_alpha_memory(alpha, node_id)

      # Should match numbers > 5000 (5001-10000 = 5000 matches)
      assert length(matches) == 5000

      # Processing should be reasonably fast (< 1 second)
      # microseconds
      assert process_time < 1_000_000
    end
  end

  describe "variable binding" do
    test "extracts and binds variables from matching facts", %{alpha_network: alpha} do
      condition =
        {:person, :name, :age, :city,
         [
           {:age, :>, 25},
           {:city, :==, "NYC"}
         ]}

      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      facts = [
        {:person, "Alice", 30, "NYC"},
        # age too low
        {:person, "Bob", 20, "NYC"},
        # wrong city
        {:person, "Carol", 35, "LA"},
        {:person, "Dave", 40, "NYC"}
      ]

      for fact <- facts do
        AlphaNetwork.process_fact_assertion(alpha, fact)
      end

      matches = AlphaNetwork.get_alpha_memory(alpha, node_id)

      # Should have bound variables for Alice and Dave
      assert length(matches) == 2

      alice_match = Enum.find(matches, fn m -> m.name == "Alice" end)
      dave_match = Enum.find(matches, fn m -> m.name == "Dave" end)

      assert alice_match == %{name: "Alice", age: 30, city: "NYC"}
      assert dave_match == %{name: "Dave", age: 40, city: "NYC"}
    end

    test "handles anonymous variables correctly", %{alpha_network: alpha} do
      # Pattern with anonymous variable for middle field
      condition = {:transaction, :_, :amount, [{:amount, :>, 1000}]}
      {:ok, node_id} = AlphaNetwork.create_alpha_node(alpha, condition)

      facts = [
        {:transaction, "tx1", 1500},
        # amount too low
        {:transaction, "tx2", 500},
        {:transaction, "tx3", 2000}
      ]

      for fact <- facts do
        AlphaNetwork.process_fact_assertion(alpha, fact)
      end

      matches = AlphaNetwork.get_alpha_memory(alpha, node_id)

      # Should match transactions with amount > 1000
      # Anonymous variable should not be in bindings
      assert length(matches) == 2

      amounts = for %{amount: amount} <- matches, do: amount
      assert Enum.sort(amounts) == [1500, 2000]

      # Should not have binding for anonymous variable
      for match <- matches do
        refute Map.has_key?(match, :_)
      end
    end
  end
end
