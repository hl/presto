defmodule Presto.WorkingMemoryTest do
  use ExUnit.Case, async: true

  alias Presto.WorkingMemory

  setup do
    {:ok, pid} = WorkingMemory.start_link([])
    %{working_memory: pid}
  end

  describe "fact storage" do
    test "can store and retrieve facts", %{working_memory: wm} do
      fact = {:person, "John", 25}

      :ok = WorkingMemory.assert_fact(wm, fact)

      facts = WorkingMemory.get_facts(wm)
      assert fact in facts
    end

    test "can store multiple facts", %{working_memory: wm} do
      facts = [
        {:person, "John", 25},
        {:person, "Jane", 30},
        {:company, "Acme", "Tech"}
      ]

      for fact <- facts do
        WorkingMemory.assert_fact(wm, fact)
      end

      stored_facts = WorkingMemory.get_facts(wm)

      for fact <- facts do
        assert fact in stored_facts
      end
    end

    test "can retract facts", %{working_memory: wm} do
      fact = {:person, "John", 25}

      WorkingMemory.assert_fact(wm, fact)
      assert fact in WorkingMemory.get_facts(wm)

      :ok = WorkingMemory.retract_fact(wm, fact)
      refute fact in WorkingMemory.get_facts(wm)
    end

    test "retracting non-existent fact is safe", %{working_memory: wm} do
      fact = {:person, "NonExistent", 99}

      :ok = WorkingMemory.retract_fact(wm, fact)
      assert [] = WorkingMemory.get_facts(wm)
    end

    test "can clear all facts", %{working_memory: wm} do
      facts = [
        {:person, "John", 25},
        {:person, "Jane", 30}
      ]

      for fact <- facts do
        WorkingMemory.assert_fact(wm, fact)
      end

      assert length(WorkingMemory.get_facts(wm)) == 2

      :ok = WorkingMemory.clear_facts(wm)
      assert [] = WorkingMemory.get_facts(wm)
    end
  end

  describe "pattern matching" do
    test "can find facts by pattern", %{working_memory: wm} do
      WorkingMemory.assert_fact(wm, {:person, "John", 25})
      WorkingMemory.assert_fact(wm, {:person, "Jane", 30})
      WorkingMemory.assert_fact(wm, {:company, "Acme", "Tech"})

      # Find all person facts
      person_facts = WorkingMemory.match_facts(wm, {:person, :_, :_})
      assert length(person_facts) == 2
      assert {:person, "John", 25} in person_facts
      assert {:person, "Jane", 30} in person_facts

      # Find specific person
      john_facts = WorkingMemory.match_facts(wm, {:person, "John", :_})
      assert [{:person, "John", 25}] = john_facts
    end

    test "can match with variable binding", %{working_memory: wm} do
      WorkingMemory.assert_fact(wm, {:person, "John", 25})
      WorkingMemory.assert_fact(wm, {:employment, "John", "Acme"})

      # Match person facts and extract bound variables
      matches = WorkingMemory.match_facts_with_bindings(wm, {:person, :name, :age})

      assert [%{name: "John", age: 25}] = matches
    end

    test "returns empty list when no matches", %{working_memory: wm} do
      WorkingMemory.assert_fact(wm, {:person, "John", 25})

      matches = WorkingMemory.match_facts(wm, {:company, :_, :_})
      assert [] = matches
    end
  end

  describe "concurrent access" do
    test "supports concurrent reads", %{working_memory: wm} do
      # Add some facts
      for i <- 1..100 do
        WorkingMemory.assert_fact(wm, {:number, i})
      end

      # Spawn multiple readers
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            facts = WorkingMemory.get_facts(wm)
            length(facts)
          end)
        end

      results = Task.await_many(tasks)

      # All readers should see the same number of facts
      assert Enum.all?(results, fn count -> count == 100 end)
    end

    test "handles concurrent writes safely", %{working_memory: wm} do
      # Spawn multiple writers
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            WorkingMemory.assert_fact(wm, {:number, i})
          end)
        end

      Task.await_many(tasks)

      facts = WorkingMemory.get_facts(wm)
      assert length(facts) == 50

      # All numbers 1-50 should be present
      numbers = for {:number, n} <- facts, do: n
      assert Enum.sort(numbers) == Enum.to_list(1..50)
    end
  end

  describe "change tracking" do
    test "tracks fact assertions", %{working_memory: wm} do
      WorkingMemory.start_tracking_changes(wm)

      fact = {:person, "John", 25}
      WorkingMemory.assert_fact(wm, fact)

      changes = WorkingMemory.get_changes(wm)
      assert [{:assert, ^fact}] = changes
    end

    test "tracks fact retractions", %{working_memory: wm} do
      fact = {:person, "John", 25}
      WorkingMemory.assert_fact(wm, fact)

      WorkingMemory.start_tracking_changes(wm)
      WorkingMemory.retract_fact(wm, fact)

      changes = WorkingMemory.get_changes(wm)
      assert [{:retract, ^fact}] = changes
    end

    test "can clear change tracking", %{working_memory: wm} do
      WorkingMemory.start_tracking_changes(wm)

      WorkingMemory.assert_fact(wm, {:person, "John", 25})
      assert length(WorkingMemory.get_changes(wm)) == 1

      WorkingMemory.clear_changes(wm)
      assert [] = WorkingMemory.get_changes(wm)
    end
  end

  describe "performance" do
    test "efficient storage and retrieval with large fact sets" do
      {:ok, wm} = WorkingMemory.start_link([])

      # Add 10,000 facts
      {assert_time, _} =
        :timer.tc(fn ->
          for i <- 1..10_000 do
            WorkingMemory.assert_fact(wm, {:number, i})
          end
        end)

      # Retrieve all facts
      {retrieve_time, facts} =
        :timer.tc(fn ->
          WorkingMemory.get_facts(wm)
        end)

      assert length(facts) == 10_000

      # Operations should be reasonably fast (< 1 second each)
      # microseconds
      assert assert_time < 1_000_000
      # microseconds
      assert retrieve_time < 100_000

      # Pattern matching should also be fast
      {match_time, matches} =
        :timer.tc(fn ->
          WorkingMemory.match_facts(wm, {:number, :_})
        end)

      assert length(matches) == 10_000
      # microseconds
      assert match_time < 100_000
    end
  end
end
