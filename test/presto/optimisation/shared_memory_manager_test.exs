defmodule Presto.Optimisation.SharedMemoryManagerTest do
  use ExUnit.Case, async: false

  alias Presto.Optimisation.SharedMemoryManager

  setup do
    manager = start_supervised!(SharedMemoryManager)
    {:ok, manager: manager}
  end

  describe "shared memory creation and retrieval" do
    test "creates and retrieves shared memory", %{manager: _manager} do
      memory_key = {[:test_data], [:id]}
      test_data = [%{id: 1, name: "test"}, %{id: 2, name: "test2"}]

      # Create shared memory
      {:ok, memory_ref} = SharedMemoryManager.get_or_create_shared_memory(memory_key, test_data)

      # Verify memory reference is returned
      assert is_binary(memory_ref)
      assert String.starts_with?(memory_ref, "shared_mem_")

      # Retrieve shared memory
      {:ok, retrieved_data} = SharedMemoryManager.get_shared_memory(memory_ref)
      assert retrieved_data == test_data
    end

    test "reuses existing shared memory for same key", %{manager: _manager} do
      memory_key = {[:identical_data], [:id]}
      test_data = [%{id: 1, value: "shared"}]

      # Create shared memory first time
      {:ok, memory_ref1} = SharedMemoryManager.get_or_create_shared_memory(memory_key, test_data)

      # Request same memory key again
      {:ok, memory_ref2} = SharedMemoryManager.get_or_create_shared_memory(memory_key, test_data)

      # Should return same reference
      assert memory_ref1 == memory_ref2
    end

    test "creates different references for different keys", %{manager: _manager} do
      key1 = {[:data1], [:id]}
      key2 = {[:data2], [:id]}
      data1 = [%{id: 1}]
      data2 = [%{id: 2}]

      {:ok, ref1} = SharedMemoryManager.get_or_create_shared_memory(key1, data1)
      {:ok, ref2} = SharedMemoryManager.get_or_create_shared_memory(key2, data2)

      assert ref1 != ref2
    end
  end

  describe "memory management" do
    test "releases shared memory when reference count reaches zero", %{manager: _manager} do
      memory_key = {[:temp_data], [:id]}
      test_data = [%{id: 1}]

      {:ok, memory_ref} = SharedMemoryManager.get_or_create_shared_memory(memory_key, test_data)

      # Release the memory
      :ok = SharedMemoryManager.release_shared_memory(memory_ref)

      # Memory should no longer be retrievable
      assert {:error, :not_found} = SharedMemoryManager.get_shared_memory(memory_ref)
    end

    test "updates existing shared memory", %{manager: _manager} do
      memory_key = {[:update_data], [:id]}
      initial_data = [%{id: 1, value: "old"}]
      updated_data = [%{id: 1, value: "new"}]

      {:ok, memory_ref} =
        SharedMemoryManager.get_or_create_shared_memory(memory_key, initial_data)

      # Update the memory
      :ok = SharedMemoryManager.update_shared_memory(memory_ref, updated_data)

      # Verify updated data
      {:ok, retrieved_data} = SharedMemoryManager.get_shared_memory(memory_ref)
      assert retrieved_data == updated_data
    end
  end

  describe "statistics" do
    test "tracks memory statistics", %{manager: _manager} do
      memory_key = {[:stats_data], [:id]}
      test_data = [%{id: 1}]

      {:ok, _memory_ref} = SharedMemoryManager.get_or_create_shared_memory(memory_key, test_data)

      stats = SharedMemoryManager.get_memory_statistics()

      assert stats.total_shared_memories >= 1
      assert stats.total_memory_usage > 0
      assert is_float(stats.memory_savings)
      assert is_float(stats.cache_hit_rate)
    end
  end
end
