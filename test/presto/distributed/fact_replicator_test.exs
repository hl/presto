defmodule Presto.Distributed.FactReplicatorTest do
  use ExUnit.Case, async: false

  alias Presto.Distributed.FactReplicator
  alias Presto.{RuleEngine, EngineRegistry}

  @moduletag :integration

  setup do
    # Start the fact replicator for testing
    {:ok, replicator_pid} = FactReplicator.start_link([])

    # Ensure clean state
    on_exit(fn ->
      if Process.alive?(replicator_pid) do
        GenServer.stop(replicator_pid)
      end
    end)

    {:ok, replicator_pid: replicator_pid}
  end

  describe "fact replication" do
    test "replicates facts to target nodes", %{replicator_pid: _pid} do
      # Start test engine
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:test_engine, engine_pid)

      # Test facts to replicate
      facts = [
        {:user, :alice, :active},
        {:user, :bob, :inactive},
        {:order, :o123, :alice}
      ]

      # Replicate facts (will only work with current node in test)
      target_nodes = [Node.self()]
      result = FactReplicator.replicate_to_nodes(target_nodes, :test_engine, facts)

      # Should succeed (even if only replicating to self)
      assert result in [:ok, {:error, :no_remote_nodes}]

      # Clean up
      EngineRegistry.unregister_engine(:test_engine)
      GenServer.stop(engine_pid)
    end

    test "handles synchronous replication", %{replicator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:sync_test_engine, engine_pid)

      facts = [{:sync_test, :data, :value}]

      result =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :sync_test_engine,
          facts,
          strategy: :synchronous,
          timeout: 5000
        )

      assert result in [:ok, {:error, :no_remote_nodes}]

      # Clean up
      EngineRegistry.unregister_engine(:sync_test_engine)
      GenServer.stop(engine_pid)
    end

    test "handles asynchronous replication", %{replicator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:async_test_engine, engine_pid)

      facts = [{:async_test, :data, :value}]

      result =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :async_test_engine,
          facts,
          strategy: :asynchronous
        )

      assert result in [:ok, {:error, :no_remote_nodes}]

      # Clean up
      EngineRegistry.unregister_engine(:async_test_engine)
      GenServer.stop(engine_pid)
    end

    test "handles batch replication", %{replicator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:batch_test_engine, engine_pid)

      # Large batch of facts
      facts =
        Enum.map(1..100, fn i ->
          {:batch_fact, i, "data_#{i}"}
        end)

      result =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :batch_test_engine,
          facts,
          batch_size: 10
        )

      assert result in [:ok, {:error, :no_remote_nodes}]

      # Clean up
      EngineRegistry.unregister_engine(:batch_test_engine)
      GenServer.stop(engine_pid)
    end
  end

  describe "conflict resolution" do
    test "handles vector clock conflicts", %{replicator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:conflict_test_engine, engine_pid)

      # Create conflicting facts with vector clocks
      fact1 = {:conflict_fact, :key1, :value1}
      fact2 = {:conflict_fact, :key1, :value2}

      result1 =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :conflict_test_engine,
          [fact1],
          conflict_resolution: :vector_clock
        )

      result2 =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :conflict_test_engine,
          [fact2],
          conflict_resolution: :vector_clock
        )

      assert result1 in [:ok, {:error, :no_remote_nodes}]
      assert result2 in [:ok, {:error, :no_remote_nodes}]

      # Clean up
      EngineRegistry.unregister_engine(:conflict_test_engine)
      GenServer.stop(engine_pid)
    end

    test "handles last-writer-wins conflicts", %{replicator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:lww_test_engine, engine_pid)

      facts = [{:lww_fact, :key1, :value1}]

      result =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :lww_test_engine,
          facts,
          conflict_resolution: :last_writer_wins
        )

      assert result in [:ok, {:error, :no_remote_nodes}]

      # Clean up
      EngineRegistry.unregister_engine(:lww_test_engine)
      GenServer.stop(engine_pid)
    end
  end

  describe "replication status" do
    test "gets replication status", %{replicator_pid: _pid} do
      status = FactReplicator.get_replication_status()

      assert is_map(status)
      assert Map.has_key?(status, :active_replications)
      assert Map.has_key?(status, :total_facts_replicated)
      assert Map.has_key?(status, :failed_replications)

      assert is_integer(status.active_replications)
      assert is_integer(status.total_facts_replicated)
      assert is_integer(status.failed_replications)
    end

    test "gets engine-specific replication status", %{replicator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:status_test_engine, engine_pid)

      status = FactReplicator.get_engine_replication_status(:status_test_engine)

      assert is_map(status)
      assert Map.has_key?(status, :engine_name)
      assert Map.has_key?(status, :replication_nodes)
      assert Map.has_key?(status, :last_replication)

      assert status.engine_name == :status_test_engine

      # Clean up
      EngineRegistry.unregister_engine(:status_test_engine)
      GenServer.stop(engine_pid)
    end
  end

  describe "error handling" do
    test "handles replication to non-existent engine", %{replicator_pid: _pid} do
      facts = [{:test, :fact, :value}]

      result =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :nonexistent_engine,
          facts
        )

      assert {:error, _reason} = result
    end

    test "handles replication with empty facts list", %{replicator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:empty_facts_engine, engine_pid)

      result =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :empty_facts_engine,
          []
        )

      assert result in [:ok, {:error, :no_facts_to_replicate}]

      # Clean up
      EngineRegistry.unregister_engine(:empty_facts_engine)
      GenServer.stop(engine_pid)
    end

    test "handles replication timeout", %{replicator_pid: _pid} do
      {:ok, engine_pid} = RuleEngine.start_link([])
      assert :ok = EngineRegistry.register_engine(:timeout_test_engine, engine_pid)

      facts = [{:timeout_test, :fact, :value}]

      result =
        FactReplicator.replicate_to_nodes(
          [Node.self()],
          :timeout_test_engine,
          facts,
          # Very short timeout
          timeout: 1
        )

      # Should either succeed quickly or timeout
      assert result in [:ok, {:error, :timeout}, {:error, :no_remote_nodes}]

      # Clean up
      EngineRegistry.unregister_engine(:timeout_test_engine)
      GenServer.stop(engine_pid)
    end
  end

  describe "replication statistics" do
    test "provides replication statistics", %{replicator_pid: _pid} do
      stats = FactReplicator.get_replication_stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :total_replications)
      assert Map.has_key?(stats, :successful_replications)
      assert Map.has_key?(stats, :failed_replications)
      assert Map.has_key?(stats, :avg_replication_time)

      assert is_integer(stats.total_replications)
      assert is_integer(stats.successful_replications)
      assert is_integer(stats.failed_replications)
      assert is_number(stats.avg_replication_time)
    end
  end
end
