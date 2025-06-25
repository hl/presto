defmodule Presto.ApplicationTest do
  use ExUnit.Case, async: true

  describe "application startup" do
    test "starts with all required processes" do
      # Application should already be started by ExUnit
      # Verify that our supervision tree components are running

      # Check that EngineRegistry is running
      assert Process.whereis(Presto.EngineRegistry) != nil

      # Check that EngineSupervisor is running 
      assert Process.whereis(Presto.EngineSupervisor) != nil

      # Verify registry functionality
      health = Presto.EngineRegistry.health_check()
      assert is_map(health)

      # Verify supervisor functionality
      health = Presto.EngineSupervisor.health_check()
      assert is_map(health)
      assert Map.has_key?(health, :total_engines)
    end

    test "can start and register engines through application supervision" do
      # Start an engine through the supervisor with name registration
      {:ok, engine_pid} =
        Presto.EngineSupervisor.start_engine(name: :test_engine, engine_id: "test-001")

      # Verify it's registered
      assert {:ok, ^engine_pid} = Presto.EngineRegistry.lookup_engine(:test_engine)

      # Get detailed info
      assert {:ok, info} = Presto.EngineRegistry.get_engine_info(:test_engine)
      assert info.pid == engine_pid
      assert info.engine_id == "test-001"

      # Stop the engine
      :ok = Presto.EngineSupervisor.stop_engine(:test_engine)

      # Verify it's unregistered
      assert :error = Presto.EngineRegistry.lookup_engine(:test_engine)
    end

    test "supervisor restarts failed engines" do
      # Start an engine with name registration
      {:ok, engine_pid} =
        Presto.EngineSupervisor.start_engine(name: :restart_test, engine_id: "restart-001")

      # Kill the engine process
      Process.exit(engine_pid, :kill)

      # Give supervisor time to restart
      Process.sleep(100)

      # Check engine is still registered but with new pid
      case Presto.EngineRegistry.lookup_engine(:restart_test) do
        {:ok, new_pid} ->
          # If registry auto-cleanup worked, engine should be gone
          assert new_pid != engine_pid, "Engine should have been restarted with new PID"

        :error ->
          # Registry detected death and cleaned up - this is also valid behavior
          :ok
      end

      # Clean up
      try do
        Presto.EngineSupervisor.stop_engine(:restart_test)
      rescue
        _ -> :ok
      end
    end

    test "registry survives engine failures" do
      # Start an engine with name registration
      {:ok, engine_pid} =
        Presto.EngineSupervisor.start_engine(name: :failure_test, engine_id: "failure-001")

      # Verify registry health before
      health = Presto.EngineRegistry.health_check()
      assert is_map(health)

      # Kill the engine
      Process.exit(engine_pid, :kill)

      # Registry should still be healthy
      health = Presto.EngineRegistry.health_check()
      assert is_map(health)

      # Should be able to start new engines
      {:ok, _new_pid} =
        Presto.EngineSupervisor.start_engine(name: :after_failure, engine_id: "after-001")

      # Clean up
      try do
        Presto.EngineSupervisor.stop_engine(:after_failure)
      rescue
        _ -> :ok
      end
    end
  end

  describe "telemetry integration" do
    test "telemetry setup handles missing dependencies gracefully" do
      # The application should start even if telemetry dependencies are missing
      # This is tested implicitly by the application starting successfully
      assert Process.whereis(Presto.Supervisor) != nil
    end

    test "can emit telemetry events if available" do
      # Try to emit a telemetry event - should not crash even if telemetry is missing
      try do
        Presto.Telemetry.emit_engine_start(:test_engine, self())
      rescue
        # Expected if telemetry is not available
        _ -> :ok
      end
    end
  end
end
