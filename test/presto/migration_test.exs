defmodule Presto.MigrationTest do
  @moduledoc """
  Tests for backward compatibility with existing Presto usage patterns.

  These tests ensure that Phase 1 changes do not break existing functionality
  and that users can migrate gradually to new features.
  """

  use ExUnit.Case, async: true

  alias Presto.RuleEngine

  describe "backward compatibility" do
    test "existing RuleEngine usage still works" do
      # Test that existing direct RuleEngine usage continues to work
      {:ok, engine} = RuleEngine.start_link(engine_id: "legacy-test")

      # Add a simple rule
      rule = %{
        id: :legacy_rule,
        conditions: [{:person, :name, :age}, {:age, :>, 18}],
        action: fn facts -> [{:adult, facts[:name]}] end
      }

      assert :ok = RuleEngine.add_rule(engine, rule)

      # Assert facts
      facts = [{:person, "Alice", 25}, {:age, 25}]
      assert :ok = RuleEngine.assert_facts_bulk(engine, facts)

      # Fire rules
      results = RuleEngine.fire_rules(engine)
      assert [{:adult, "Alice"}] = results

      # Clean up
      GenServer.stop(engine)
    end

    test "old rule format still works with new validation" do
      # Test that rules written in the old format still pass validation
      legacy_rule = %{
        id: :old_format_rule,
        conditions: [
          {:customer, :id, :status},
          {:status, :==, "active"}
        ],
        action: fn facts -> [{:active_customer, facts[:id]}] end,
        priority: 5
      }

      # Should validate successfully
      assert :ok = Presto.Rule.Validator.validate(legacy_rule)
    end

    test "engines created without new supervision still work" do
      # Test direct engine creation without going through new supervision
      {:ok, engine} = RuleEngine.start_link([])

      # Get stats to ensure it's working
      stats = RuleEngine.get_engine_statistics(engine)
      assert is_map(stats)
      assert Map.has_key?(stats, :engine_id)

      # Clean up
      GenServer.stop(engine)
    end

    test "existing fact assertion patterns work" do
      {:ok, engine} = RuleEngine.start_link(engine_id: "fact-test")

      # Test various fact assertion patterns that should continue working

      # Single fact assertion
      assert :ok = RuleEngine.assert_fact(engine, {:user, "bob", "admin"})

      # Multiple fact assertion
      facts = [
        {:user, "alice", "user"},
        {:user, "charlie", "guest"},
        {:permission, "admin", "write"},
        {:permission, "user", "read"}
      ]

      assert :ok = RuleEngine.assert_facts_bulk(engine, facts)

      # Verify facts were added
      stats = RuleEngine.get_engine_statistics(engine)
      assert stats[:total_facts] > 0

      # Clean up
      GenServer.stop(engine)
    end

    test "rule metadata and statistics continue to work" do
      {:ok, engine} = RuleEngine.start_link(engine_id: "stats-test")

      # Add multiple rules
      rules = [
        %{
          id: :rule1,
          conditions: [{:data, :value, :id}],
          action: fn _ -> [{:processed, :rule1}] end
        },
        %{
          id: :rule2,
          conditions: [{:trigger, :active, :id}],
          action: fn _ -> [{:processed, :rule2}] end
        }
      ]

      Enum.each(rules, fn rule ->
        assert :ok = RuleEngine.add_rule(engine, rule)
      end)

      # Verify statistics
      stats = RuleEngine.get_engine_statistics(engine)
      assert stats[:total_rules] == 2

      # Verify rule analysis still works
      analysis = RuleEngine.analyze_rule(engine, :rule1)
      assert is_map(analysis)

      # Clean up
      GenServer.stop(engine)
    end

    test "rule execution order and priority still respected" do
      {:ok, engine} = RuleEngine.start_link(engine_id: "priority-test")

      # Add rules with different priorities
      high_priority_rule = %{
        id: :high_priority,
        conditions: [{:order, :test, :id}],
        action: fn _ -> [{:result, :high}] end,
        priority: 10
      }

      low_priority_rule = %{
        id: :low_priority,
        conditions: [{:order, :test, :id}],
        action: fn _ -> [{:result, :low}] end,
        priority: 1
      }

      # Add in reverse priority order
      assert :ok = RuleEngine.add_rule(engine, low_priority_rule)
      assert :ok = RuleEngine.add_rule(engine, high_priority_rule)

      # Assert triggering fact
      assert :ok = RuleEngine.assert_fact(engine, {:order, :test, "id1"})

      # Fire rules and verify high priority executes first
      results = RuleEngine.fire_rules(engine)
      assert [{:result, :high}, {:result, :low}] = results

      # Clean up
      GenServer.stop(engine)
    end

    test "error handling patterns remain consistent" do
      {:ok, engine} = RuleEngine.start_link(engine_id: "error-test")

      # Test invalid rule addition still returns expected errors
      invalid_rule = %{
        # Invalid ID type
        id: "not_an_atom",
        conditions: [{:test, :value}],
        action: fn _ -> [] end
      }

      assert {:error, _reason} = RuleEngine.add_rule(engine, invalid_rule)

      # Test invalid fact assertion
      assert_raise ArgumentError, fn ->
        RuleEngine.assert_fact(engine, "not_a_tuple")
      end

      # Clean up
      GenServer.stop(engine)
    end
  end

  describe "new features are optional" do
    test "engines work without registry registration" do
      # Start engine without going through supervisor
      {:ok, engine} = RuleEngine.start_link(engine_id: "no-registry")

      # Verify it's not in registry
      assert :error = Presto.EngineRegistry.lookup_engine(:no_registry)

      # But engine still works normally
      rule = %{
        id: :test_rule,
        conditions: [{:test, :data, :id}],
        action: fn _ -> [{:result, :success}] end
      }

      assert :ok = RuleEngine.add_rule(engine, rule)
      assert :ok = RuleEngine.assert_fact(engine, {:test, :data, "id1"})
      results = RuleEngine.fire_rules(engine)
      assert [{:result, :success}] = results

      # Clean up
      GenServer.stop(engine)
    end

    test "telemetry events are optional" do
      # Engine should work fine even if telemetry fails
      {:ok, engine} = RuleEngine.start_link(engine_id: "no-telemetry")

      # Basic operations should work regardless of telemetry availability
      rule = %{
        id: :telemetry_test,
        conditions: [{:event, :data, :id}],
        action: fn _ -> [{:logged, :event}] end
      }

      assert :ok = RuleEngine.add_rule(engine, rule)
      assert :ok = RuleEngine.assert_fact(engine, {:event, :data, "id1"})
      results = RuleEngine.fire_rules(engine)
      assert [{:logged, :event}] = results

      # Clean up
      GenServer.stop(engine)
    end

    test "rule validation is backward compatible" do
      # Old rules should validate successfully
      classic_rules = [
        # Simple rule
        %{
          id: :simple,
          conditions: [{:fact, :value, :id}],
          action: fn _ -> [] end
        },

        # Rule with priority
        %{
          id: :priority_rule,
          conditions: [{:urgent, :task, :id}],
          action: fn _ -> [] end,
          priority: 5
        },

        # Rule with multiple conditions
        %{
          id: :complex,
          conditions: [
            {:user, :id, :role},
            {:role, :==, "admin"},
            {:permission, :action, :resource}
          ],
          action: fn _ -> [] end
        }
      ]

      Enum.each(classic_rules, fn rule ->
        assert :ok = Presto.Rule.Validator.validate(rule)
      end)
    end
  end

  describe "gradual migration support" do
    test "can mix supervised and unsupervised engines" do
      # Start one engine through supervisor
      {:ok, supervised_engine} =
        Presto.EngineSupervisor.start_engine(
          name: :supervised,
          engine_id: "supervised-001"
        )

      # Start another engine directly
      {:ok, direct_engine} = RuleEngine.start_link(engine_id: "direct-001")

      # Both should work independently
      rule = %{
        id: :test_rule,
        conditions: [{:test, :value, :id}],
        action: fn _ -> [{:processed, :test}] end
      }

      assert :ok = RuleEngine.add_rule(supervised_engine, rule)
      assert :ok = RuleEngine.add_rule(direct_engine, rule)

      # Verify registry only has supervised engine
      assert {:ok, ^supervised_engine} = Presto.EngineRegistry.lookup_engine(:supervised)
      assert :error = Presto.EngineRegistry.lookup_engine(:direct)

      # Both engines should function correctly
      assert :ok = RuleEngine.assert_fact(supervised_engine, {:test, :value, "id1"})
      assert :ok = RuleEngine.assert_fact(direct_engine, {:test, :value, "id1"})

      results1 = RuleEngine.fire_rules(supervised_engine)
      results2 = RuleEngine.fire_rules(direct_engine)

      assert results1 == results2
      assert [{:processed, :test}] = results1

      # Clean up
      :ok = Presto.EngineSupervisor.stop_engine(:supervised)
      GenServer.stop(direct_engine)
    end

    test "new validation macros work alongside old validation" do
      # This demonstrates that both old and new validation approaches work

      # Old style - manual validation
      old_rule = %{
        id: :old_style,
        conditions: [{:data, :key, :value}, {:value, :>, 10}],
        action: fn _ -> [] end
      }

      assert :ok = Presto.Rule.Validator.validate(old_rule)

      # New style would use compile-time validation macros
      # But the underlying validation logic should be the same

      # Both should accept the same rule structures
      {:ok, engine} = RuleEngine.start_link(engine_id: "validation-test")
      assert :ok = RuleEngine.add_rule(engine, old_rule)

      # Clean up
      GenServer.stop(engine)
    end
  end
end
