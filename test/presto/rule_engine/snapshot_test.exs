defmodule Presto.RuleEngine.SnapshotTest do
  use ExUnit.Case, async: true

  alias Presto.RuleEngine

  @test_dir "tmp/test_snapshots"

  setup do
    # Clean up before each test
    File.rm_rf!(@test_dir)
    File.mkdir_p!(@test_dir)

    on_exit(fn ->
      File.rm_rf!(@test_dir)
    end)

    :ok
  end

  describe "engine snapshot creation" do
    test "creates snapshot of empty engine" do
      {:ok, engine} = RuleEngine.start_link([])

      {:ok, snapshot} = RuleEngine.create_snapshot(engine)

      assert is_map(snapshot)
      assert Map.has_key?(snapshot, :version)
      assert Map.has_key?(snapshot, :timestamp)
      assert Map.has_key?(snapshot, :engine_id)
      assert Map.has_key?(snapshot, :facts)
      assert Map.has_key?(snapshot, :rules)

      assert snapshot.version == "1.0"
      assert is_integer(snapshot.timestamp)
      assert is_binary(snapshot.engine_id)

      GenServer.stop(engine)
    end

    test "creates snapshot with facts and rules" do
      {:ok, engine} = RuleEngine.start_link([])

      # Add some rules
      rule1 = %{
        id: :test_rule_1,
        conditions: [{:person, :name, :age}],
        action: fn _ -> [{:adult, "test"}] end,
        priority: 10
      }

      rule2 = %{
        id: :test_rule_2,
        conditions: [{:person, :name, :salary}],
        action: fn _ -> [{:employed, "test"}] end,
        priority: 5
      }

      assert :ok = RuleEngine.add_rule(engine, rule1)
      assert :ok = RuleEngine.add_rule(engine, rule2)

      # Add some facts
      facts = [
        {:person, "Alice", 30},
        {:person, "Bob", 25},
        {:salary, "Alice", 50_000}
      ]

      Enum.each(facts, fn fact ->
        RuleEngine.assert_fact(engine, fact)
      end)

      # Create snapshot
      {:ok, snapshot} = RuleEngine.create_snapshot(engine)

      assert is_map(snapshot)
      assert map_size(snapshot.rules) == 2

      # Verify rules are in snapshot
      rule_ids = Map.keys(snapshot.rules)
      assert :test_rule_1 in rule_ids
      assert :test_rule_2 in rule_ids

      # Verify facts are in snapshot (format depends on adapter)
      assert is_map(snapshot.facts) or is_list(snapshot.facts)

      GenServer.stop(engine)
    end

    test "includes engine statistics in snapshot" do
      {:ok, engine} = RuleEngine.start_link([])

      # Add rule and fire it to generate statistics
      rule = %{
        id: :stats_rule,
        conditions: [{:test, :value}],
        action: fn _ -> [{:result, "processed"}] end
      }

      RuleEngine.add_rule(engine, rule)
      RuleEngine.assert_fact(engine, {:test, "value"})
      RuleEngine.fire_all_rules(engine)

      {:ok, snapshot} = RuleEngine.create_snapshot(engine)

      assert Map.has_key?(snapshot, :statistics)
      assert is_map(snapshot.statistics)

      GenServer.stop(engine)
    end
  end

  describe "engine snapshot restoration" do
    test "restores empty engine from snapshot" do
      # Create original engine and snapshot
      {:ok, original_engine} = RuleEngine.start_link([])
      {:ok, snapshot} = RuleEngine.create_snapshot(original_engine)
      GenServer.stop(original_engine)

      # Create new engine and restore
      {:ok, new_engine} = RuleEngine.start_link([])
      assert :ok = RuleEngine.restore_from_snapshot(new_engine, snapshot)

      # Verify state
      rules = RuleEngine.get_rules(new_engine)
      assert rules == %{}

      GenServer.stop(new_engine)
    end

    test "restores engine with rules and facts" do
      # Create original engine
      {:ok, original_engine} = RuleEngine.start_link([])

      # Add rules
      rule1 = %{
        id: :restore_rule_1,
        conditions: [{:person, :name, :age}],
        action: fn facts -> [{:adult, facts[:name]}] end,
        priority: 10
      }

      rule2 = %{
        id: :restore_rule_2,
        conditions: [{:person, :name, :salary}],
        action: fn facts -> [{:employed, facts[:name]}] end,
        priority: 5
      }

      RuleEngine.add_rule(original_engine, rule1)
      RuleEngine.add_rule(original_engine, rule2)

      # Add facts
      facts = [
        {:person, "Alice", 30},
        {:person, "Bob", 25},
        {:salary, "Alice", 50_000}
      ]

      Enum.each(facts, fn fact ->
        RuleEngine.assert_fact(original_engine, fact)
      end)

      # Create snapshot
      {:ok, snapshot} = RuleEngine.create_snapshot(original_engine)
      GenServer.stop(original_engine)

      # Create new engine and restore
      {:ok, new_engine} = RuleEngine.start_link([])
      assert :ok = RuleEngine.restore_from_snapshot(new_engine, snapshot)

      # Verify rules are restored
      restored_rules = RuleEngine.get_rules(new_engine)
      assert map_size(restored_rules) == 2

      rule_ids = Map.keys(restored_rules)
      assert :restore_rule_1 in rule_ids
      assert :restore_rule_2 in rule_ids

      # Verify facts are restored by checking if rules can still fire
      results = RuleEngine.fire_all_rules(new_engine)
      assert length(results) > 0

      GenServer.stop(new_engine)
    end

    test "handles invalid snapshot format" do
      {:ok, engine} = RuleEngine.start_link([])

      invalid_snapshots = [
        "not_a_map",
        %{invalid: "format"},
        # Missing required fields
        %{version: "1.0"},
        # Missing version and other required fields
        %{facts: [], rules: []}
      ]

      Enum.each(invalid_snapshots, fn invalid_snapshot ->
        assert {:error, _reason} = RuleEngine.restore_from_snapshot(engine, invalid_snapshot)
      end)

      GenServer.stop(engine)
    end

    test "preserves engine functionality after restore" do
      # Create original engine with working rules
      {:ok, original_engine} = RuleEngine.start_link([])

      rule = %{
        id: :functional_rule,
        conditions: [{:input, :value}],
        action: fn facts -> [{:output, facts[:value] * 2}] end
      }

      RuleEngine.add_rule(original_engine, rule)
      RuleEngine.assert_fact(original_engine, {:input, 5})

      original_results = RuleEngine.fire_all_rules(original_engine)

      # Create snapshot
      {:ok, snapshot} = RuleEngine.create_snapshot(original_engine)
      GenServer.stop(original_engine)

      # Restore to new engine
      {:ok, new_engine} = RuleEngine.start_link([])
      RuleEngine.restore_from_snapshot(new_engine, snapshot)

      # Test that rules still work
      RuleEngine.assert_fact(new_engine, {:input, 10})
      new_results = RuleEngine.fire_all_rules(new_engine)

      # Should get result for the new fact
      assert length(new_results) > 0
      output_facts = Enum.filter(new_results, fn {type, _} -> type == :output end)
      assert length(output_facts) > 0

      GenServer.stop(new_engine)
    end
  end

  describe "file-based snapshot operations" do
    test "saves and loads snapshot to/from file" do
      {:ok, engine} = RuleEngine.start_link([])

      # Add some content to snapshot
      rule = %{
        id: :file_test_rule,
        conditions: [{:test, :data}],
        action: fn _ -> [{:result, "file_test"}] end
      }

      RuleEngine.add_rule(engine, rule)
      RuleEngine.assert_fact(engine, {:test, "data"})

      # Save snapshot to file
      file_path = Path.join(@test_dir, "test_snapshot.bin")
      assert :ok = RuleEngine.save_snapshot_to_file(engine, file_path)

      # Verify file was created
      assert File.exists?(file_path)

      # Create new engine and load snapshot
      {:ok, new_engine} = RuleEngine.start_link([])
      assert :ok = RuleEngine.load_snapshot_from_file(new_engine, file_path)

      # Verify content was restored
      rules = RuleEngine.get_rules(new_engine)
      assert map_size(rules) == 1
      assert Map.has_key?(rules, :file_test_rule)

      GenServer.stop(engine)
      GenServer.stop(new_engine)
    end

    test "handles file operation errors" do
      {:ok, engine} = RuleEngine.start_link([])

      # Try to save to invalid path
      invalid_path = "/invalid/path/snapshot.bin"
      assert {:error, _reason} = RuleEngine.save_snapshot_to_file(engine, invalid_path)

      # Try to load from non-existent file
      non_existent_path = Path.join(@test_dir, "nonexistent.bin")
      assert {:error, _reason} = RuleEngine.load_snapshot_from_file(engine, non_existent_path)

      # Try to load from invalid file
      invalid_file = Path.join(@test_dir, "invalid.bin")
      File.write!(invalid_file, "not a valid snapshot")
      assert {:error, _reason} = RuleEngine.load_snapshot_from_file(engine, invalid_file)

      GenServer.stop(engine)
    end

    test "creates directory structure when saving snapshot" do
      {:ok, engine} = RuleEngine.start_link([])

      # Save to nested directory that doesn't exist
      nested_path = Path.join([@test_dir, "nested", "deep", "snapshot.bin"])
      assert :ok = RuleEngine.save_snapshot_to_file(engine, nested_path)

      # Verify directory was created
      assert File.exists?(nested_path)
      assert File.dir?(Path.dirname(nested_path))

      GenServer.stop(engine)
    end
  end

  describe "snapshot format and compatibility" do
    test "snapshot contains all required metadata" do
      {:ok, engine} = RuleEngine.start_link([])
      {:ok, snapshot} = RuleEngine.create_snapshot(engine)

      required_fields = [:version, :timestamp, :engine_id, :facts, :rules]

      Enum.each(required_fields, fn field ->
        assert Map.has_key?(snapshot, field), "Snapshot missing required field: #{field}"
      end)

      # Verify data types
      assert is_binary(snapshot.version)
      assert is_integer(snapshot.timestamp)
      assert is_binary(snapshot.engine_id)
      assert is_map(snapshot.rules)

      GenServer.stop(engine)
    end

    test "snapshot preserves rule metadata" do
      {:ok, engine} = RuleEngine.start_link([])

      original_rule = %{
        id: :metadata_rule,
        conditions: [{:test, :value}],
        action: fn _ -> [{:result, "test"}] end,
        priority: 42
      }

      RuleEngine.add_rule(engine, original_rule)
      {:ok, snapshot} = RuleEngine.create_snapshot(engine)

      # Find the rule in snapshot
      snapshotted_rule = Map.get(snapshot.rules, :metadata_rule)

      assert snapshotted_rule != nil
      assert snapshotted_rule.id == original_rule.id
      assert snapshotted_rule.conditions == original_rule.conditions
      assert snapshotted_rule.priority == original_rule.priority
      assert is_function(snapshotted_rule.action)

      GenServer.stop(engine)
    end

    test "handles large snapshots" do
      {:ok, engine} = RuleEngine.start_link([])

      # Add many rules and facts
      Enum.each(1..100, fn i ->
        rule = %{
          id: String.to_atom("rule_#{i}"),
          conditions: [{:test, :field, String.to_atom("var_#{i}")}],
          action: fn _ -> [{:result, i}] end,
          priority: rem(i, 10)
        }

        RuleEngine.add_rule(engine, rule)
        RuleEngine.assert_fact(engine, {:test, "value_#{i}", i})
      end)

      # Create and verify snapshot
      {:ok, snapshot} = RuleEngine.create_snapshot(engine)
      assert map_size(snapshot.rules) == 100

      # Test restoration
      {:ok, new_engine} = RuleEngine.start_link([])
      assert :ok = RuleEngine.restore_from_snapshot(new_engine, snapshot)

      restored_rules = RuleEngine.get_rules(new_engine)
      assert map_size(restored_rules) == 100

      GenServer.stop(engine)
      GenServer.stop(new_engine)
    end
  end

  describe "snapshot with different persistence adapters" do
    # Skip by default as it requires setting up DETS
    @tag :skip
    test "works with DETS adapter" do
      # Save original adapter
      original_adapter = Application.get_env(:presto, :persistence_adapter)

      try do
        # Configure DETS adapter
        Application.put_env(:presto, :persistence_adapter, Presto.Persistence.DetsAdapter)

        {:ok, engine} = RuleEngine.start_link(adapter_opts: [storage_dir: @test_dir])

        # Add content
        rule = %{
          id: :dets_rule,
          conditions: [{:persistent, :data}],
          action: fn _ -> [{:result, "persistent"}] end
        }

        RuleEngine.add_rule(engine, rule)
        RuleEngine.assert_fact(engine, {:persistent, "data"})

        # Create snapshot
        {:ok, snapshot} = RuleEngine.create_snapshot(engine)
        assert is_map(snapshot)

        # Restore to new engine
        {:ok, new_engine} = RuleEngine.start_link(adapter_opts: [storage_dir: @test_dir])

        assert :ok = RuleEngine.restore_from_snapshot(new_engine, snapshot)

        rules = RuleEngine.get_rules(new_engine)
        assert map_size(rules) == 1
        assert Map.has_key?(rules, :dets_rule)

        GenServer.stop(engine)
        GenServer.stop(new_engine)
      after
        # Restore original adapter
        if original_adapter do
          Application.put_env(:presto, :persistence_adapter, original_adapter)
        else
          Application.delete_env(:presto, :persistence_adapter)
        end
      end
    end
  end
end
