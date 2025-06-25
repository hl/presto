defmodule Presto.PersistenceTest do
  use ExUnit.Case, async: true

  alias Presto.Persistence

  describe "adapter configuration" do
    test "defaults to ETS adapter when no config provided" do
      assert Persistence.get_adapter() == Presto.Persistence.EtsAdapter
    end

    test "uses configured adapter from application env" do
      # Save original config
      original_adapter = Application.get_env(:presto, :persistence_adapter)

      # Set test config
      Application.put_env(:presto, :persistence_adapter, Presto.Persistence.DetsAdapter)

      try do
        assert Persistence.get_adapter() == Presto.Persistence.DetsAdapter
      after
        # Restore original config
        if original_adapter do
          Application.put_env(:presto, :persistence_adapter, original_adapter)
        else
          Application.delete_env(:presto, :persistence_adapter)
        end
      end
    end
  end

  describe "basic persistence operations" do
    setup do
      # Use ETS adapter for fast tests
      table = Persistence.create_table(:test_persistence, type: :set)
      on_exit(fn -> Persistence.delete_table(table) end)
      {:ok, table: table}
    end

    test "creates table with options", %{table: table} do
      assert table == :test_persistence
      assert Persistence.table_info(table, :type) == :set
    end

    test "inserts and retrieves data", %{table: table} do
      assert :ok = Persistence.insert(table, {"key1", "value1"})
      assert Persistence.lookup(table, "key1") == ["value1"]
      assert Persistence.lookup(table, "nonexistent") == []
    end

    test "deletes data", %{table: table} do
      Persistence.insert(table, {"key1", "value1"})
      Persistence.insert(table, {"key2", "value2"})

      assert :ok = Persistence.delete(table, "key1")
      assert Persistence.lookup(table, "key1") == []
      assert Persistence.lookup(table, "key2") == ["value2"]
    end

    test "lists all entries", %{table: table} do
      entries = [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}]

      Enum.each(entries, fn entry ->
        Persistence.insert(table, entry)
      end)

      all_entries = Persistence.list_all(table)
      assert length(all_entries) == 3

      Enum.each(entries, fn entry ->
        assert entry in all_entries
      end)
    end

    test "clears table", %{table: table} do
      Persistence.insert(table, {"key1", "value1"})
      Persistence.insert(table, {"key2", "value2"})

      assert Persistence.size(table) == 2
      assert :ok = Persistence.clear(table)
      assert Persistence.size(table) == 0
      assert Persistence.list_all(table) == []
    end

    test "reports table size", %{table: table} do
      assert Persistence.size(table) == 0

      Persistence.insert(table, {"key1", "value1"})
      assert Persistence.size(table) == 1

      Persistence.insert(table, {"key2", "value2"})
      assert Persistence.size(table) == 2
    end

    test "provides table metadata", %{table: table} do
      assert Persistence.table_info(table, :type) == :set
      assert Persistence.table_info(table, :name) == :test_persistence
      assert is_integer(Persistence.table_info(table, :memory))
    end
  end

  describe "batch operations" do
    setup do
      table = Persistence.create_table(:batch_test, type: :set)
      on_exit(fn -> Persistence.delete_table(table) end)
      {:ok, table: table}
    end

    test "batch inserts when adapter supports it", %{table: table} do
      entries = [
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
      ]

      assert :ok = Persistence.insert_batch(table, entries)
      assert Persistence.size(table) == 3

      Enum.each(entries, fn {key, value} ->
        assert Persistence.lookup(table, key) == [value]
      end)
    end

    test "handles empty batch insert", %{table: table} do
      assert :ok = Persistence.insert_batch(table, [])
      assert Persistence.size(table) == 0
    end

    test "falls back to individual inserts for adapters without batch support" do
      # Create a mock adapter module that doesn't implement insert_batch
      defmodule MockAdapter do
        @behaviour Presto.Persistence.Adapter

        def create_table(name, _opts), do: :ets.new(name, [:set, :public, :named_table])

        def insert(table, entry),
          do:
            (
              :ets.insert(table, entry)
              :ok
            )

        def lookup(table, key) do
          case :ets.lookup(table, key) do
            [] -> []
            results -> Enum.map(results, fn {^key, value} -> value end)
          end
        end

        def delete(table, key),
          do:
            (
              :ets.delete(table, key)
              :ok
            )

        def delete_table(table),
          do:
            (
              :ets.delete(table)
              :ok
            )

        def list_all(table), do: :ets.tab2list(table)

        def clear(table),
          do:
            (
              :ets.delete_all_objects(table)
              :ok
            )

        def size(table), do: :ets.info(table, :size) || 0
        def table_info(table, key), do: :ets.info(table, key)
        # Note: No insert_batch/2 implementation
      end

      # Save original adapter
      original_adapter = Application.get_env(:presto, :persistence_adapter)
      Application.put_env(:presto, :persistence_adapter, MockAdapter)

      try do
        table = Persistence.create_table(:fallback_test)

        entries = [{"key1", "value1"}, {"key2", "value2"}]
        assert :ok = Persistence.insert_batch(table, entries)
        assert Persistence.size(table) == 2

        Persistence.delete_table(table)
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

  describe "snapshot and restore operations" do
    setup do
      table = Persistence.create_table(:snapshot_test, type: :set)
      on_exit(fn -> Persistence.delete_table(table) end)
      {:ok, table: table}
    end

    test "creates and restores snapshots", %{table: table} do
      # Insert test data
      entries = [{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}]
      Enum.each(entries, fn entry -> Persistence.insert(table, entry) end)

      # Create snapshot
      {:ok, snapshot} = Persistence.snapshot(table)
      assert is_list(snapshot) or is_map(snapshot)

      # Clear table
      Persistence.clear(table)
      assert Persistence.size(table) == 0

      # Restore from snapshot
      assert :ok = Persistence.restore(table, snapshot)
      assert Persistence.size(table) == 3

      # Verify all data is restored
      Enum.each(entries, fn {key, value} ->
        assert Persistence.lookup(table, key) == [value]
      end)
    end

    test "handles snapshot for empty table", %{table: table} do
      {:ok, snapshot} = Persistence.snapshot(table)
      assert :ok = Persistence.restore(table, snapshot)
      assert Persistence.size(table) == 0
    end

    test "falls back to default snapshot implementation for adapters without native support" do
      # Test with adapter that doesn't implement snapshot/restore
      defmodule MockSimpleAdapter do
        @behaviour Presto.Persistence.Adapter

        def create_table(name, _opts), do: :ets.new(name, [:set, :public, :named_table])

        def insert(table, entry),
          do:
            (
              :ets.insert(table, entry)
              :ok
            )

        def lookup(table, key) do
          case :ets.lookup(table, key) do
            [] -> []
            results -> Enum.map(results, fn {^key, value} -> value end)
          end
        end

        def delete(table, key),
          do:
            (
              :ets.delete(table, key)
              :ok
            )

        def delete_table(table),
          do:
            (
              :ets.delete(table)
              :ok
            )

        def list_all(table), do: :ets.tab2list(table)

        def clear(table),
          do:
            (
              :ets.delete_all_objects(table)
              :ok
            )

        def size(table), do: :ets.info(table, :size) || 0
        def table_info(table, key), do: :ets.info(table, key)
        # Note: No snapshot/restore implementation
      end

      # Save original adapter
      original_adapter = Application.get_env(:presto, :persistence_adapter)
      Application.put_env(:presto, :persistence_adapter, MockSimpleAdapter)

      try do
        table = Persistence.create_table(:simple_test)

        # Insert data
        Persistence.insert(table, {"key1", "value1"})
        Persistence.insert(table, {"key2", "value2"})

        # Create snapshot (should use default list_all implementation)
        {:ok, snapshot} = Persistence.snapshot(table)
        assert is_list(snapshot)
        assert length(snapshot) == 2

        # Clear and restore
        Persistence.clear(table)
        assert :ok = Persistence.restore(table, snapshot)
        assert Persistence.size(table) == 2

        Persistence.delete_table(table)
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

  describe "adapter option handling" do
    test "merges adapter-specific options" do
      # This test ensures adapter_opts are properly merged
      table =
        Persistence.create_table(:options_test,
          type: :bag,
          access: :protected,
          adapter_opts: [read_concurrency: false]
        )

      # For ETS adapter, these should be applied
      assert Persistence.table_info(table, :type) == :bag
      assert Persistence.table_info(table, :protection) == :protected

      Persistence.delete_table(table)
    end

    test "handles invalid options gracefully" do
      # Create table with some invalid options that adapter should ignore
      table =
        Persistence.create_table(:invalid_options_test,
          type: :set,
          invalid_option: "should_be_ignored",
          another_invalid: 12345
        )

      # Table should still be created successfully
      assert Persistence.table_info(table, :type) == :set

      Persistence.delete_table(table)
    end
  end

  describe "integration with different table types" do
    test "works with bag tables" do
      table = Persistence.create_table(:bag_integration, type: :bag)

      # Insert multiple values for same key
      assert :ok = Persistence.insert(table, {"key1", "value1"})
      assert :ok = Persistence.insert(table, {"key1", "value2"})

      values = Persistence.lookup(table, "key1")
      assert length(values) == 2
      assert "value1" in values
      assert "value2" in values

      Persistence.delete_table(table)
    end

    test "works with ordered_set tables" do
      table = Persistence.create_table(:ordered_integration, type: :ordered_set)

      # Insert in non-sorted order
      Persistence.insert(table, {3, "third"})
      Persistence.insert(table, {1, "first"})
      Persistence.insert(table, {2, "second"})

      # List all should return entries (order depends on adapter)
      entries = Persistence.list_all(table)
      assert length(entries) == 3

      # All entries should be findable
      assert Persistence.lookup(table, 1) == ["first"]
      assert Persistence.lookup(table, 2) == ["second"]
      assert Persistence.lookup(table, 3) == ["third"]

      Persistence.delete_table(table)
    end
  end
end
