defmodule Presto.Persistence.EtsAdapterTest do
  use ExUnit.Case, async: true

  alias Presto.Persistence.EtsAdapter

  describe "ETS adapter basic operations" do
    test "creates named table with default options" do
      table = EtsAdapter.create_table(:test_table)

      assert table == :test_table
      assert :ets.info(:test_table, :type) == :set
      assert :ets.info(:test_table, :protection) == :public

      # Cleanup
      EtsAdapter.delete_table(table)
    end

    test "creates table with custom options" do
      table =
        EtsAdapter.create_table(:ordered_table,
          type: :ordered_set,
          access: :protected,
          read_concurrency: false
        )

      assert table == :ordered_table
      assert :ets.info(:ordered_table, :type) == :ordered_set
      assert :ets.info(:ordered_table, :protection) == :protected

      # Cleanup
      EtsAdapter.delete_table(table)
    end

    test "handles table creation errors gracefully" do
      # Create table first
      table1 = EtsAdapter.create_table(:duplicate_table)

      # Try to create same table again - should return existing table
      table2 = EtsAdapter.create_table(:duplicate_table)
      assert table1 == table2

      # Cleanup
      EtsAdapter.delete_table(table1)
    end
  end

  describe "insert and lookup operations" do
    setup do
      table = EtsAdapter.create_table(:test_ops)
      on_exit(fn -> EtsAdapter.delete_table(table) end)
      {:ok, table: table}
    end

    test "inserts and retrieves single value", %{table: table} do
      assert :ok = EtsAdapter.insert(table, {"key1", "value1"})

      assert EtsAdapter.lookup(table, "key1") == ["value1"]
      assert EtsAdapter.lookup(table, "nonexistent") == []
    end

    test "inserts and retrieves multiple values in bag table" do
      bag_table = EtsAdapter.create_table(:bag_table, type: :bag)

      assert :ok = EtsAdapter.insert(bag_table, {"key1", "value1"})
      assert :ok = EtsAdapter.insert(bag_table, {"key1", "value2"})

      values = EtsAdapter.lookup(bag_table, "key1")
      assert length(values) == 2
      assert "value1" in values
      assert "value2" in values

      # Cleanup
      EtsAdapter.delete_table(bag_table)
    end

    test "deletes keys", %{table: table} do
      EtsAdapter.insert(table, {"key1", "value1"})
      EtsAdapter.insert(table, {"key2", "value2"})

      assert :ok = EtsAdapter.delete(table, "key1")
      assert EtsAdapter.lookup(table, "key1") == []
      assert EtsAdapter.lookup(table, "key2") == ["value2"]
    end

    test "handles operations on non-existent table" do
      assert {:error, :table_not_found} = EtsAdapter.insert(:nonexistent, {"key", "value"})
      assert EtsAdapter.lookup(:nonexistent, "key") == []
      assert {:error, :table_not_found} = EtsAdapter.delete(:nonexistent, "key")
    end
  end

  describe "table management operations" do
    test "lists all entries" do
      table = EtsAdapter.create_table(:list_test)

      EtsAdapter.insert(table, {"key1", "value1"})
      EtsAdapter.insert(table, {"key2", "value2"})
      EtsAdapter.insert(table, {"key3", "value3"})

      entries = EtsAdapter.list_all(table)
      assert length(entries) == 3
      assert {"key1", "value1"} in entries
      assert {"key2", "value2"} in entries
      assert {"key3", "value3"} in entries

      # Cleanup
      EtsAdapter.delete_table(table)
    end

    test "clears table data" do
      table = EtsAdapter.create_table(:clear_test)

      EtsAdapter.insert(table, {"key1", "value1"})
      EtsAdapter.insert(table, {"key2", "value2"})

      assert EtsAdapter.size(table) == 2
      assert :ok = EtsAdapter.clear(table)
      assert EtsAdapter.size(table) == 0
      assert EtsAdapter.list_all(table) == []

      # Cleanup
      EtsAdapter.delete_table(table)
    end

    test "reports table size" do
      table = EtsAdapter.create_table(:size_test)

      assert EtsAdapter.size(table) == 0

      EtsAdapter.insert(table, {"key1", "value1"})
      assert EtsAdapter.size(table) == 1

      EtsAdapter.insert(table, {"key2", "value2"})
      assert EtsAdapter.size(table) == 2

      EtsAdapter.delete(table, "key1")
      assert EtsAdapter.size(table) == 1

      # Cleanup
      EtsAdapter.delete_table(table)
    end

    test "provides table information" do
      table = EtsAdapter.create_table(:info_test, type: :ordered_set, access: :private)

      assert EtsAdapter.table_info(table, :type) == :ordered_set
      assert EtsAdapter.table_info(table, :protection) == :private
      assert EtsAdapter.table_info(table, :name) == :info_test
      assert is_integer(EtsAdapter.table_info(table, :memory))

      # Cleanup
      EtsAdapter.delete_table(table)
    end
  end

  describe "batch operations" do
    setup do
      table = EtsAdapter.create_table(:batch_test)
      on_exit(fn -> EtsAdapter.delete_table(table) end)
      {:ok, table: table}
    end

    test "batch inserts multiple entries", %{table: table} do
      entries = [
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
      ]

      assert :ok = EtsAdapter.insert_batch(table, entries)
      assert EtsAdapter.size(table) == 3

      assert EtsAdapter.lookup(table, "key1") == ["value1"]
      assert EtsAdapter.lookup(table, "key2") == ["value2"]
      assert EtsAdapter.lookup(table, "key3") == ["value3"]
    end

    test "handles empty batch insert", %{table: table} do
      assert :ok = EtsAdapter.insert_batch(table, [])
      assert EtsAdapter.size(table) == 0
    end

    test "handles batch insert errors gracefully" do
      assert {:error, :table_not_found} =
               EtsAdapter.insert_batch(:nonexistent, [{"key", "value"}])
    end
  end

  describe "snapshot and restore operations" do
    setup do
      table = EtsAdapter.create_table(:snapshot_test)
      on_exit(fn -> EtsAdapter.delete_table(table) end)
      {:ok, table: table}
    end

    test "creates and restores snapshots", %{table: table} do
      # Insert test data
      EtsAdapter.insert(table, {"key1", "value1"})
      EtsAdapter.insert(table, {"key2", "value2"})
      EtsAdapter.insert(table, {"key3", "value3"})

      # Create snapshot
      {:ok, snapshot} = EtsAdapter.snapshot(table)
      assert is_map(snapshot)
      assert Map.has_key?(snapshot, :data)
      assert Map.has_key?(snapshot, :metadata)
      assert length(snapshot.data) == 3

      # Clear table
      EtsAdapter.clear(table)
      assert EtsAdapter.size(table) == 0

      # Restore from snapshot
      assert :ok = EtsAdapter.restore(table, snapshot)
      assert EtsAdapter.size(table) == 3
      assert EtsAdapter.lookup(table, "key1") == ["value1"]
      assert EtsAdapter.lookup(table, "key2") == ["value2"]
      assert EtsAdapter.lookup(table, "key3") == ["value3"]
    end

    test "restores from simple list format for backward compatibility", %{table: table} do
      data = [{"key1", "value1"}, {"key2", "value2"}]

      assert :ok = EtsAdapter.restore(table, data)
      assert EtsAdapter.size(table) == 2
      assert EtsAdapter.lookup(table, "key1") == ["value1"]
      assert EtsAdapter.lookup(table, "key2") == ["value2"]
    end

    test "handles snapshot and restore errors gracefully" do
      assert {:error, :table_not_found} = EtsAdapter.snapshot(:nonexistent)
      assert {:error, :table_not_found} = EtsAdapter.restore(:nonexistent, [])
    end

    test "handles invalid snapshot format" do
      table = EtsAdapter.create_table(:invalid_snapshot_test)

      assert {:error, :invalid_snapshot_format} = EtsAdapter.restore(table, "invalid")
      assert {:error, :invalid_snapshot_format} = EtsAdapter.restore(table, %{invalid: "format"})

      # Cleanup
      EtsAdapter.delete_table(table)
    end
  end

  describe "error handling and edge cases" do
    test "handles table deletion" do
      table = EtsAdapter.create_table(:delete_test)

      assert :ok = EtsAdapter.delete_table(table)
      assert :ets.info(table) == :undefined

      # Deleting non-existent table should return error
      assert {:error, :table_not_found} = EtsAdapter.delete_table(:nonexistent)
    end

    test "handles various data types as keys and values" do
      table = EtsAdapter.create_table(:types_test)

      # Test different key/value types
      test_cases = [
        {1, "integer_key"},
        {"string", 42},
        {:atom, %{map: "value"}},
        {{:tuple, :key}, [:list, :value]},
        {make_ref(), :reference_key}
      ]

      Enum.each(test_cases, fn {key, value} ->
        assert :ok = EtsAdapter.insert(table, {key, value})
        assert EtsAdapter.lookup(table, key) == [value]
      end)

      assert EtsAdapter.size(table) == length(test_cases)

      # Cleanup
      EtsAdapter.delete_table(table)
    end

    test "handles table info for non-existent keys" do
      table = EtsAdapter.create_table(:info_edge_test)

      # ETS adapter catches ArgumentError and returns nil for nonexistent info keys
      assert EtsAdapter.table_info(table, :nonexistent_key) == nil
      # ETS returns :undefined for nonexistent tables (not caught by adapter)
      assert EtsAdapter.table_info(:nonexistent_table, :type) == :undefined

      # Cleanup
      EtsAdapter.delete_table(table)
    end
  end
end
