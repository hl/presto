defmodule Presto.Persistence.DetsAdapterTest do
  # DETS operations use files, can't be async
  use ExUnit.Case, async: false

  alias Presto.Persistence.DetsAdapter

  @test_dir "tmp/test_dets"

  setup do
    # Clean up before each test
    File.rm_rf!(@test_dir)
    File.mkdir_p!(@test_dir)

    on_exit(fn ->
      # Clean up after each test
      File.rm_rf!(@test_dir)
    end)

    :ok
  end

  describe "DETS adapter basic operations" do
    test "creates table with default options" do
      table = DetsAdapter.create_table(:test_table, storage_dir: @test_dir)

      assert table == :test_table
      assert :dets.info(:test_table, :type) == :set

      # Verify file was created
      file_path = Path.join(@test_dir, "test_table.dets")
      assert File.exists?(file_path)

      # Cleanup
      DetsAdapter.delete_table(table)
    end

    test "creates table with custom options" do
      table =
        DetsAdapter.create_table(:ordered_table,
          storage_dir: @test_dir,
          # DETS doesn't support :ordered_set
          type: :set,
          auto_sync: false
        )

      assert table == :ordered_table
      assert :dets.info(:ordered_table, :type) == :set

      # Cleanup
      DetsAdapter.delete_table(table)
    end

    test "handles storage directory creation" do
      deep_dir = Path.join([@test_dir, "deep", "nested", "dir"])
      table = DetsAdapter.create_table(:deep_table, storage_dir: deep_dir)

      file_path = Path.join(deep_dir, "deep_table.dets")
      assert File.exists?(file_path)

      # Cleanup
      DetsAdapter.delete_table(table)
    end
  end

  describe "insert and lookup operations" do
    setup do
      table = DetsAdapter.create_table(:test_ops, storage_dir: @test_dir)
      on_exit(fn -> DetsAdapter.delete_table(table) end)
      {:ok, table: table}
    end

    test "inserts and retrieves single value", %{table: table} do
      assert :ok = DetsAdapter.insert(table, {"key1", "value1"})

      assert DetsAdapter.lookup(table, "key1") == ["value1"]
      assert DetsAdapter.lookup(table, "nonexistent") == []
    end

    test "inserts and retrieves multiple values in bag table", %{table: table} do
      # Note: DETS supports :bag type
      bag_table =
        DetsAdapter.create_table(:bag_table,
          storage_dir: @test_dir,
          type: :bag
        )

      assert :ok = DetsAdapter.insert(bag_table, {"key1", "value1"})
      assert :ok = DetsAdapter.insert(bag_table, {"key1", "value2"})

      values = DetsAdapter.lookup(bag_table, "key1")
      assert length(values) == 2
      assert "value1" in values
      assert "value2" in values

      # Cleanup
      DetsAdapter.delete_table(bag_table)
    end

    test "deletes keys", %{table: table} do
      DetsAdapter.insert(table, {"key1", "value1"})
      DetsAdapter.insert(table, {"key2", "value2"})

      assert :ok = DetsAdapter.delete(table, "key1")
      assert DetsAdapter.lookup(table, "key1") == []
      assert DetsAdapter.lookup(table, "key2") == ["value2"]
    end

    test "persists data between table opens", %{table: table} do
      # Insert data
      DetsAdapter.insert(table, {"persistent_key", "persistent_value"})

      # Close table
      :dets.close(table)

      # Reopen table with same name and storage
      new_table = DetsAdapter.create_table(:test_ops, storage_dir: @test_dir)

      # Data should still be there
      assert DetsAdapter.lookup(new_table, "persistent_key") == ["persistent_value"]
    end
  end

  describe "table management operations" do
    test "lists all entries" do
      table = DetsAdapter.create_table(:list_test, storage_dir: @test_dir)

      DetsAdapter.insert(table, {"key1", "value1"})
      DetsAdapter.insert(table, {"key2", "value2"})
      DetsAdapter.insert(table, {"key3", "value3"})

      entries = DetsAdapter.list_all(table)
      assert length(entries) == 3
      assert {"key1", "value1"} in entries
      assert {"key2", "value2"} in entries
      assert {"key3", "value3"} in entries

      # Cleanup
      DetsAdapter.delete_table(table)
    end

    test "clears table data" do
      table = DetsAdapter.create_table(:clear_test, storage_dir: @test_dir)

      DetsAdapter.insert(table, {"key1", "value1"})
      DetsAdapter.insert(table, {"key2", "value2"})

      assert DetsAdapter.size(table) == 2
      assert :ok = DetsAdapter.clear(table)
      assert DetsAdapter.size(table) == 0
      assert DetsAdapter.list_all(table) == []

      # Cleanup
      DetsAdapter.delete_table(table)
    end

    test "reports table size" do
      table = DetsAdapter.create_table(:size_test, storage_dir: @test_dir)

      assert DetsAdapter.size(table) == 0

      DetsAdapter.insert(table, {"key1", "value1"})
      assert DetsAdapter.size(table) == 1

      DetsAdapter.insert(table, {"key2", "value2"})
      assert DetsAdapter.size(table) == 2

      DetsAdapter.delete(table, "key1")
      assert DetsAdapter.size(table) == 1

      # Cleanup
      DetsAdapter.delete_table(table)
    end

    test "provides table information" do
      table =
        DetsAdapter.create_table(:info_test,
          storage_dir: @test_dir,
          type: :set
        )

      assert DetsAdapter.table_info(table, :type) == :set
      assert is_list(DetsAdapter.table_info(table, :filename))
      assert is_integer(DetsAdapter.table_info(table, :file_size))

      # Cleanup
      DetsAdapter.delete_table(table)
    end
  end

  describe "batch operations" do
    setup do
      table = DetsAdapter.create_table(:batch_test, storage_dir: @test_dir)
      on_exit(fn -> DetsAdapter.delete_table(table) end)
      {:ok, table: table}
    end

    test "batch inserts multiple entries", %{table: table} do
      entries = [
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
      ]

      assert :ok = DetsAdapter.insert_batch(table, entries)
      assert DetsAdapter.size(table) == 3

      assert DetsAdapter.lookup(table, "key1") == ["value1"]
      assert DetsAdapter.lookup(table, "key2") == ["value2"]
      assert DetsAdapter.lookup(table, "key3") == ["value3"]
    end

    test "handles empty batch insert", %{table: table} do
      assert :ok = DetsAdapter.insert_batch(table, [])
      assert DetsAdapter.size(table) == 0
    end
  end

  describe "snapshot and restore operations" do
    setup do
      table = DetsAdapter.create_table(:snapshot_test, storage_dir: @test_dir)
      on_exit(fn -> DetsAdapter.delete_table(table) end)
      {:ok, table: table}
    end

    test "creates and restores snapshots", %{table: table} do
      # Insert test data
      DetsAdapter.insert(table, {"key1", "value1"})
      DetsAdapter.insert(table, {"key2", "value2"})
      DetsAdapter.insert(table, {"key3", "value3"})

      # Create snapshot
      {:ok, snapshot} = DetsAdapter.snapshot(table)
      assert is_map(snapshot)
      assert Map.has_key?(snapshot, :data)
      assert Map.has_key?(snapshot, :metadata)
      assert length(snapshot.data) == 3

      # Verify metadata
      metadata = snapshot.metadata
      assert metadata.type == :set
      assert metadata.size == 3
      assert is_integer(metadata.timestamp)

      # Clear table
      DetsAdapter.clear(table)
      assert DetsAdapter.size(table) == 0

      # Restore from snapshot
      assert :ok = DetsAdapter.restore(table, snapshot)
      assert DetsAdapter.size(table) == 3
      assert DetsAdapter.lookup(table, "key1") == ["value1"]
      assert DetsAdapter.lookup(table, "key2") == ["value2"]
      assert DetsAdapter.lookup(table, "key3") == ["value3"]
    end

    test "restores from simple list format for backward compatibility", %{table: table} do
      data = [{"key1", "value1"}, {"key2", "value2"}]

      assert :ok = DetsAdapter.restore(table, data)
      assert DetsAdapter.size(table) == 2
      assert DetsAdapter.lookup(table, "key1") == ["value1"]
      assert DetsAdapter.lookup(table, "key2") == ["value2"]
    end

    test "handles invalid snapshot format", %{table: table} do
      assert {:error, :invalid_snapshot_format} = DetsAdapter.restore(table, "invalid")
      assert {:error, :invalid_snapshot_format} = DetsAdapter.restore(table, %{invalid: "format"})
    end
  end

  describe "file management and persistence" do
    test "deletes table and file" do
      table = DetsAdapter.create_table(:delete_test, storage_dir: @test_dir)
      file_path = Path.join(@test_dir, "delete_test.dets")

      # Verify file exists
      assert File.exists?(file_path)

      # Delete table
      assert :ok = DetsAdapter.delete_table(table)

      # Verify file is deleted
      refute File.exists?(file_path)
    end

    test "handles concurrent access" do
      # Create table and insert data
      table1 = DetsAdapter.create_table(:concurrent_test, storage_dir: @test_dir)
      DetsAdapter.insert(table1, {"shared_key", "value1"})

      # Close first table
      :dets.close(table1)

      # Open same table from different "process" 
      table2 = DetsAdapter.create_table(:concurrent_test, storage_dir: @test_dir)

      # Should see the data from first table
      assert DetsAdapter.lookup(table2, "shared_key") == ["value1"]

      # Add more data
      DetsAdapter.insert(table2, {"new_key", "value2"})

      # Close and reopen again
      :dets.close(table2)
      table3 = DetsAdapter.create_table(:concurrent_test, storage_dir: @test_dir)

      # Should see all data
      assert DetsAdapter.lookup(table3, "shared_key") == ["value1"]
      assert DetsAdapter.lookup(table3, "new_key") == ["value2"]

      # Cleanup
      DetsAdapter.delete_table(table3)
    end
  end

  describe "error handling and edge cases" do
    test "handles invalid storage directory" do
      # Try to create table in a file path instead of directory
      invalid_file = Path.join(@test_dir, "not_a_directory.txt")
      File.write!(invalid_file, "content")

      # This should either raise ArgumentError or return error tuple
      # The exact behavior depends on the file system and DETS implementation
      try do
        DetsAdapter.create_table(:invalid_dir_test, storage_dir: invalid_file)
        flunk("Expected an error when creating table in invalid directory")
      rescue
        # Expected behavior
        ArgumentError -> :ok
        # Also acceptable - indicates DETS returned error tuple
        MatchError -> :ok
      end
    end

    test "handles various data types as keys and values" do
      table = DetsAdapter.create_table(:types_test, storage_dir: @test_dir)

      # Test different key/value types
      test_cases = [
        {1, "integer_key"},
        {"string", 42},
        {:atom, %{map: "value"}},
        {{:tuple, :key}, [:list, :value]}
      ]

      Enum.each(test_cases, fn {key, value} ->
        assert :ok = DetsAdapter.insert(table, {key, value})
        assert DetsAdapter.lookup(table, key) == [value]
      end)

      assert DetsAdapter.size(table) == length(test_cases)

      # Cleanup
      DetsAdapter.delete_table(table)
    end

    test "handles auto-sync configuration" do
      # Create table with auto-sync disabled
      table =
        DetsAdapter.create_table(:sync_test,
          storage_dir: @test_dir,
          auto_sync: false
        )

      # Insert data
      DetsAdapter.insert(table, {"sync_key", "sync_value"})

      # Force manual sync by creating snapshot (which calls :dets.sync)
      {:ok, _snapshot} = DetsAdapter.snapshot(table)

      # Data should still be there
      assert DetsAdapter.lookup(table, "sync_key") == ["sync_value"]

      # Cleanup
      DetsAdapter.delete_table(table)
    end
  end
end
