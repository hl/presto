defmodule Presto.Optimization.AdvancedIndexingTest do
  use ExUnit.Case, async: true

  alias Presto.Optimization.AdvancedIndexing

  describe "multi-level index creation" do
    test "creates multi-level index with default config" do
      join_keys = [:employee_id, :date]

      index = AdvancedIndexing.create_multi_level_index(join_keys)

      assert is_map(index)
      assert Map.has_key?(index, :primary_index)
      assert Map.has_key?(index, :secondary_indexes)
      assert Map.has_key?(index, :composite_indexes)
      assert Map.has_key?(index, :index_metadata)

      # Verify secondary indexes exist for each join key
      assert Map.has_key?(index.secondary_indexes, :employee_id)
      assert Map.has_key?(index.secondary_indexes, :date)
    end

    test "creates index with custom configuration" do
      join_keys = [:id]

      config = %{
        type: :hash,
        threshold: 50,
        max_size: 1000
      }

      index = AdvancedIndexing.create_multi_level_index(join_keys, config)

      assert index.index_metadata.config.type == :hash
      assert index.index_metadata.config.threshold == 50
      assert index.index_metadata.config.max_size == 1000
    end
  end

  describe "data indexing" do
    test "indexes data correctly" do
      join_keys = [:employee_id]
      index = AdvancedIndexing.create_multi_level_index(join_keys)

      test_data = [
        %{employee_id: 1, name: "Alice", hours: 8},
        %{employee_id: 2, name: "Bob", hours: 7},
        # Duplicate employee_id
        %{employee_id: 1, name: "Alice", hours: 6}
      ]

      updated_index = AdvancedIndexing.index_data(index, test_data)

      # Verify indexing completed without errors
      assert is_map(updated_index)
      assert updated_index.index_metadata.stats.rebuilds >= 1
    end

    test "handles empty data gracefully" do
      join_keys = [:id]
      index = AdvancedIndexing.create_multi_level_index(join_keys)

      updated_index = AdvancedIndexing.index_data(index, [])

      assert is_map(updated_index)
    end
  end

  describe "advanced lookups" do
    setup do
      join_keys = [:employee_id, :department]
      index = AdvancedIndexing.create_multi_level_index(join_keys)

      test_data = [
        %{employee_id: 1, department: "engineering", name: "Alice"},
        %{employee_id: 2, department: "engineering", name: "Bob"},
        %{employee_id: 3, department: "sales", name: "Carol"},
        # Cross-department
        %{employee_id: 1, department: "sales", name: "Alice2"}
      ]

      indexed = AdvancedIndexing.index_data(index, test_data)

      {:ok, index: indexed, data: test_data}
    end

    test "performs single key lookup", %{index: index} do
      lookup_criteria = %{employee_id: 1}

      results = AdvancedIndexing.lookup_with_advanced_index(index, lookup_criteria)

      assert is_list(results)
      assert length(results) >= 1
      # All results should have employee_id: 1
      assert Enum.all?(results, fn record -> record.employee_id == 1 end)
    end

    test "performs multi-key lookup", %{index: index} do
      lookup_criteria = %{employee_id: 1, department: "engineering"}

      results = AdvancedIndexing.lookup_with_advanced_index(index, lookup_criteria)

      assert is_list(results)
      # Should find exact matches for both criteria
      assert Enum.all?(results, fn record ->
               record.employee_id == 1 and record.department == "engineering"
             end)
    end

    test "handles lookup with no matches", %{index: index} do
      # Non-existent employee
      lookup_criteria = %{employee_id: 999}

      results = AdvancedIndexing.lookup_with_advanced_index(index, lookup_criteria)

      assert results == []
    end

    test "handles partial key lookup", %{index: index} do
      lookup_criteria = %{department: "engineering"}

      results = AdvancedIndexing.lookup_with_advanced_index(index, lookup_criteria)

      assert is_list(results)
      assert length(results) >= 1
      assert Enum.all?(results, fn record -> record.department == "engineering" end)
    end
  end

  describe "index optimization" do
    test "rebuilds indexes when needed" do
      join_keys = [:id]
      index = AdvancedIndexing.create_multi_level_index(join_keys)

      test_data = [%{id: 1, value: "test"}]
      indexed = AdvancedIndexing.index_data(index, test_data)

      updated_index = AdvancedIndexing.rebuild_indexes_if_needed(indexed, test_data)

      assert is_map(updated_index)
    end

    test "optimizes index configuration" do
      join_keys = [:employee_id]
      index = AdvancedIndexing.create_multi_level_index(join_keys)

      sample_data = [
        %{employee_id: 1, value: "a"},
        %{employee_id: 2, value: "b"}
      ]

      optimized_config = AdvancedIndexing.optimize_index_configuration(index, sample_data)

      assert is_map(optimized_config)
      assert Map.has_key?(optimized_config, :type)
      assert Map.has_key?(optimized_config, :threshold)
      assert Map.has_key?(optimized_config, :max_size)
    end
  end

  describe "index statistics" do
    test "tracks index statistics" do
      join_keys = [:id]
      index = AdvancedIndexing.create_multi_level_index(join_keys)

      stats = AdvancedIndexing.get_index_statistics(index)

      assert is_map(stats)
      assert Map.has_key?(stats, :lookups)
      assert Map.has_key?(stats, :hits)
      assert Map.has_key?(stats, :misses)
      assert Map.has_key?(stats, :rebuilds)
      assert Map.has_key?(stats, :average_lookup_time)
    end
  end
end
