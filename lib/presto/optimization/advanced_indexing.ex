defmodule Presto.Optimization.AdvancedIndexing do
  @moduledoc """
  Advanced multi-level indexing for complex joins in RETE networks.

  This module implements RETE-II/RETE-NT style advanced indexing techniques
  including hierarchical indexing, composite key indexing, and adaptive
  index selection based on data characteristics.
  """

  @type index_type :: :hash | :btree | :composite | :hierarchical
  @type index_config :: %{
          type: index_type(),
          keys: [atom()],
          threshold: non_neg_integer(),
          max_size: non_neg_integer(),
          rebuild_frequency: non_neg_integer()
        }

  @type index_stats :: %{
          lookups: non_neg_integer(),
          hits: non_neg_integer(),
          misses: non_neg_integer(),
          rebuilds: non_neg_integer(),
          last_rebuild: integer(),
          average_lookup_time: float()
        }

  @type multi_level_index :: %{
          primary_index: :ets.tab(),
          secondary_indexes: %{atom() => :ets.tab()},
          composite_indexes: %{[atom()] => :ets.tab()},
          index_metadata: %{
            config: index_config(),
            stats: index_stats(),
            last_update: integer()
          }
        }

  # Client API

  @spec create_multi_level_index([atom()]) :: multi_level_index()
  @spec create_multi_level_index([atom()], index_config() | nil) :: multi_level_index()
  def create_multi_level_index(join_keys, config \\ nil) do
    default_config = %{
      type: :composite,
      keys: join_keys,
      threshold: 100,
      max_size: 50_000,
      rebuild_frequency: 1000
    }

    merged_config = if config, do: Map.merge(default_config, config), else: default_config

    # Create primary index
    primary_index = :ets.new(:primary_index, [:set, :private])

    # Create secondary indexes for individual keys
    secondary_indexes =
      Enum.reduce(join_keys, %{}, fn key, acc ->
        secondary_table = :ets.new(:"secondary_#{key}", [:bag, :private])
        Map.put(acc, key, secondary_table)
      end)

    # Create composite indexes for key combinations
    composite_indexes = create_composite_indexes(join_keys)

    %{
      primary_index: primary_index,
      secondary_indexes: secondary_indexes,
      composite_indexes: composite_indexes,
      index_metadata: %{
        config: merged_config,
        stats: init_stats(),
        last_update: System.monotonic_time(:millisecond)
      }
    }
  end

  @spec index_data(multi_level_index(), [map()]) :: multi_level_index()
  def index_data(index, data) when is_list(data) do
    start_time = System.monotonic_time(:microsecond)

    # Clear existing indexes
    clear_all_indexes(index)

    # Build all index levels
    updated_index =
      Enum.reduce(data, index, fn record, acc_index ->
        index_single_record(acc_index, record)
      end)

    # Update statistics
    indexing_time = System.monotonic_time(:microsecond) - start_time
    update_indexing_stats(updated_index, length(data), indexing_time)
  end

  @spec lookup_with_advanced_index(multi_level_index(), map()) :: [map()]
  def lookup_with_advanced_index(index, lookup_criteria) do
    start_time = System.monotonic_time(:microsecond)

    # Choose optimal index strategy based on lookup criteria
    strategy = choose_lookup_strategy(index, lookup_criteria)

    # Perform lookup using chosen strategy
    results =
      case strategy do
        :primary -> lookup_primary_index(index, lookup_criteria)
        :secondary -> lookup_secondary_indexes(index, lookup_criteria)
        :composite -> lookup_composite_indexes(index, lookup_criteria)
        :hierarchical -> lookup_hierarchical(index, lookup_criteria)
        :full_scan -> lookup_full_scan(index, lookup_criteria)
      end

    # Update lookup statistics
    lookup_time = System.monotonic_time(:microsecond) - start_time
    update_lookup_stats(index, strategy, length(results), lookup_time)

    results
  end

  @spec rebuild_indexes_if_needed(multi_level_index(), [map()]) :: multi_level_index()
  def rebuild_indexes_if_needed(index, current_data) do
    config = index.index_metadata.config
    stats = index.index_metadata.stats

    should_rebuild =
      stats.lookups > 0 and
        (rem(stats.lookups, config.rebuild_frequency) == 0 or
           should_rebuild_based_on_performance?(stats))

    if should_rebuild do
      rebuild_indexes(index, current_data)
    else
      index
    end
  end

  @spec get_index_statistics(multi_level_index()) :: index_stats()
  def get_index_statistics(index) do
    index.index_metadata.stats
  end

  @spec optimize_index_configuration(multi_level_index(), [map()]) :: index_config()
  def optimize_index_configuration(index, sample_data) do
    current_config = index.index_metadata.config
    stats = index.index_metadata.stats

    # Analyze data characteristics
    data_analysis = analyze_data_characteristics(sample_data, current_config.keys)

    # Optimize based on performance statistics and data characteristics
    optimized_config = %{
      current_config
      | type: choose_optimal_index_type(data_analysis, stats),
        threshold: optimize_threshold(data_analysis, stats),
        max_size: optimize_max_size(data_analysis)
    }

    optimized_config
  end

  # Private functions

  defp create_composite_indexes(join_keys) do
    # Create indexes for all meaningful key combinations
    key_combinations = generate_key_combinations(join_keys)

    Enum.reduce(key_combinations, %{}, fn key_combo, acc ->
      if length(key_combo) > 1 and length(key_combo) <= 3 do
        table_name = :"composite_#{Enum.join(key_combo, "_")}"
        composite_table = :ets.new(table_name, [:set, :private])
        Map.put(acc, key_combo, composite_table)
      else
        acc
      end
    end)
  end

  defp generate_key_combinations(keys) do
    # Generate all useful combinations of keys (2-3 keys typically)
    max_combo_size = min(3, length(keys))

    if max_combo_size >= 2 do
      for combo_size <- 2..max_combo_size,
          combo <- combinations(keys, combo_size) do
        combo
      end
    else
      []
    end
  end

  defp combinations(list, k) when k > length(list), do: []
  defp combinations([], _k), do: [[]]
  defp combinations(_list, 0), do: [[]]

  defp combinations([h | t], k) do
    for(combo <- combinations(t, k - 1), do: [h | combo]) ++ combinations(t, k)
  end

  defp clear_all_indexes(index) do
    # Clear primary index
    :ets.delete_all_objects(index.primary_index)

    # Clear secondary indexes
    Enum.each(index.secondary_indexes, fn {_key, table} ->
      :ets.delete_all_objects(table)
    end)

    # Clear composite indexes
    Enum.each(index.composite_indexes, fn {_keys, table} ->
      :ets.delete_all_objects(table)
    end)
  end

  defp index_single_record(index, record) do
    config = index.index_metadata.config

    # Generate primary key (hash of all join key values)
    primary_key = generate_primary_key(record, config.keys)

    # Index in primary index
    :ets.insert(index.primary_index, {primary_key, record})

    # Index in secondary indexes (individual keys)
    Enum.each(config.keys, fn key ->
      if Map.has_key?(record, key) do
        value = Map.get(record, key)
        secondary_table = Map.get(index.secondary_indexes, key)

        if secondary_table do
          :ets.insert(secondary_table, {value, record})
        end
      end
    end)

    # Index in composite indexes
    Enum.each(index.composite_indexes, fn {key_combo, table} ->
      if Enum.all?(key_combo, &Map.has_key?(record, &1)) do
        composite_key = Enum.map(key_combo, &Map.get(record, &1))
        :ets.insert(table, {composite_key, record})
      end
    end)

    index
  end

  defp generate_primary_key(record, join_keys) do
    join_key_values = Enum.map(join_keys, &Map.get(record, &1, :undefined))
    :crypto.hash(:sha256, :erlang.term_to_binary(join_key_values))
  end

  defp choose_lookup_strategy(index, lookup_criteria) do
    config = index.index_metadata.config
    criteria_keys = Map.keys(lookup_criteria)

    cond do
      # All join keys present - use primary index
      Enum.all?(config.keys, &(&1 in criteria_keys)) ->
        :primary

      # Multiple keys match composite indexes
      has_composite_match?(index, criteria_keys) ->
        :composite

      # Single key matches secondary index
      length(criteria_keys) == 1 and List.first(criteria_keys) in config.keys ->
        :secondary

      # Multiple levels of filtering needed
      length(criteria_keys) > 1 ->
        :hierarchical

      # Fallback to full scan
      true ->
        :full_scan
    end
  end

  defp has_composite_match?(index, criteria_keys) do
    Map.keys(index.composite_indexes)
    |> Enum.any?(fn composite_keys ->
      Enum.all?(composite_keys, &(&1 in criteria_keys))
    end)
  end

  defp lookup_primary_index(index, lookup_criteria) do
    config = index.index_metadata.config

    # Generate primary key from lookup criteria
    primary_key = generate_primary_key(lookup_criteria, config.keys)

    case :ets.lookup(index.primary_index, primary_key) do
      [{^primary_key, record}] -> [record]
      [] -> []
    end
  end

  defp lookup_secondary_indexes(index, lookup_criteria) do
    # Use the first available secondary index
    {key, value} =
      Enum.find(lookup_criteria, fn {k, _v} ->
        Map.has_key?(index.secondary_indexes, k)
      end)

    secondary_table = Map.get(index.secondary_indexes, key)

    case :ets.lookup(secondary_table, value) do
      results -> Enum.map(results, fn {_key, record} -> record end)
    end
  end

  defp lookup_composite_indexes(index, lookup_criteria) do
    # Find the best matching composite index
    best_composite =
      Map.keys(index.composite_indexes)
      |> Enum.filter(fn composite_keys ->
        Enum.all?(composite_keys, &Map.has_key?(lookup_criteria, &1))
      end)
      |> Enum.max_by(&length/1, fn -> nil end)

    if best_composite do
      composite_table = Map.get(index.composite_indexes, best_composite)
      composite_key = Enum.map(best_composite, &Map.get(lookup_criteria, &1))

      case :ets.lookup(composite_table, composite_key) do
        results -> Enum.map(results, fn {_key, record} -> record end)
      end
    else
      []
    end
  end

  defp lookup_hierarchical(index, lookup_criteria) do
    # Start with most selective secondary index, then filter
    config = index.index_metadata.config
    available_keys = Enum.filter(config.keys, &Map.has_key?(lookup_criteria, &1))

    if Enum.empty?(available_keys) do
      []
    else
      # Use first available key for initial filtering
      first_key = List.first(available_keys)
      first_value = Map.get(lookup_criteria, first_key)

      # Get candidates from secondary index
      secondary_table = Map.get(index.secondary_indexes, first_key)

      candidates =
        case :ets.lookup(secondary_table, first_value) do
          results -> Enum.map(results, fn {_key, record} -> record end)
        end

      # Filter candidates by remaining criteria
      remaining_criteria = Map.delete(lookup_criteria, first_key)
      filter_candidates(candidates, remaining_criteria)
    end
  end

  defp lookup_full_scan(index, lookup_criteria) do
    # Fallback: scan primary index and filter
    all_records =
      :ets.foldl(
        fn {_key, record}, acc ->
          [record | acc]
        end,
        [],
        index.primary_index
      )

    filter_candidates(all_records, lookup_criteria)
  end

  defp filter_candidates(candidates, criteria) do
    Enum.filter(candidates, fn record ->
      Enum.all?(criteria, fn {key, value} ->
        Map.get(record, key) == value
      end)
    end)
  end

  defp init_stats() do
    %{
      lookups: 0,
      hits: 0,
      misses: 0,
      rebuilds: 0,
      last_rebuild: System.monotonic_time(:millisecond),
      average_lookup_time: 0.0
    }
  end

  defp update_indexing_stats(index, _record_count, _indexing_time) do
    current_stats = index.index_metadata.stats

    updated_stats = %{
      current_stats
      | rebuilds: current_stats.rebuilds + 1,
        last_rebuild: System.monotonic_time(:millisecond)
    }

    put_in(index, [:index_metadata, :stats], updated_stats)
  end

  defp update_lookup_stats(index, _strategy, result_count, lookup_time) do
    current_stats = index.index_metadata.stats

    # Update running average of lookup time
    new_avg_time =
      if current_stats.lookups > 0 do
        (current_stats.average_lookup_time * current_stats.lookups + lookup_time) /
          (current_stats.lookups + 1)
      else
        lookup_time
      end

    updated_stats = %{
      current_stats
      | lookups: current_stats.lookups + 1,
        hits: current_stats.hits + if(result_count > 0, do: 1, else: 0),
        misses: current_stats.misses + if(result_count == 0, do: 1, else: 0),
        average_lookup_time: new_avg_time
    }

    put_in(index, [:index_metadata, :stats], updated_stats)
  end

  defp should_rebuild_based_on_performance?(stats) do
    # Rebuild if hit rate is low or lookup times are degrading
    hit_rate = if stats.lookups > 0, do: stats.hits / stats.lookups, else: 1.0
    # 1ms threshold
    hit_rate < 0.7 or stats.average_lookup_time > 1000.0
  end

  defp rebuild_indexes(index, current_data) do
    # Optimize configuration and rebuild
    optimized_config = optimize_index_configuration(index, current_data)

    updated_index = put_in(index, [:index_metadata, :config], optimized_config)
    index_data(updated_index, current_data)
  end

  defp analyze_data_characteristics(data, join_keys) do
    data_size = length(data)

    # Analyze key cardinality and distribution
    key_analysis =
      Enum.reduce(join_keys, %{}, fn key, acc ->
        values = Enum.map(data, &Map.get(&1, key)) |> Enum.reject(&is_nil/1)
        unique_values = Enum.uniq(values) |> length()

        Map.put(acc, key, %{
          cardinality: unique_values,
          selectivity: if(data_size > 0, do: unique_values / data_size, else: 1.0),
          has_nulls: Enum.any?(data, &is_nil(Map.get(&1, key)))
        })
      end)

    %{
      data_size: data_size,
      key_analysis: key_analysis,
      complexity_score: calculate_complexity_score(key_analysis)
    }
  end

  defp calculate_complexity_score(key_analysis) do
    # Calculate a complexity score based on cardinality and selectivity
    Enum.reduce(key_analysis, 0.0, fn {_key, analysis}, acc ->
      acc + analysis.cardinality * analysis.selectivity
    end)
  end

  defp choose_optimal_index_type(data_analysis, stats) do
    cond do
      data_analysis.data_size < 100 -> :hash
      data_analysis.complexity_score > 1000 -> :hierarchical
      stats.average_lookup_time > 500.0 -> :composite
      true -> :hash
    end
  end

  defp optimize_threshold(data_analysis, _stats) do
    # Optimize threshold based on data size
    max(10, div(data_analysis.data_size, 100))
  end

  defp optimize_max_size(data_analysis) do
    # Set max size to 5x current data size
    max(1000, data_analysis.data_size * 5)
  end
end
