defmodule Presto.RuleEngine.FactLineage do
  @moduledoc """
  Fact lineage and provenance tracking for the Presto Rule Engine.

  This module handles fact lineage tracking, including fact derivation
  chains, generation management, and incremental processing support.
  """

  alias Presto.RuleEngine.State

  @doc """
  Creates a unique key for a fact for lineage tracking.
  """
  @spec create_fact_key(tuple()) :: String.t()
  def create_fact_key(fact) do
    :crypto.hash(:sha256, :erlang.term_to_binary(fact)) |> Base.encode16(case: :lower)
  end

  @doc """
  Updates fact lineage tracking.
  """
  @spec update_fact_lineage(State.t(), String.t(), map()) :: State.t()
  def update_fact_lineage(%State{} = state, fact_key, lineage_info) do
    updated_lineage = Map.put(state.fact_lineage, fact_key, lineage_info)
    %{state | fact_lineage: updated_lineage, fact_generation: state.fact_generation + 1}
  end

  @doc """
  Updates facts since incremental execution.
  """
  @spec update_facts_since_incremental(State.t(), [tuple()]) :: State.t()
  def update_facts_since_incremental(%State{} = state, facts) do
    %{state | facts_since_incremental: facts}
  end

  @doc """
  Updates incremental execution timestamp.
  """
  @spec update_incremental_timestamp(State.t(), integer()) :: State.t()
  def update_incremental_timestamp(%State{} = state, timestamp) do
    %{state | last_incremental_execution: timestamp}
  end

  @doc """
  Gets fact lineage for a specific fact.
  """
  @spec get_fact_lineage(State.t(), String.t()) :: map() | nil
  def get_fact_lineage(%State{} = state, fact_key) do
    Map.get(state.fact_lineage, fact_key)
  end

  @doc """
  Gets facts since last incremental execution.
  """
  @spec get_facts_since_incremental(State.t()) :: [tuple()]
  def get_facts_since_incremental(%State{} = state), do: state.facts_since_incremental
end
