defmodule Presto.Utils do
  @moduledoc """
  Shared utility functions for the Presto RETE engine.

  This module consolidates common functionality used across multiple components
  to reduce code duplication and ensure consistency.
  """

  @doc """
  Determines if an atom represents a literal value rather than a variable.

  Literal atoms include:
  - Atoms starting with uppercase (likely modules/types)
  - Common literal values: true, false, nil, :ok, :error
  - Fact types: :person, :employment, :company, :number

  This function is used in pattern matching to distinguish between variables
  and literal values in RETE patterns.

  ## Examples

      iex> Presto.Utils.is_literal_atom?(:Person)
      true
      
      iex> Presto.Utils.is_literal_atom?(:person)
      true
      
      iex> Presto.Utils.is_literal_atom?(:variable_name)
      false
      
      iex> Presto.Utils.is_literal_atom?(:ok)
      true
  """
  @spec is_literal_atom?(atom()) :: boolean()
  def is_literal_atom?(atom) when is_atom(atom) do
    # Consider atoms that start with uppercase or are common literals as literals
    str = Atom.to_string(atom)

    case str do
      <<first::utf8, _rest::binary>> when first >= ?A and first <= ?Z ->
        # Starts with uppercase, likely a module/literal
        true

      _ ->
        # Check if it's a common literal  
        atom in [true, false, nil, :ok, :error]
    end
  end

  @doc """
  Determines if an atom represents a variable in pattern matching.

  Variables are atoms that are not wildcard (:_) and not literal atoms.

  ## Examples

      iex> Presto.Utils.variable?(:name)
      true
      
      iex> Presto.Utils.variable?(:_)
      false
      
      iex> Presto.Utils.variable?(:Person)
      false
  """
  @spec variable?(atom()) :: boolean()
  def variable?(atom) when is_atom(atom) do
    # Variables are atoms that are not :_ and not literals
    atom != :_ and not is_literal_atom?(atom)
  end

  def variable?(_), do: false
end
