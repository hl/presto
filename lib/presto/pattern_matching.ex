defmodule Presto.PatternMatching do
  @moduledoc """
  Pattern matching utilities for the Presto RETE engine.

  Provides functions to classify atoms as variables or literals
  in RETE pattern matching contexts. This focused module helps
  distinguish between different types of atoms during rule compilation
  and pattern analysis.
  """

  @doc """
  Determines if an atom represents a literal value rather than a variable.

  Literal atoms include:
  - Atoms starting with uppercase (likely modules/types)
  - Common literal values: true, false, nil, :ok, :error

  This function is used in pattern matching to distinguish between variables
  and literal values in RETE patterns.

  ## Examples

      iex> Presto.PatternMatching.is_literal_atom?(:Person)
      true
      iex> Presto.PatternMatching.is_literal_atom?(:person)
      false
      iex> Presto.PatternMatching.is_literal_atom?(:variable_name)
      false
      iex> Presto.PatternMatching.is_literal_atom?(:ok)
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

      iex> Presto.PatternMatching.variable?(:name)
      true
      iex> Presto.PatternMatching.variable?(:_)
      false
      iex> Presto.PatternMatching.variable?(:Person)
      false
  """
  @spec variable?(atom()) :: boolean()
  def variable?(atom) when is_atom(atom) do
    # Variables are atoms that are not :_ and not literals
    atom != :_ and not is_literal_atom?(atom)
  end

  def variable?(_), do: false
end
