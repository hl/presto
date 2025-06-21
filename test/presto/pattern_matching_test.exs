defmodule Presto.PatternMatchingTest do
  use ExUnit.Case, async: true

  alias Presto.PatternMatching

  # doctest Presto.PatternMatching  # Disabled for manual testing

  describe "is_literal_atom?/1" do
    test "identifies uppercase atoms as literals" do
      assert PatternMatching.is_literal_atom?(:Person)
      assert PatternMatching.is_literal_atom?(:Module)
      assert PatternMatching.is_literal_atom?(:SomeModule)
    end

    test "identifies common literal atoms" do
      assert PatternMatching.is_literal_atom?(true)
      assert PatternMatching.is_literal_atom?(false)
      assert PatternMatching.is_literal_atom?(nil)
      assert PatternMatching.is_literal_atom?(:ok)
      assert PatternMatching.is_literal_atom?(:error)
    end

    test "does not treat fact types as automatic literals" do
      # Fact types like :person, :employment are not automatically literals
      # They are determined by pattern position, not inherent type
      refute PatternMatching.is_literal_atom?(:person)
      refute PatternMatching.is_literal_atom?(:employment)
      refute PatternMatching.is_literal_atom?(:company)
      refute PatternMatching.is_literal_atom?(:number)
    end

    test "identifies variable atoms correctly" do
      refute PatternMatching.is_literal_atom?(:name)
      refute PatternMatching.is_literal_atom?(:age)
      refute PatternMatching.is_literal_atom?(:variable_name)
      refute PatternMatching.is_literal_atom?(:some_var)
    end

    test "handles wildcard correctly" do
      refute PatternMatching.is_literal_atom?(:_)
    end
  end

  describe "variable?/1" do
    test "identifies variables correctly" do
      assert PatternMatching.variable?(:name)
      assert PatternMatching.variable?(:age)
      assert PatternMatching.variable?(:variable_name)
      assert PatternMatching.variable?(:some_var)
    end

    test "does not identify wildcard as variable" do
      refute PatternMatching.variable?(:_)
    end

    test "does not identify literals as variables" do
      # Uppercase = literal
      refute PatternMatching.variable?(:Person)
      # Common literal
      refute PatternMatching.variable?(:ok)
      # Common literal
      refute PatternMatching.variable?(true)
      # Note: :person is now treated as a variable by Utils, but position matters in patterns
    end

    test "handles non-atoms correctly" do
      refute PatternMatching.variable?("string")
      refute PatternMatching.variable?(123)
      refute PatternMatching.variable?([])
    end
  end
end
