defmodule Presto.UtilsTest do
  use ExUnit.Case, async: true

  alias Presto.Utils

  # doctest Presto.Utils  # Disabled for manual testing

  describe "is_literal_atom?/1" do
    test "identifies uppercase atoms as literals" do
      assert Utils.is_literal_atom?(:Person)
      assert Utils.is_literal_atom?(:Module)
      assert Utils.is_literal_atom?(:SomeModule)
    end

    test "identifies common literal atoms" do
      assert Utils.is_literal_atom?(true)
      assert Utils.is_literal_atom?(false)
      assert Utils.is_literal_atom?(nil)
      assert Utils.is_literal_atom?(:ok)
      assert Utils.is_literal_atom?(:error)
    end

    test "does not treat fact types as automatic literals" do
      # Fact types like :person, :employment are not automatically literals
      # They are determined by pattern position, not inherent type
      refute Utils.is_literal_atom?(:person)
      refute Utils.is_literal_atom?(:employment)
      refute Utils.is_literal_atom?(:company)
      refute Utils.is_literal_atom?(:number)
    end

    test "identifies variable atoms correctly" do
      refute Utils.is_literal_atom?(:name)
      refute Utils.is_literal_atom?(:age)
      refute Utils.is_literal_atom?(:variable_name)
      refute Utils.is_literal_atom?(:some_var)
    end

    test "handles wildcard correctly" do
      refute Utils.is_literal_atom?(:_)
    end
  end

  describe "variable?/1" do
    test "identifies variables correctly" do
      assert Utils.variable?(:name)
      assert Utils.variable?(:age)
      assert Utils.variable?(:variable_name)
      assert Utils.variable?(:some_var)
    end

    test "does not identify wildcard as variable" do
      refute Utils.variable?(:_)
    end

    test "does not identify literals as variables" do
      # Uppercase = literal
      refute Utils.variable?(:Person)
      # Common literal
      refute Utils.variable?(:ok)
      # Common literal
      refute Utils.variable?(true)
      # Note: :person is now treated as a variable by Utils, but position matters in patterns
    end

    test "handles non-atoms correctly" do
      refute Utils.variable?("string")
      refute Utils.variable?(123)
      refute Utils.variable?([])
    end
  end
end
