defmodule Cassandrax.QueryableTest do
  @moduledoc false

  use ExUnit.Case

  describe "to_query/1" do
    test "with a query" do
      query = %Cassandrax.Query{}
      assert Cassandrax.Queryable.to_query(query) == query
    end

    defmodule TestSchema do
      use Cassandrax.Schema

      @primary_key [:id]

      table "tests" do
        field(:id, :integer)
      end
    end

    test "with a schema" do
      assert %Cassandrax.Query{from: "tests", schema: TestSchema} =
               Cassandrax.Queryable.to_query(TestSchema)
    end

    defmodule TestMod do
    end

    test "with other modules" do
      assert_raise(CompileError, " the given module does not provide a schema", fn ->
        Cassandrax.Queryable.to_query(TestMod)
      end)
    end

    test "with other atoms" do
      assert_raise(CompileError, " the given module does not exist", fn ->
        Cassandrax.Queryable.to_query(RandomAtom)
      end)
    end
  end
end
