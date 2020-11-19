defmodule Cassandrax.QueryTest do
  use ExUnit.Case, async: true

  import Cassandrax.Query
  doctest Cassandrax.Query

  defmodule TestSchema do
    use Cassandrax.Schema

    @primary_key [:id]

    table "test_schemas" do
      field(:id, :integer)
      field(:field1, :string)
      field(:field2, :boolean)
    end
  end

  describe "select/2" do
    test "returns a query with select, from and schema definitions" do
      query = select(TestSchema, [:id, :field1])

      assert %Cassandrax.Query{select: [:id, :field1]} = query
      assert %Cassandrax.Query{from: "test_schemas"} = query
      assert %Cassandrax.Query{schema: TestSchema} = query
    end
  end

  describe "where/2" do
    test "keyword syntax results in equals operator" do
      query = where(TestSchema, id: 123)

      assert %Cassandrax.Query{wheres: [[:id, :==, 123]]}
    end

    test "operator syntax" do
      query = TestSchema |> where(:id == 123) |> where(:field1 <= "string")

      assert %Cassandrax.Query{wheres: [[:field1, :<=, "string"], [:id, :==, 123]]}
    end
  end

  describe "limit/2" do
    test "returns a query with the given limit flag" do
      query = limit(TestSchema, 1234)

      assert %Cassandrax.Query{limit: 1234} = query
    end

    test "returns a query with the default limit flag" do
      query = limit(TestSchema)

      assert %Cassandrax.Query{limit: 100} = query
    end
  end

  describe "order_by/2" do
    test "returns a query with the given order by flags" do
      query = order_by(TestSchema, [:id, :field1])

      assert %Cassandrax.Query{order_bys: [:id, :field1]} = query
    end
  end

  describe "group_by/2" do
    test "returns a query with the given group by flags" do
      query = group_by(TestSchema, [:id, :field1])

      assert %Cassandrax.Query{group_bys: [:id, :field1]} = query
    end
  end

  describe "distinct/2" do
    test "returns a query with the given distinct flags" do
      query = distinct(TestSchema, [:id, :field1])

      assert %Cassandrax.Query{distinct: [:id, :field1]} = query
    end
  end

  describe "allow_filtering/1" do
    test "returns a query with the allow filtering flag" do
      query = allow_filtering(TestSchema)

      assert %Cassandrax.Query{allow_filtering: true} = query
    end
  end

  describe "per_partition_limit/2" do
    test "returns a query with the given per partition limit flag" do
      query = per_partition_limit(TestSchema, 1234)

      assert %Cassandrax.Query{per_partition_limit: 1234} = query
    end

    test "returns a query with the default per partition limit flag" do
      query = per_partition_limit(TestSchema)

      assert %Cassandrax.Query{per_partition_limit: 100} = query
    end
  end
end
