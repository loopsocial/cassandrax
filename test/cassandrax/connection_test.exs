Code.require_file("../support/data_case.exs", __DIR__)

defmodule Cassandrax.ConnectionTest do
  use Cassandrax.DataCase

  describe "child_spec/1" do
    assert %{start: {Xandra.Cluster, :start_link, ["options"]}} =
             Cassandrax.Connection.child_spec("options")
  end

  defmodule TestKeyspace do
    use Cassandrax.Keyspace, cluster: Cassandrax.TestConn, name: "test_keyspace"
  end

  defmodule TestSchema do
    use Cassandrax.Schema
    import Ecto.Changeset

    @primary_key [:id, :order_id]

    table "my_table" do
      field(:id, :string)
      field(:order_id, :integer)
      field(:field, :string)
      field(:set, MapSetType)
      field(:list, {:array, :string})
      field(:map, :map)
    end

    def change(%TestSchema{} = struct, attrs) do
      struct
      |> cast(attrs, [
        :id,
        :order_id,
        :set,
        :list,
        :map
      ])
    end
  end

  import Kernel, except: [to_string: 1]
  defp to_string({iodata, _values}), do: IO.iodata_to_binary(iodata)

  describe "all/2" do
    import Cassandrax.Query

    defp all(queryable), do: Cassandrax.Connection.all(TestKeyspace, queryable) |> to_string()

    test "defined fields to be selected" do
      queryable = TestSchema |> select([:id, :order_id, :map])
      assert all(queryable) =~ ~r/SELECT "id", "order_id", "map" FROM "test_keyspace"."my_table"/
    end

    test "defined distinct fields" do
      queryable = TestSchema |> distinct([:id, :order_id])
      assert all(queryable) =~ ~r/SELECT DISTINCT "id", "order_id"/
    end

    test "defined group by clause" do
      queryable = TestSchema |> group_by([:order_id, :field])
      assert all(queryable) =~ ~r/GROUP BY "order_id", "field"/
    end

    test "defined order by clause" do
      queryable = TestSchema |> order_by([:order_id])
      assert all(queryable) =~ ~r/ORDER BY "order_id"/
    end

    test "defined per partition limit clause" do
      queryable = TestSchema |> per_partition_limit(25)
      assert all(queryable) =~ ~r/PER PARTITION LIMIT \?/
    end

    test "defined limit clause" do
      queryable = TestSchema |> limit(25)
      assert all(queryable) =~ ~r/LIMIT \?/
    end

    test "defined allow filtering clause" do
      queryable = TestSchema |> allow_filtering()
      assert all(queryable) =~ ~r/ALLOW FILTERING/
    end

    test "defined multiple where clauses" do
      queryable =
        TestSchema |> where(:id == "abc123") |> where(:order_id > 100) |> where(:order_id < 350)

      assert all(queryable) =~
               ~r/WHERE \("order_id" < \?\) AND \("order_id" > \?\) AND \("id" = \?\)/
    end

    test "defined keyword where clause" do
      queryable = TestSchema |> where(id: "abc123")

      assert all(queryable) =~ ~r/WHERE \("id" = \?\)/
    end

    test "defined keyword where clause with list as value" do
      queryable = TestSchema |> where(id: ["abc123", "def456"])

      assert all(queryable) =~ ~r/WHERE \("id" IN \?\)/
    end

    test "defined keyword where clause with list as variable" do
      list = ["abc123", "def456"]
      queryable = TestSchema |> where(id: list)
      assert all(queryable) =~ ~r/WHERE \("id" IN \?\)/
    end

    test "defined keyword where clause with multipe values as variables" do
      id = 1
      order_id = 2
      queryable = TestSchema |> where(id: id) |> where(order_id: order_id)

      assert all(queryable) =~ ~r/WHERE \("order_id" = \?\) AND \("id" = \?\)/
    end

    @tag :pending
    test "defined where clause with contains operators" do
      # queryable = TestSchema |> where(:list contains "abc123") |> where(:map contains_key "def456")
      # assert all(queryable) =~ ~r/WHERE \("map" CONTAINS KEY \?\) AND \("list" CONTAINS \?\)/
    end
  end
end
