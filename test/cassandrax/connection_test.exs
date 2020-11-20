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

    test "defined fields to be selected" do
      queryable = TestSchema |> select([:id, :order_id, :map])
      statement = Cassandrax.Connection.all(TestKeyspace, queryable) |> to_string()
      assert statement =~ ~r/SELECT "id", "order_id", "map" FROM "test_keyspace"."my_table"/
    end

    test "defined where clauses" do
      queryable = TestSchema |> where(:id == "abc123") |> where(:order_id < 100)
      statement = Cassandrax.Connection.all(TestKeyspace, queryable) |> to_string()
      assert statement =~ ~r/WHERE \("order_id" < \?\) AND \("id" = \?\)/
    end

    test "defined distinct fields" do
      queryable = TestSchema |> distinct([:id, :order_id])
      statement = Cassandrax.Connection.all(TestKeyspace, queryable) |> to_string()
      assert statement =~ ~r/SELECT DISTINCT\("id", "order_id"\)/
    end
  end
end
