defmodule Cassandrax.SchemaTest do
  use ExUnit.Case, async: true

  alias Cassandrax.Query

  defmodule TestSchema do
    use Cassandrax.Schema

    @primary_key [:partition_key, :clustering_key]

    table "my table" do
      field(:partition_key, :integer)
      field(:clustering_key, :string)
      field(:field_1, :boolean)
      field(:field_2, MapSetType, default: MapSet.new(["a", "b"]))
    end
  end

  test "metadata" do
    assert TestSchema.__schema__(:pk) == [[:partition_key], [:clustering_key]]
    assert TestSchema.__schema__(:queryable) == %Query{from: "my table", schema: TestSchema}
    assert TestSchema.__schema__(:fields) == [:partition_key, :clustering_key, :field_1, :field_2]
  end

  describe "convert/1" do
    test "with nil" do
      assert TestSchema.convert(nil) == nil
    end

    test "with map" do
      map = %{
        "partition_key" => 123,
        "clustering_key" => "abc",
        "field_1" => true,
        "field_2" => MapSet.new([])
      }

      struct = TestSchema.convert(map)

      assert struct.partition_key == 123
      assert struct.clustering_key == "abc"
      assert struct.field_1
      assert struct.field_2 == MapSet.new([])
    end
  end

  describe "errors" do
    test "empty primary_key" do
      assert_raise(Cassandrax.SchemaError, ~r/You must define a @primary_key/, fn ->
        defmodule SchemaWithoutPrimaryKey do
          use Cassandrax.Schema

          table "my table" do
          end
        end
      end)

      assert_raise(Cassandrax.SchemaError, ~r/You must define a @primary_key/, fn ->
        defmodule SchemaWithoutPrimaryKey do
          use Cassandrax.Schema

          @primary_key []
          table "my table" do
          end
        end
      end)
    end

    test "partition key not in fields" do
      assert_raise(
        Cassandrax.SchemaError,
        ~r/@primary_key defines a partition key that wasn't defined in the schema/,
        fn ->
          defmodule SchemaWithNonExistingPartitionKey do
            use Cassandrax.Schema

            @primary_key [:pk]
            table "my table" do
            end
          end
        end
      )
    end

    test "partition key invalid" do
      assert_raise(Cassandrax.SchemaError, ~r/@primary_key cannot define an empty/, fn ->
        defmodule SchemaWithNonExistingPartitionKey do
          use Cassandrax.Schema

          @primary_key [[], :ck]
          table "my table" do
          end
        end
      end)
    end

    test "clustering key not in fields" do
      assert_raise(
        Cassandrax.SchemaError,
        ~r/@primary_key defines a clustering key that wasn't defined in the schema/,
        fn ->
          defmodule SchemaWithNonExistingPartitionKey do
            use Cassandrax.Schema

            @primary_key [:pk, :ck]
            table "my table" do
              field(:pk, :integer)
            end
          end
        end
      )
    end
  end
end
