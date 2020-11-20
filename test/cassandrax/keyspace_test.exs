Code.require_file("../support/data_case.exs", __DIR__)

defmodule TestData do
  use Cassandrax.Schema

  import Ecto.Changeset
  import Cassandrax.Query

  alias Cassandrax.TestConn

  @type t :: %__MODULE__{}
  @primary_key [:id]

  table "test_data" do
    field :id, :string
    field :value, :string
    field :svalue, MapSetType
  end

  def changeset(%__MODULE__{} = data, attrs \\ %{}) do
    data
    |> cast(attrs, [:id, :value])
    |> validate_required([:id])
  end

  def create_table do
    statement = [
      "CREATE TABLE IF NOT EXISTS ",
      "#{TestKeyspace.__keyspace__()}.test_data(",
      "id text, ",
      "value text, ",
      "svalue set<text>, ",
      "PRIMARY KEY (id))"
    ]

    {:ok, _result} = Cassandrax.cql(TestConn, statement)
  end

  def drop_table do
    statement = "DROP TABLE IF EXISTS #{TestKeyspace.__keyspace__()}.test_data"

    {:ok, _result} = Cassandrax.cql(TestConn, statement)
  end
end

defmodule Cassandrax.KeyspaceTest do
  use Cassandrax.DataCase

  import Ecto.Changeset
  import Cassandrax.Query

  alias Cassandrax.TestConn
  alias Cassandrax.Schema
  alias Cassandrax.Keyspace.Batch
  alias Ecto.Changeset

  setup context do
    TestData.create_table()
    on_exit(fn -> TestData.drop_table() end)
    seeds = Map.get(context, :seeds, [])
    Enum.each(seeds, &TestKeyspace.insert!(&1))
    :ok
  end

  # TODO: override Kernel.== or use https://github.com/coingaming/comparable
  defp schema_equal({:ok, a}, {:ok, b}), do: schema_equal(a, b)
  defp schema_equal(a, b) do
    Map.delete(a, :__meta__) == Map.delete(b, :__meta__)
  end

  defp list_sets_equal?(a, b) do
    MapSet.equal?(MapSet.new(a), MapSet.new(b))
  end

  defp list_set_includes?(a, b) do
    MapSet.subset?(MapSet.new(b), MapSet.new(a))
  end

  @zero %TestData{id: "0", value: "zero"}
  @one %TestData{id: "1", value: "one", svalue: MapSet.new(["one", "another one"])}
  @two %TestData{id: "2", value: "two"}
  @three %TestData{id: "3", value: "three"}
  @four %TestData{id: "4", value: "four"}
  # invalid because 1 is an integer
  @invalid_one %TestData{id: "1", value: 1}

  describe "changeset operations" do
    @describetag seeds: [@zero]

    test "insert valid data" do
      assert schema_equal(TestKeyspace.insert(@one), {:ok, @one})
    end

    test "insert! valid data" do
      assert schema_equal(TestKeyspace.insert!(@one), @one)
    end

    @tag :pending
    test "insert invalid data" do
      assert {:error, :invalid_data} == TestKeyspace.insert(@invalid_one)
    end

    @tag :pending
    test "insert! invalid data" do
      try do
        TestKeyspace.insert!(@invalid_one)
      rescue
        _ in Cassandrax.InvalidDataError -> nil #made up error
      end
    end

    test "update" do
      changeset = Changeset.change(@zero, value: "ZERO")
      expectation = %{@zero | value: "ZERO"}
      assert schema_equal(TestKeyspace.update(changeset), {:ok, expectation})
      assert schema_equal(TestKeyspace.get(TestData, id: "0"), expectation)
    end

    test "update!" do
      changeset = Changeset.change(@zero, value: "ZERO")
      expectation = %{@zero | value: "ZERO"}
      assert schema_equal(TestKeyspace.update!(changeset), expectation)
      assert schema_equal(TestKeyspace.get(TestData, id: "0"), expectation)
    end

    test "delete" do
      assert schema_equal(TestKeyspace.delete(@zero), {:ok, @zero})
      assert TestKeyspace.get(TestData, id: "0") == nil
    end

    test "delete!" do
      assert schema_equal(TestKeyspace.delete!(@zero), @zero)
      assert TestKeyspace.get(TestData, id: "0") == nil
    end
  end

  describe "queryables" do
    @describetag seeds: [@zero, @one]

    test "get" do
      assert TestKeyspace.get(TestData, id: "0") == @zero
      assert TestKeyspace.get(TestData, id: "3") == nil
    end

    test "all" do
      assert list_sets_equal?(TestKeyspace.all(TestData), [@zero, @one])
    end
  end

  describe "one" do
    test "fails on table with no entries" do
      assert TestKeyspace.one(TestData) == nil
    end

    test "from table with one entry" do
      TestKeyspace.insert!(@zero)

      assert TestKeyspace.one(TestData) == @zero
    end

    test "from table with multiple entries" do
      TestKeyspace.insert!(@zero)
      TestKeyspace.insert!(@one)

      assert TestKeyspace.one(where(TestData, id: "0")) == @zero
      assert TestKeyspace.one(where(TestData, id: "1")) == @one
      try do
        TestKeyspace.one(TestData)
      rescue
        _ in Cassandrax.MultipleResultsError -> nil
      end
    end
  end

  #TODO: test batch options [:logged, :unlogged]
  describe "batch operations" do
    @describetag seeds: [@zero, @one]

    test "empty batch" do
      TestKeyspace.batch(fn batch -> batch end)
    end

    test "single insert" do
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_insert(@two)
      end)

      assert TestKeyspace.get(TestData, id: "2") == @two
    end

    @doc """
    In Cassandra, INSERT inserts an entire row or upserts data into an existing row
    https://docs.datastax.com/en/archived/ddaccql/doc/cql/cql_reference/cql_commands/cqlInsert.html
    """
    test "duplicate key insert" do
      expectation = %{@two | value: "new two"}
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_insert(expectation)
      end)
      assert TestKeyspace.one(where(TestData, id: "2")) == expectation
    end

    test "multiple inserts" do
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_insert(@three)
        |> TestKeyspace.batch_insert(@four)
      end)

      assert list_set_includes?(TestKeyspace.all(TestData), [@three, @four])
    end

    test "single update" do
      changeset = TestKeyspace.get(TestData, id: "0") |> Changeset.change(value: "new zero")
      expectation = %{@zero | value: "new zero"}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset)
      end)

      assert TestKeyspace.one(where(TestData, id: "0")) == expectation
    end

    test "multiple updates" do
      changeset1 = TestKeyspace.get(TestData, id: "0") |> Changeset.change(value: "new zero")
      expectation1 = %TestData{@zero | value: "new zero"}
      changeset2 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "new one")
      expectation2 = %TestData{@one | value: "new one"}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset1)
        |> TestKeyspace.batch_update(changeset2)
      end)

      assert list_set_includes?(TestKeyspace.all(TestData), [expectation1, expectation2])
    end

    @doc """
    Statement order does not matter within a batch;
    Cassandra applies all rows using the same timestamp. Use client-supplied timestamps to achieve a particular order.
    https://docs.datastax.com/en/archived/cql/3.1/cql/cql_reference/batch_r.html
    """
    # @tag :pending
    test "update scalar data of the same records" do
      changeset1 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "new one")
      changeset2 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "last one")
      changeset3 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "one last one")
      expectation = ["new one", "last one", "one last one"] #%{@one | value: "one last one"}
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset1)
        |> TestKeyspace.batch_update(changeset2)
        |> TestKeyspace.batch_update(changeset3)
      end)
      assert TestKeyspace.one(where(TestData, id: "1")).value in expectation
    end

    test "update set data of the same records" do
      changeset1 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(svalue: MapSet.new(["hello world"]))
      changeset2 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(svalue: MapSet.new(["pandemic world"]))
      expectation2 = %{@one | svalue: MapSet.new(["hello world", "pandemic world"])}
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset1)
        |> TestKeyspace.batch_update(changeset2)
      end)
      assert TestKeyspace.one(where(TestData, id: "1")) == expectation2
    end

    test "single delete" do
      TestKeyspace.batch(fn batch ->
        data = TestKeyspace.get(TestData, id: "0")
        batch
        |> TestKeyspace.batch_delete(data)
      end)

      assert TestKeyspace.one(where(TestData, id: "0")) == nil
    end

    test "multiple deletes" do
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_delete(@zero)
        |> TestKeyspace.batch_delete(@one)
      end)

      refute list_set_includes?(TestKeyspace.all(TestData), [@zero,  @one])
    end

    test "insert and update" do
      changeset = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "new one")
      expectation = %{@one | value: "new one"}
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_insert(@four)
        |> TestKeyspace.batch_update(changeset)
      end)

      assert list_set_includes?(TestKeyspace.all(TestData), [@four, expectation])
    end

    test "delete then update" do
      changeset = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "new one")
      expectation = [@one, %{@one | value: "new one"}, nil]
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_delete(@one)
        |> TestKeyspace.batch_update(changeset)
      end)

      assert TestKeyspace.one(where(TestData, id: "1")) in expectation
    end
   end

  describe "cql" do
    @describetag seeds: [@zero, @one, @two]

    test "valid cql" do
      statement = """
      SELECT * FROM #{TestKeyspace.__keyspace__()}.test_data
      """
      assert {:ok, _} = Cassandrax.cql(TestConn, statement)

      statement = [
        "SELECT value FROM ",
        "#{TestKeyspace.__keyspace__()}.test_data ",
        "WHERE id = '1'"
      ]
      assert {:ok, _} = Cassandrax.cql(TestConn, statement)
    end

    test "invalid cql" do
      statement = """
      SELECT * #{TestKeyspace.__keyspace__()}.test_data
      """
      assert {:error, %{reason: :invalid_syntax}} = Cassandrax.cql(TestConn, statement)
    end
  end
end
