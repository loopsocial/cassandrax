Code.require_file("../support/data_case.exs", __DIR__)

defmodule Cassandrax.KeyspaceTest do
  use Cassandrax.DataCase

  import Ecto.Changeset
  import Cassandrax.Query

  alias Cassandrax.TestConn
  alias Ecto.Changeset

  defmodule TestData do
    use Cassandrax.Schema

    import Ecto.Changeset
    import Cassandrax.Query

    alias Cassandrax.TestConn

    @type t :: %__MODULE__{}
    @primary_key [:id, :timestamp]

    table "test_data" do
      field(:id, :string)
      field(:timestamp, :string)
      field(:data, :string)
      field(:svalue, MapSetType)
    end

    def changeset(%__MODULE__{} = data, attrs \\ %{}) do
      data
      |> cast(attrs, [:id, :timestamp, :data, :svalue])
      |> validate_required([:id])
    end

    def create_table do
      statement = """
        CREATE TABLE IF NOT EXISTS
        #{TestKeyspace.__keyspace__()}.test_data(
        id text,
        timestamp text,
        data text,
        svalue set<text>,
        PRIMARY KEY (id, timestamp))
        WITH CLUSTERING ORDER BY (timestamp DESC)
      """

      {:ok, _result} = Cassandrax.cql(TestConn, statement)
    end

    def drop_table do
      statement = "DROP TABLE IF EXISTS #{TestKeyspace.__keyspace__()}.test_data"

      {:ok, _result} = Cassandrax.cql(TestConn, statement)
    end
  end

  defmodule TestDataWithClusteringKey do
    use Cassandrax.Schema

    @primary_key [:id, :first_cluster, :second_cluster]

    table "test_data_with_clustering_key" do
      field(:id, :integer)
      field(:first_cluster, :string)
      field(:second_cluster, :string)
      field(:field1, :boolean)
      field(:field2, :integer)
    end

    def create_table do
      statement = """
        CREATE TABLE IF NOT EXISTS
        #{TestKeyspace.__keyspace__()}.test_data_with_clustering_key(
        id int,
        first_cluster text,
        second_cluster text,
        field1 boolean,
        field2 int,
        PRIMARY KEY (id, first_cluster, second_cluster))
        WITH CLUSTERING ORDER BY (first_cluster DESC, second_cluster DESC)
      """

      {:ok, _result} = Cassandrax.cql(TestConn, statement)
    end

    def drop_table do
      statement =
        "DROP TABLE IF EXISTS #{TestKeyspace.__keyspace__()}.test_data_with_clustering_key"

      {:ok, _result} = Cassandrax.cql(TestConn, statement)
    end
  end

  setup do
    TestData.create_table()
    on_exit(fn -> TestData.drop_table() end)
    :ok
  end

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

  describe "default_options" do
    test "default option read: :one" do
      assert TestKeyspace.__default_options__(:read) == [consistency: :one]
    end

    test "default option write: :one" do
      assert TestKeyspace.__default_options__(:write) == [consistency: :one]
    end
  end

  describe "default_options with custom configs" do
    setup do
      Application.put_env(:cassandrax, Cassandrax.TestConn,
        write_options: [consistency: :quorum],
        read_options: [consistency: :local_quorum]
      )

      on_exit(fn -> Application.put_env(:cassandrax, Cassandrax.TestConn, []) end)

      :ok
    end

    test "default option read: :one" do
      assert TestKeyspace.__default_options__(:read) == [consistency: :local_quorum]
    end

    test "default option write: :one" do
      assert TestKeyspace.__default_options__(:write) == [consistency: :quorum]
    end
  end

  test "keyspace" do
    assert TestKeyspace.__keyspace__() == "test_keyspace"
  end

  test "connection" do
    assert TestKeyspace.__conn__() == Cassandrax.TestConn
  end

  defp fixture(schema, data), do: struct(schema, data) |> TestKeyspace.insert!()

  describe "changeset operations" do
    setup do
      [record: fixture(TestData, id: "0", timestamp: "00:00", data: "0")]
    end

    test "insert valid data" do
      attributes =
        struct(TestData,
          id: "1",
          timestamp: "01:00",
          data: "1",
          svalue: MapSet.new(["one", "another one"])
        )

      assert schema_equal(TestKeyspace.insert(attributes), {:ok, attributes})
    end

    test "insert! valid data" do
      attributes =
        struct(TestData,
          id: "1",
          timestamp: "01:00",
          data: "1",
          svalue: MapSet.new(["one", "another one"])
        )

      assert schema_equal(TestKeyspace.insert!(attributes), attributes)
    end

    test "insert invalid data" do
      invalid_one = struct(TestData, id: "1", timestamp: 100)
      assert {:error, %Ecto.ChangeError{}} = TestKeyspace.insert(invalid_one)
    end

    test "insert! invalid data" do
      invalid_one = struct(TestData, id: "1", timestamp: 100)
      assert_raise(Ecto.ChangeError, fn -> TestKeyspace.insert!(invalid_one) end)
    end

    test "update valid data", %{record: record} do
      changeset = Changeset.change(record, data: "zero")
      expectation = %{record | data: "zero"}

      assert schema_equal(TestKeyspace.update(changeset), {:ok, expectation})
      assert schema_equal(TestKeyspace.get(TestData, id: "0"), expectation)
    end

    test "update! valid data", %{record: record} do
      changeset = Changeset.change(record, data: "zero")
      expectation = %{record | data: "zero"}

      assert schema_equal(TestKeyspace.update!(changeset), expectation)
      assert schema_equal(TestKeyspace.get(TestData, id: "0"), expectation)
    end

    test "update invalid data", %{record: record} do
      changeset = Changeset.change(record, timestamp: 1)
      assert {:error, %Ecto.ChangeError{}} = TestKeyspace.update(changeset)
    end

    test "update! invalid data", %{record: record} do
      changeset = Changeset.change(record, timestamp: 1)
      assert_raise(Ecto.ChangeError, fn -> TestKeyspace.update!(changeset) end)
    end

    test "delete valid data", %{record: record} do
      assert schema_equal(TestKeyspace.delete(record), {:ok, record})
      assert TestKeyspace.get(TestData, id: "0") == nil
    end

    test "delete! valid data", %{record: record} do
      assert schema_equal(TestKeyspace.delete!(record), record)
      assert TestKeyspace.get(TestData, id: "0") == nil
    end

    test "delete invalid id" do
      attributes_with_invalid_id = struct(TestData, id: "1", timestamp: 100)

      assert {:error, %Ecto.ChangeError{}} = TestKeyspace.delete(attributes_with_invalid_id)
    end

    test "delete! invalid id" do
      attributes_with_invalid_id = struct(TestData, id: "1", timestamp: 100)

      assert_raise(Ecto.ChangeError, fn -> TestKeyspace.delete!(attributes_with_invalid_id) end)
    end
  end

  describe "get with simple primary key" do
    setup do
      [
        first: fixture(TestData, id: "0", timestamp: "00:00", data: "0"),
        second:
          fixture(TestData,
            id: "1",
            timestamp: "01:00",
            data: "1",
            svalue: MapSet.new(["one", "another one"])
          )
      ]
    end

    test "get", %{first: first, second: second} do
      assert TestKeyspace.get(TestData, id: "0") == first
      assert TestKeyspace.get(TestData, id: "1") == second
      assert TestKeyspace.get(TestData, id: "3") == nil
    end
  end

  describe "get with compound primary key" do
    setup do
      TestDataWithClusteringKey.create_table()
      on_exit(fn -> TestDataWithClusteringKey.drop_table() end)

      [
        first:
          fixture(TestDataWithClusteringKey, id: 0, first_cluster: "abc", second_cluster: "123"),
        second:
          fixture(
            TestDataWithClusteringKey,
            id: 1,
            first_cluster: "abc",
            second_cluster: "123",
            field1: true,
            field2: 2
          )
      ]
    end

    test "get", %{first: first, second: second} do
      assert TestKeyspace.get(TestDataWithClusteringKey,
               id: 0,
               first_cluster: "abc",
               second_cluster: "123"
             ) ==
               first

      assert TestKeyspace.get(TestDataWithClusteringKey,
               id: 1,
               first_cluster: "abc",
               second_cluster: "123"
             ) ==
               second

      assert TestKeyspace.get(TestDataWithClusteringKey,
               id: 0,
               first_cluster: "bcd",
               second_cluster: "123"
             ) ==
               nil

      assert TestKeyspace.get(TestDataWithClusteringKey,
               id: 0,
               first_cluster: "abc",
               second_cluster: "234"
             ) ==
               nil
    end
  end

  describe "all" do
    setup do
      [
        first: fixture(TestData, id: "0", timestamp: "00:00", data: "0"),
        second:
          fixture(TestData,
            id: "1",
            timestamp: "01:00",
            data: "1",
            svalue: MapSet.new(["one", "another one"])
          )
      ]
    end

    test "all", %{first: first, second: second} do
      assert list_sets_equal?(TestKeyspace.all(TestData), [first, second])
    end
  end

  describe "delete_all" do
    setup do
      fixture(TestData, id: "0", timestamp: "00:00", data: "1")
      fixture(TestData, id: "1", timestamp: "01:00", data: "2")
      :ok
    end

    test "raises on empty filter" do
      msg = "cannot perform Cassandrax.Keyspace.delete_all/2 with an empty primary key"
      assert_raise(ArgumentError, msg, fn -> TestKeyspace.delete_all(TestData) end)
    end

    test "raises on partial primary key" do
      msg =
        "Cannot perform Cassandrax.Keyspace.delete_all/2 with a partial partition key. " <>
          "If you need data filtering, use `allow_filtering/0` to enable slow queries."

      assert_raise(ArgumentError, msg, fn ->
        TestData |> where(timestamp: "00:00") |> TestKeyspace.delete_all()
      end)
    end

    test "raises on non-primary key" do
      msg =
        "Cannot perform Cassandrax.Keyspace.delete_all/2 with non-primary key filters. " <>
          "If you need data filtering, use `allow_filtering/0` to enable slow queries."

      assert_raise(ArgumentError, msg, fn ->
        TestData |> where(id: "0") |> where(data: "1") |> TestKeyspace.delete_all()
      end)
    end

    test "deletes all entries that meet the filter requirement" do
      func = fn ->
        TestData
        |> where(id: "0")
        |> TestKeyspace.all()
        |> length()
      end

      assert func.() == 1

      TestData |> where(id: "0") |> TestKeyspace.delete_all()
      assert func.() == 0
    end
  end

  describe "stream" do
    setup do
      [
        first: fixture(TestData, id: "0", timestamp: "00:00", data: "0"),
        second:
          fixture(TestData,
            id: "1",
            timestamp: "01:00",
            data: "1",
            svalue: MapSet.new(["one", "another one"])
          )
      ]
    end

    test "stream", %{first: first, second: second} do
      streamed_records =
        TestData
        |> TestKeyspace.stream(page_size: 1)
        |> Enum.to_list()

      assert list_sets_equal?(streamed_records, [first, second])
    end
  end

  describe "one" do
    test "fails on table with no entries" do
      assert TestKeyspace.one(TestData) == nil
    end

    test "from table with one entry" do
      record = fixture(TestData, id: "0", timestamp: "00:00", data: "0")

      assert TestKeyspace.one(TestData) == record
    end

    test "from table with multiple entries" do
      first_record = fixture(TestData, id: "0", timestamp: "00:00", data: "0")

      second_record =
        fixture(TestData,
          id: "1",
          timestamp: "01:00",
          data: "1",
          svalue: MapSet.new(["one", "another one"])
        )

      assert TestData |> where(id: "0") |> TestKeyspace.one() == first_record
      assert TestData |> where(id: "1") |> TestKeyspace.one() == second_record
      assert_raise(Cassandrax.MultipleResultsError, fn -> TestKeyspace.one(TestData) end)
    end
  end

  describe "batch operations" do
    setup do
      [
        first: fixture(TestData, id: "0", timestamp: "00:00", data: "0"),
        second:
          fixture(TestData,
            id: "1",
            timestamp: "01:00",
            data: "1",
            svalue: MapSet.new(["one", "another one"])
          )
      ]
    end

    test "empty batch" do
      TestKeyspace.batch(fn batch -> batch end)
    end

    test "single insert" do
      expectation = struct(TestData, id: "2", timestamp: "02:00", data: "2")

      TestKeyspace.batch(fn batch -> TestKeyspace.batch_insert(batch, expectation) end)

      two = TestKeyspace.get(TestData, id: "2")
      assert two.id == expectation.id
      assert two.timestamp == expectation.timestamp
      assert two.svalue == expectation.svalue
    end

    test "duplicate key insert" do
      expectation = %{
        struct(TestData, id: "2", timestamp: "02:00", data: "2")
        | timestamp: "02:05"
      }

      TestKeyspace.batch(fn batch -> TestKeyspace.batch_insert(batch, expectation) end)

      result = TestData |> where(id: "2") |> TestKeyspace.one()
      assert result.id == expectation.id
      assert result.timestamp == expectation.timestamp
      assert result.svalue == expectation.svalue
    end

    test "multiple inserts" do
      third = struct(TestData, id: "3", timestamp: "03:00", data: "3")
      fourth = struct(TestData, id: "4", timestamp: "04:00", data: "4")

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_insert(third)
        |> TestKeyspace.batch_insert(fourth)
      end)

      # Mimic structs being loaded from database
      third = %{third | __meta__: %{third.__meta__ | state: :loaded}}
      fourth = %{fourth | __meta__: %{fourth.__meta__ | state: :loaded}}

      assert list_set_includes?(TestKeyspace.all(TestData), [third, fourth])
    end

    test "single update", %{first: first} do
      changeset = TestKeyspace.get(TestData, id: "0") |> Changeset.change(data: "zero")
      expectation = %{first | data: "zero"}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset)
      end)

      result = TestData |> where(id: "0") |> TestKeyspace.one()
      assert result == expectation
    end

    test "multiple updates", %{first: first, second: second} do
      changeset1 = TestKeyspace.get(TestData, id: "0") |> Changeset.change(data: "zero")
      expectation1 = %TestData{first | data: "zero"}
      changeset2 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(data: "one")
      expectation2 = %TestData{second | data: "one"}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset1)
        |> TestKeyspace.batch_update(changeset2)
      end)

      assert list_set_includes?(TestKeyspace.all(TestData), [expectation1, expectation2])
    end

    test "update scalar data of the same records" do
      changeset1 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(data: "new one")
      changeset2 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(data: "last one")
      changeset3 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(data: "one last one")

      expectation = ["new one", "last one", "one last one"]

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset1)
        |> TestKeyspace.batch_update(changeset2)
        |> TestKeyspace.batch_update(changeset3)
      end)

      result = TestData |> where(id: "1") |> TestKeyspace.one()
      assert result.data in expectation
    end

    test "update set data of the same records", %{second: second} do
      changeset1 =
        TestKeyspace.get(TestData, id: "1")
        |> Changeset.change(svalue: MapSet.new(["hello world"]))

      changeset2 =
        TestKeyspace.get(TestData, id: "1")
        |> Changeset.change(svalue: MapSet.new(["pandemic world"]))

      expectation = %{second | svalue: MapSet.new(["hello world", "pandemic world"])}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset1)
        |> TestKeyspace.batch_update(changeset2)
      end)

      result = TestData |> where(id: "1") |> TestKeyspace.one()
      assert result == expectation
    end

    test "single delete" do
      TestKeyspace.batch(fn batch ->
        data = TestKeyspace.get(TestData, id: "0")

        batch
        |> TestKeyspace.batch_delete(data)
      end)

      result = TestData |> where(id: "0") |> TestKeyspace.one()
      assert result == nil
    end

    test "multiple deletes", %{first: first, second: second} do
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_delete(first)
        |> TestKeyspace.batch_delete(second)
      end)

      refute list_set_includes?(TestKeyspace.all(TestData), [first, second])
    end

    test "insert and update", %{second: second} do
      fourth = struct(TestData, id: "4", timestamp: "04:00", data: "4")

      changeset = TestKeyspace.get(TestData, id: "1") |> Changeset.change(data: "one")
      expectation = %{second | data: "one"}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_insert(fourth)
        |> TestKeyspace.batch_update(changeset)
      end)

      # Mimic structs being loaded from database
      fourth = %{fourth | __meta__: %{fourth.__meta__ | state: :loaded}}

      assert list_set_includes?(TestKeyspace.all(TestData), [fourth, expectation])
    end

    test "delete then update", %{second: second} do
      changeset = TestKeyspace.get(TestData, id: "1") |> Changeset.change(data: "one")
      expectation = [second, %{second | data: "one"}, nil]

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_delete(second)
        |> TestKeyspace.batch_update(changeset)
      end)

      result = TestData |> where(id: "1") |> TestKeyspace.one()
      assert result in expectation
    end
  end

  describe "cql" do
    setup do
      [
        first: fixture(TestData, id: "0", timestamp: "00:00", data: "0"),
        second:
          fixture(TestData,
            id: "1",
            timestamp: "01:00",
            data: "1",
            svalue: MapSet.new(["one", "another one"])
          ),
        third: fixture(TestData, id: "2", timestamp: "02:00", data: "2")
      ]
    end

    test "valid cql" do
      statement = """
      SELECT * FROM #{TestKeyspace.__keyspace__()}.test_data
      """

      assert {:ok, _} = Cassandrax.cql(TestConn, statement)

      statement = [
        "SELECT timestamp FROM ",
        "#{TestKeyspace.__keyspace__()}.test_data ",
        "WHERE id = '1'"
      ]

      {:ok, page} = Cassandrax.cql(TestConn, statement)
      assert [%{"timestamp" => "01:00"}] = Enum.to_list(page)
    end

    test "invalid cql" do
      statement = """
      SELECT * #{TestKeyspace.__keyspace__()}.test_data
      """

      assert {:error, %{reason: :invalid_syntax}} = Cassandrax.cql(TestConn, statement)
    end
  end

  describe "query expressions" do
    setup do
      [
        first: fixture(TestData, id: "0", timestamp: "00:00", data: "0"),
        second: fixture(TestData, id: "0", timestamp: "00:01", data: "0"),
        third: fixture(TestData, id: "1", timestamp: "01:00", data: "1")
      ]
    end

    test "order by", %{first: first, second: second} do
      query = TestData |> allow_filtering() |> where(id: "0") |> order_by([:timestamp])

      assert [^first, ^second] = TestKeyspace.all(query)
    end

    test "order by asc", %{first: first, second: second} do
      query = TestData |> allow_filtering() |> where(id: "0") |> order_by(asc: :timestamp)

      assert [^first, ^second] = TestKeyspace.all(query)
    end

    test "order by desc", %{first: first, second: second} do
      query = TestData |> allow_filtering() |> where(id: "0") |> order_by(desc: :timestamp)

      assert [^second, ^first] = TestKeyspace.all(query)
    end

    test "distinct" do
      query = TestData |> allow_filtering() |> distinct([:id])
      assert [%TestData{id: "0"}, %TestData{id: "1"}] = TestKeyspace.all(query)
    end

    test "group by" do
      query = TestData |> allow_filtering() |> group_by([:id])
      assert [%TestData{id: "0"}, %TestData{id: "1"}] = TestKeyspace.all(query)
    end
  end
end
