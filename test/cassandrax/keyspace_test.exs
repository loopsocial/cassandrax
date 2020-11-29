Code.require_file("../support/data_case.exs", __DIR__)

defmodule Cassandrax.KeyspaceTest do
  use Cassandrax.DataCase

  import Ecto.Changeset
  import Cassandrax.Query

  alias Cassandrax.TestConn
  # alias Cassandrax.Schema
  # alias Cassandrax.Keyspace.Batch
  alias Ecto.Changeset

  defmodule TestData do
    use Cassandrax.Schema

    import Ecto.Changeset
    import Cassandrax.Query

    alias Cassandrax.TestConn

    @type t :: %__MODULE__{}
    @primary_key [:id]

    table "test_data" do
      field(:id, :string)
      field(:value, :string)
      field(:svalue, MapSetType)
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

  @zero [id: "0", value: "zero"]
  @one [id: "1", value: "one", svalue: MapSet.new(["one", "another one"])]
  @two [id: "2", value: "two"]
  @three [id: "3", value: "three"]
  @four [id: "4", value: "four"]

  # invalid because 1 is an integer
  @invalid_one [id: "1", value: 1]
  @invalid_id [id: 1, value: 1]

  defp create_zero(_), do: [zero: fixture(@zero)]
  defp create_one(_), do: [one: fixture(@one)]
  defp create_two(_), do: [two: fixture(@two)]
  # defp create_three(_), do: [three: fixture(@three)]
  # defp create_four(_), do: [four: fixture(@four)]
  defp fixture(data), do: struct(TestData, data) |> TestKeyspace.insert!()

  describe "changeset operations" do
    setup [:create_zero]

    test "insert valid data" do
      one = struct(TestData, @one)
      assert schema_equal(TestKeyspace.insert(one), {:ok, one})
    end

    test "insert! valid data" do
      one = struct(TestData, @one)
      assert schema_equal(TestKeyspace.insert!(one), one)
    end

    test "insert invalid data" do
      invalid_one = struct(TestData, @invalid_one)
      assert {:error, %Ecto.ChangeError{}} = TestKeyspace.insert(invalid_one)
    end

    test "insert! invalid data" do
      invalid_one = struct(TestData, @invalid_one)
      assert_raise(Ecto.ChangeError, fn -> TestKeyspace.insert!(invalid_one) end)
    end

    test "update valid data", %{zero: zero} do
      changeset = Changeset.change(zero, value: "new zero")
      expectation = %{zero | value: "new zero"}
      assert schema_equal(TestKeyspace.update(changeset), {:ok, expectation})
      assert schema_equal(TestKeyspace.get(TestData, id: "0"), expectation)
    end

    test "update! valid data", %{zero: zero} do
      changeset = Changeset.change(zero, value: "new zero")
      expectation = %{zero | value: "new zero"}
      assert schema_equal(TestKeyspace.update!(changeset), expectation)
      assert schema_equal(TestKeyspace.get(TestData, id: "0"), expectation)
    end

    test "update invalid data", %{zero: zero} do
      changeset = Changeset.change(zero, value: 1)
      assert {:error, %Ecto.ChangeError{}} = TestKeyspace.update(changeset)
    end

    test "update! invalid data", %{zero: zero} do
      changeset = Changeset.change(zero, value: 1)
      assert_raise(Ecto.ChangeError, fn -> TestKeyspace.update!(changeset) end)
    end

    test "delete valid data", %{zero: zero} do
      assert schema_equal(TestKeyspace.delete(zero), {:ok, zero})
      assert TestKeyspace.get(TestData, id: "0") == nil
    end

    test "delete! valid data", %{zero: zero} do
      assert schema_equal(TestKeyspace.delete!(zero), zero)
      assert TestKeyspace.get(TestData, id: "0") == nil
    end

    test "delete invalid id" do
      invalid_id = struct(TestData, @invalid_id)
      assert {:error, %Ecto.ChangeError{}} = TestKeyspace.delete(invalid_id)
    end

    test "delete! invalid id" do
      invalid_id = struct(TestData, @invalid_id)
      assert_raise(Ecto.ChangeError, fn -> TestKeyspace.delete!(invalid_id) end)
    end
  end

  describe "queryables" do
    setup [:create_zero, :create_one]

    test "get", %{zero: zero} do
      assert TestKeyspace.get(TestData, id: "0") == zero
      assert TestKeyspace.get(TestData, id: "3") == nil
    end

    test "all", %{zero: zero, one: one} do
      assert list_sets_equal?(TestKeyspace.all(TestData), [zero, one])
    end
  end

  describe "one" do
    test "fails on table with no entries" do
      assert TestKeyspace.one(TestData) == nil
    end

    test "from table with one entry" do
      [zero: zero] = create_zero(nil)

      assert TestKeyspace.one(TestData) == zero
    end

    test "from table with multiple entries" do
      [zero: zero] = create_zero(nil)
      [one: one] = create_one(nil)

      assert TestData |> where(id: "0") |> TestKeyspace.one() == zero
      assert TestData |> where(id: "1") |> TestKeyspace.one() == one
      assert_raise(Cassandrax.MultipleResultsError, fn -> TestKeyspace.one(TestData) end)
    end
  end

  describe "batch operations" do
    setup [:create_zero, :create_one]

    test "empty batch" do
      TestKeyspace.batch(fn batch -> batch end)
    end

    test "single insert" do
      expectation = struct(TestData, @two)

      TestKeyspace.batch(fn batch -> TestKeyspace.batch_insert(batch, expectation) end)

      two = TestKeyspace.get(TestData, id: "2")
      assert two.id == expectation.id
      assert two.value == expectation.value
      assert two.svalue == expectation.svalue
    end

    test "duplicate key insert" do
      expectation = %{struct(TestData, @two) | value: "new two"}

      TestKeyspace.batch(fn batch -> TestKeyspace.batch_insert(batch, expectation) end)

      result = TestData |> where(id: "2") |> TestKeyspace.one()
      assert result.id == expectation.id
      assert result.value == expectation.value
      assert result.svalue == expectation.svalue
    end

    test "multiple inserts" do
      three = struct(TestData, @three)
      four = struct(TestData, @four)

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_insert(three)
        |> TestKeyspace.batch_insert(four)
      end)

      # Mimic structs being loaded from database
      three = %{three | __meta__: %{three.__meta__ | state: :loaded}}
      four = %{four | __meta__: %{four.__meta__ | state: :loaded}}

      assert list_set_includes?(TestKeyspace.all(TestData), [three, four])
    end

    test "single update", %{zero: zero} do
      changeset = TestKeyspace.get(TestData, id: "0") |> Changeset.change(value: "new zero")
      expectation = %{zero | value: "new zero"}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset)
      end)

      result = TestData |> where(id: "0") |> TestKeyspace.one()
      assert result == expectation
    end

    test "multiple updates", %{zero: zero, one: one} do
      changeset1 = TestKeyspace.get(TestData, id: "0") |> Changeset.change(value: "new zero")
      expectation1 = %TestData{zero | value: "new zero"}
      changeset2 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "new one")
      expectation2 = %TestData{one | value: "new one"}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset1)
        |> TestKeyspace.batch_update(changeset2)
      end)

      assert list_set_includes?(TestKeyspace.all(TestData), [expectation1, expectation2])
    end

    test "update scalar data of the same records" do
      changeset1 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "new one")
      changeset2 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "last one")
      changeset3 = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "one last one")
      expectation = ["new one", "last one", "one last one"]

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_update(changeset1)
        |> TestKeyspace.batch_update(changeset2)
        |> TestKeyspace.batch_update(changeset3)
      end)

      result = TestData |> where(id: "1") |> TestKeyspace.one()
      assert result.value in expectation
    end

    test "update set data of the same records", %{one: one} do
      changeset1 =
        TestKeyspace.get(TestData, id: "1")
        |> Changeset.change(svalue: MapSet.new(["hello world"]))

      changeset2 =
        TestKeyspace.get(TestData, id: "1")
        |> Changeset.change(svalue: MapSet.new(["pandemic world"]))

      expectation = %{one | svalue: MapSet.new(["hello world", "pandemic world"])}

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

    test "multiple deletes", %{zero: zero, one: one} do
      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_delete(zero)
        |> TestKeyspace.batch_delete(one)
      end)

      refute list_set_includes?(TestKeyspace.all(TestData), [zero, one])
    end

    test "insert and update", %{one: one} do
      four = struct(TestData, @four)

      changeset = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "new one")
      expectation = %{one | value: "new one"}

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_insert(four)
        |> TestKeyspace.batch_update(changeset)
      end)

      # Mimic structs being loaded from database
      four = %{four | __meta__: %{four.__meta__ | state: :loaded}}

      assert list_set_includes?(TestKeyspace.all(TestData), [four, expectation])
    end

    test "delete then update", %{one: one} do
      changeset = TestKeyspace.get(TestData, id: "1") |> Changeset.change(value: "new one")
      expectation = [one, %{one | value: "new one"}, nil]

      TestKeyspace.batch(fn batch ->
        batch
        |> TestKeyspace.batch_delete(one)
        |> TestKeyspace.batch_update(changeset)
      end)

      result = TestData |> where(id: "1") |> TestKeyspace.one()
      assert result in expectation
    end
  end

  describe "cql" do
    setup [:create_zero, :create_one, :create_two]

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

      {:ok, page} = Cassandrax.cql(TestConn, statement)
      assert [%{"value" => "one"}] = Enum.to_list(page)
    end

    test "invalid cql" do
      statement = """
      SELECT * #{TestKeyspace.__keyspace__()}.test_data
      """

      assert {:error, %{reason: :invalid_syntax}} = Cassandrax.cql(TestConn, statement)
    end
  end
end
