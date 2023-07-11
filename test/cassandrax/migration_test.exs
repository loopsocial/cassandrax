Code.require_file("../support/data_case.exs", __DIR__)

# defmodule TestRepo do
#   use Ecto.Repo,
#     otp_app: Cassandrax,
#     adapter: Cassandrax.Adapter
# end

defmodule TestData do
  use Cassandrax.Schema

  import Ecto.Changeset

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
end

defmodule TestMigrations.CreateTestData do
  use Ecto.Migration, migration_repo: TestKeyspace

  def change do
    create table("test_data") do
      add :id, :string, primary_key: true
      add :timestamp, :string
      add :data, :string
      add :svalue, MapSetType
    end
  end
end

defmodule Cassandrax.KeyspaceTest do
  use ExUnit.Case, async: false

  setup do
    if Process.whereis(TestKeyspace) == nil do
      {:ok, _} = TestKeyspace.start_link(nil)
    end
    :ok
  end

  # setup do
  #   case Cassandrax.Adapter.storage_up(TestKeyspace.config) do
  #     :ok -> nil
  #     {:error, :already_up} -> nil
  #     error -> raise "Error returned while creating keyspace: #{inspect error}"
  #   end
  # end

  test "can create test data table in both directions" do
    Ecto.Migrator.up(TestKeyspace, 20080906120000, TestMigrations.CreateTestData)
    # TODO: verify that table is there
    Ecto.Migrator.down(TestKeyspace, 20080906120000, TestMigrations.CreateTestData)
    # TODO: verify that table is gone
  end
end
