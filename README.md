# Cassandrax

[![CI](https://github.com/loopsocial/cassandrax/actions/workflows/ci.yml/badge.svg)](https://github.com/loopsocial/cassandrax/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/cassandrax.svg)](https://hex.pm/packages/cassandrax)

Cassandrax is a Cassandra data mapping toolkit built on top of
[Ecto](https://github.com/elixir-ecto/ecto)
and query builder and runner on top of [Xandra](https://github.com/lexhide/xandra).

Cassandrax is heavily inspired by the [Triton](https://github.com/blitzstudios/triton) and
[Ecto](https://github.com/elixir-ecto/ecto) projects. It allows you to build
and run CQL statements as well as map results to Elixir structs.

The docs can be found at [https://hexdocs.pm/cassandrax](https://hexdocs.pm/cassandrax).

## Installation

```elixir
def deps do
  [
    {:cassandrax, "~> 0.1.0"}
  ]
end
```

## Setup

```elixir
test_conn_attrs = [
  nodes: ["127.0.0.1:9043"],
  username: "cassandra",
  password: "cassandra"
]

# MyApp.MyCluster is just an atom
child = Cassandrax.Supervisor.child_spec(MyApp.MyCluster, test_conn_attrs)
Cassandrax.start_link([child])
```

Alternatively, if you're using CassandraDB on a Phoenix app, you can edit your
`config/config.exs` file to add Cassandrax to your supervision tree:

```elixir
# In your config/config.exs, you can add as many clusters as you like

config :cassandrax, clusters: [MyApp.MyCluster]

config :cassandrax, MyApp.MyCluster,
  protocol_version: :v4,
  nodes: ["127.0.0.1:9042"],
  pool_size: System.get_env("CASSANDRADB_POOL_SIZE") || 10,
  username: System.get_env("CASSANDRADB_USER") || "cassandra",
  password: System.get_env("CASSANDRADB_PASSWORD") || "cassandra",
  # Default write/read options
  write_options: [consistency: :local_quorum],
  read_options: [consistency: :one]
```

## Usage

You can easily define a Keyspace module that will act as a wrapper for
read/write operations:

```elixir
defmodule MyKeyspace do
  use Cassandrax.Keyspace, cluster: MyApp.MyCluster, name: "my_keyspace"
end
```

To define your schema, use the `Cassandrax.Schema` module, which provides the
`table` macro:

```elixir
defmodule UserById do
  use Cassandrax.Schema

  # Defines :id as partition key and :age as clustering key
  @primary_key [:id, :age]

  table "user_by_id" do
    field :id, :integer
    field :age, :integer
    field :user_name, :string
    field :nicknames, MapSetType
  end
end
```

While we work to support an actual migration DSL, you can run plain CQL statements to
migrate the database schema, like so:

```elixir
iex(1)> statement = """
   CREATE KEYSPACE IF NOT EXISTS my_keyspace
   WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
 """

# Creating the Keyspace
iex(2)> Cassandrax.cql(MyApp.MyCluster, statement)
{:ok,
 %Xandra.SchemaChange{
   effect: "CREATED",
   options: %{keyspace: "my_keyspace"},
   target: "KEYSPACE",
   tracing_id: nil
 }}

iex(3)> statement = """
   CREATE TABLE IF NOT EXISTS my_keyspace.user_by_id(
   id int,
   age int,
   user_name varchar,
   nicknames set<varchar>,
   PRIMARY KEY (id, age))
"""

# Creating the Table
iex(4)> Cassandrax.cql(MyApp.MyCluster, statement)
{:ok,
 %Xandra.SchemaChange{
   effect: "CREATED",
   options: %{keyspace: "my_keyspace", subject: "user_by_id"},
   target: "TABLE",
   tracing_id: nil
 }}
```

### Migrations
In future, we plan to support pure cassandrax migrations, but so far we still depend on Ecto to
keep track of migrations. Below we present a strategy to keep cassandrax migrations separated
from your main database migrations.

Let's configure a new `Ecto.Repo` to put migrations on `priv/cassandrax_repo/migrations`:

```elixir
# Configure an additional Ecto.Repo
config :my_app, MyApp.CassandraxRepo,
  database: "same as your main database",
  hostname: "localhost",
  username: "username",
  password: "password"

config :my_app, MyApp.CassandraxRepo,
  # ensure cassandrax connection is ready before the migration runs
  start_apps_before_migration: [:cassandrax],
```

Then create the additional `Ecto.Repo` pointing to a different table than `schema_migrations`,
to not conflict with your main database migrations.

```elixir
defmodule MyApp.CassandraxRepo do
  @moduledoc """
  Keep track of versions for Cassandra migrations.
  """
  use Ecto.Repo,
    otp_app: :repo,
    adapter: Ecto.Adapters.Postgres,
    migration_source: "cassandra_migrations"
end
```

Now you can simply create a new migration with

```
mix ecto.gen.migration -r MyApp.CassandraxRepo create_first_table`
```

And edit the file

```elixir
defmodule Repo.Migrations.CreateFirstTable do
  use Ecto.Migration
  alias MyApp.MyCluster

  def up do
    statement = """
      CREATE TABLE IF NOT EXISTS my_keyspace.user_by_id(
      id int,
      age int,
      user_name varchar,
      nicknames set<varchar>,
      PRIMARY KEY (id, age))
      """

    {:ok, _result} = Cassandrax.cql(Cluster, statement)
  end

  def down do
    statement = "DROP TABLE IF EXISTS my_keyspace.user_by_id"
    {:ok, _result} = Cassandrax.cql(Cluster, statement)
  end
end
```

Also, remember to include `MyApp.CassandraxRepo` migrations on your deploy scripts!

### CRUD
Mutating data is as easy as it is with a regular Ecto schema. You can work
straight with structs, or with changesets:

#### Insert
```elixir
iex(5)> user =  %UserById{id: 1, user_name: "alice"}
%UserById{id: 1, user_name: "alice"}

iex(6)> MyKeyspace.insert(user) 
{:ok, %UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "alice"}}

iex(7)> MyKeyspace.insert!(user)
%UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "alice"}
```

#### Update
```elixir
iex(8)> changeset = Changeset.change(user, user_name: "bob")
#Ecto.Changeset<changes: %{user_name: "bob"}, ...>

iex(9)> MyKeyspace.update(changeset)
{:ok, %UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "bob"}}

iex(10)> MyKeyspace.update!(changeset)
%UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "bob"}
```

#### Delete
```elixir
iex(11)> MyKeyspace.delete(user)
{:ok, %UserById{__meta__: %Ecto.Schema.Metadata{:deleted, "user_by_id"}, id: 1, user_name: "bob"}}

iex(12)> MyKeyspace.delete!(user)
%UserById{__meta__: %Ecto.Schema.Metadata{:deleted, "user_by_id"}, id: 1, user_name: "bob"}
```

#### Batch operations

You can issue many operation at once with a `BATCH` operation. For more
information on how Batches work in Cassandra DB, please refer to [CassandraDB
Batches](https://cassandra.apache.org/doc/latest/cql/dml.html#batch).

```elixir
iex(13)> user = %UserById{id: 1, user_name: "alice"}
%UserById{id: 1, user_name: "alice"}

iex(14)> changeset = MyKeyspace.get(UserById, id: 2) |> Changeset.change(user_name: "eve")
#Ecto.Changeset<changes: %{user_name: "eve", ...}>

iex(15)> MyKeyspace.batch(fn batch ->
  batch
  |> MyKeyspace.batch_insert(user)
  |> MyKeyspace.batch_update(changeset)
 end)
:ok
```

#### Querying

##### Disclaimer
> One thing to keep in mind when it comes to querying is the API is still under
development and, therefore, can still change in version prior to `0.1.0`.
>
> If you use it in production, be cautious when updating `cassandrax`, and make
sure all your queries work correctly after installing the new version.

`Cassandrax` queries are very similar to `Ecto`'s, you can use the `all/2`, `get/2`
and `one/2` functions directly from your Keyspace module.

```elixir
iex(16)> MyKeyspace.get(UserById, [id: 1, age: 20])
%UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, age: 20, user_name: "alice"}

iex(17)> MyKeyspace.all(UserById)
[
  %UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "alice"},
  %UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 2, user_name: "eve"},
  ...
]
```

Also, you can use `Cassandrax.Query` macros to build your own queries.

```elixir
iex(18)> import Cassandrax.Query
true

iex(19)> UserById |> where(id: 1, age: 20) |> MyKeyspace.one()
%UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, age: 20, user_name: "alice"}

# Remember when filtering data by non-primary key fields, you should use ALLOW FILTERING:
iex(20)> UserById
  |> where(id: 3)
  |> where(:user_name == "adam")
  |> where(:age >= 30)
  |> allow_filtering()
  |> MyKeyspace.all()
[%UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 3, user_name: "adam", age: 31}}]
```

Streaming data is supported.

```elixir
iex(21)> users = MyKeyspace.stream(UserById, page_size: 20)
#Function<59.58486609/2 in Stream.transform/3>

iex(22) Emum.to_list(users)
[
  %UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "alice"},
  %UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 2, user_name: "eve"},
  ...
]
```
