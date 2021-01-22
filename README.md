# Cassandrax

Cassandrax is a Cassandra ORM built on top of [Xandra](https://github.com/lexhide/xandra) library
and [Ecto](https://github.com/elixir-ecto/ecto) data mapping.

Cassandrax is inspired by the [Triton](https://github.com/blitzstudios/triton) and
[Ecto](https://github.com/elixir-ecto/ecto) projects. It allows you to build and run CQL statements
as well as map results to Elixir structs.

The docs can be found at [https://hexdocs.pm/cassandrax](https://hexdocs.pm/cassandrax).

## Installation

```elixir
def deps do
  [
    {:cassandrax, "~> 0.0.3"}
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

Keep in mind that in order to use your current Ecto Repo migrations to run the above
commands (always defining `up` and `down` functions separately), first you need to make
sure `:cassandrax` is started before migrations are ran. To do that, edit your
`config/config.exs` like so:

```elixir
config :my_app, MyApp.Repo, start_apps_before_migration: [:cassandrax]
```

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

`Cassandrax` queries are very similar to `Ecto`'s, you can use the `all`, `get`
and `one` functions directly from your Keyspace module.

```elixir
iex(16)> MyKeyspace.get(UserById, id: 1)
%UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "alice"}

iex(17)> MyKeyspace.all(UserById)
[
  %UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "alice"},
  %UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 2, user_name: "eve"},
  ...
]

iex(18)> import Cassandrax.Query
true

iex(19)> UserById |> where(id: 1) |> MyKeyspace.one()
%UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 1, user_name: "alice"}
```

Also, as described in the last query above, you can use `Cassandrax.Query`
macros to build your own queries. Just keep in mind we're still working on the
API, so it is still unstable and can change in any version prior to `0.1.0`

```elixir
# Remember when filtering data by non-primary key fields, you should use ALLOW FILTERING:
iex(20)> UserById
  |> where(id: 3)
  |> where(:user_name == "adam")
  |> where(:age >= 30)
  |> allow_filtering()
  |> MyKeyspace.all()
[%UserById{__meta__: %Ecto.Schema.Metadata{:loaded, "user_by_id"}, id: 3, user_name: "adam", age: 31}}]
```
