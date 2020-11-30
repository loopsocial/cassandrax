# Cassandrax

Cassandrax is a Cassandra ORM built on top of [Xandra](https://github.com/lexhide/xandra) library and
[Ecto](https://github.com/elixir-ecto/ecto) data mapping.

Cassandrax is inspired by the [Triton](https://github.com/blitzstudios/triton) and
[Ecto](https://github.com/elixir-ecto/ecto) projects.

It allows you to build and run CQL statements as well as map results to Elixir structs.

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

child = Cassandrax.Supervisor.child_spec(Cassandrax.MyConn, test_conn_attrs)
Cassandrax.start_link([child])
```

Defining a new keyspace module.

```elixir
defmodule MyKeyspace do
  use Cassandrax.Keyspace, cluster: Cassandrax.MyConn, name: "my_keyspace"
end
```

Creating a keyspace.

```elixir
statement = \"""
CREATE KEYSPACE IF NOT EXISTS my_keyspace
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
\"""

Cassandrax.cql(Cassandrax.MyConn, statement)
```

Creating a table in the Keyspace.

```elixir
statement = [
  "CREATE TABLE IF NOT EXISTS ",
  "my_keyspace.user(",
  "id integer, ",
  "user_name text, ",
  "svalue set<text>, ",
  "PRIMARY KEY (id))"
]

{:ok, _result} = Cassandrax.cql(Cassandrax.MyConn, statement)
```
## Usage

Inserting data. 

```elixir
user =  %User{id: 1, user_name: "alice"}

{:ok, user} = MyKeyspace.insert(user) 
user = MyKeyspace.insert!(user)
```

Updating data.

```elixir
changeset = Changeset.change(user, user_name: "bob")

{:ok, updated_user} = MyKeyspace.update(changeset)
updated_user = MyKeyspace.update!(changeset)
```

Deleting data. 

```elixir
{:ok, user} = MyKeyspace.delete(user)
user = MyKeyspace.delete!(user)
```

Batch uperations.

```elixir
user = %User{id: 1, user_name: "alice"}
changeset = MyKeyspace.get(TestData, id: 2) |> Changeset.change(user_name: "eve")

MyKeyspace.batch(fn batch ->
  batch
  |> MyKeyspace.batch_insert(user)
  |> MyKeyspace.batch_update(changeset)
end)
```

Querying data.

Get records.
```elixir
MyKeyspace.get(User, id: 0)
```

Get all records.
```elixir
MyKeyspace.all(User)
```

Get one record.

```elixir
User |> where(id: 0) |> MyKeyspace.one()
```
      