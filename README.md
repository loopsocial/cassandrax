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
    {:cassandrax, "~> 0.0.2"}
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
> statement = """
>   CREATE KEYSPACE IF NOT EXISTS my_keyspace
>   WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
> """

> Cassandrax.cql(Cassandrax.MyConn, statement)
{:ok,
 %Xandra.SchemaChange{
   effect: "CREATED",
   options: %{keyspace: "my_keyspace"},
   target: "KEYSPACE",
   tracing_id: nil
 }}
```

Creating a table in the Keyspace.

```elixir
> statement = [
>   "CREATE TABLE IF NOT EXISTS ",
>   "my_keyspace.user(",
>   "id integer, ",
>   "user_name text, ",
>   "svalue set<text>, ",
>   "PRIMARY KEY (id))"
> ]

> Cassandrax.cql(Cassandrax.MyConn, statement)
{:ok,
 %Xandra.SchemaChange{
   effect: "CREATED",
   options: %{keyspace: "my_keyspace", subject: "user"},
   target: "TABLE",
   tracing_id: nil
 }}
```
## Usage

Inserting data. 

```elixir
> user =  %User{id: 1, user_name: "alice"}
%User{id: 1, user_name: "alice"}

> MyKeyspace.insert(user) 
{:ok, %User{__meta__: #Ecto.Schema.Metadata<:loaded, "user">, id: 1, user_name: "alice"}}

> MyKeyspace.insert!(user)
%User{__meta__: #Ecto.Schema.Metadata<:loaded, "user">, id: 1, user_name: "alice"}
```

Updating data.

```elixir
> changeset = Changeset.change(user, user_name: "bob")
#Ecto.Changeset<changes: %{user_name: "bob"}, ...>

> MyKeyspace.update(changeset)
%User{__meta__: #Ecto.Schema.Metadata<:loaded, "user">, id: 1, user_name: "bob"}

> MyKeyspace.update!(changeset)
%User{__meta__: #Ecto.Schema.Metadata<:loaded, "user">, id: 1, user_name: "bob"}
```

Deleting data. 

```elixir
> MyKeyspace.delete(user)
{:ok, %User{__meta__: #Ecto.Schema.Metadata<:deleted, "user">, id: 1, user_name: "bob"}}

> MyKeyspace.delete!(user)
%User{__meta__: #Ecto.Schema.Metadata<:deleted, "user">, id: 1, user_name: "bob"}
```

Batch operations.

```elixir
> user = %User{id: 1, user_name: "alice"}
%User{id: 1, user_name: "alice"}

> changeset = MyKeyspace.get(User, id: 2) |> Changeset.change(user_name: "eve")
#Ecto.Changeset<changes: %{user_name: "eve", ...}>

> MyKeyspace.batch(fn batch ->
>   batch
>   |> MyKeyspace.batch_insert(user)
>   |> MyKeyspace.batch_update(changeset)
> end)
:ok
```

Querying data.

Get records.
```elixir
> MyKeyspace.get(User, id: 1)
%User{__meta__: #Ecto.Schema.Metadata<:loaded, "user">, id: 1, user_name: "alice"}
```

Get all records.
```elixir
> MyKeyspace.all(User)
[
  %User{__meta__: #Ecto.Schema.Metadata<:loaded, "user">, id: 1, user_name: "alice"},
  %User{__meta__: #Ecto.Schema.Metadata<:loaded, "user">, id: 2, user_name: "eve"},
  ...
]
```

Get one record.

```elixir
> User |> where(id: 1) |> MyKeyspace.one()
%User{__meta__: #Ecto.Schema.Metadata<:loaded, "user">, id: 1, user_name: "alice"}
```
