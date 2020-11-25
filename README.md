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

Application configuration.

```elixir
config :cassandrax, Cassandrax.MyConn,
nodes: ["127.0.0.1:9042"],
username: "cassandra",
password: "cassandra",
write_options: [consistency: :one],
read_options: [consistency: :one]
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


