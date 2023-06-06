defmodule Cassandrax do
  @moduledoc ~S"""
  Cassandrax is a Cassandra ORM built on top of [Xandra](https://github.com/lexhide/xandra) and
  [Ecto](https://github.com/elixir-ecto/ecto). `Xandra` provides the driver for communication
  with the Database, and `Ecto` provides the data mapping functionality as well as changesets
  to control data mutations.

  Cassandrax is heavily inspired by [Triton](https://github.com/blitzstudios/triton) and
  [Ecto](https://github.com/elixir-ecto/ecto) projects, if you have used any of those projects,
  you'll get up to speed in no time to use Cassandrax.

  Cassandrax is split into 3 components following the design choices made on Ecto:

    * `Cassandrax.Keyspace` - keyspaces are the touchpoints with your Cassandra.
      Keyspaces provide the API for inserting, updating and deleting data from your tables,
      as well as querying the data already stored.
      Keyspaces need a `cluster` or a connection and a `name`.

    * `Cassandrax.Schema` - schemas are used to map data fetched from Cassandra into an Elixir
      struct. They use `Ecto.Schema` under the hood and should require a `@primary_key` to be
      defined before the table definition.

    * `Cassandrax.Query` - following `Ecto` design, they're written in Elixir syntax
      and are used to retrieve data from a Cassandra table. Queries are composable,
      even though they should remain as straight forward as possible. Unlike with
      relational databases, where you first model the data and their relationships and later
      you define how you should query that data, on Cassandra query design decisions are made
      when modeling the tables. For more information, please refer to
      [Cassandra Docs](https://cassandra.apache.org/doc/latest/data_modeling/index.html)

  Next we'll provide an overview of these components and how you'll use them to
  insert/change/fetch from/to Cassandra. Please check their corresponding module documentation
  for more in-depth description of the features available and options.

  ## Keyspaces

  `Cassandrax.Keyspace` is a wrapper around the keyspace. Each keyspace contains one or multiple
  tables and belongs to a cluster. A keyspace can be defined like so:

      defmodule SomeKeyspace do
        use Cassandrax.Keyspace, cluster: SomeCluster, name: "some_keyspace"
      end

  And the configuration for `SomeCluster` must be in your application environment,
  usually defined in your `config/config.exs`:

      config :cassandrax, clusters: [SomeCluster]
      
      config :cassandrax, SomeCluster,
          protocol_version: :v4,
          nodes: ["127.0.0.1:9042"]
          username: "cassandra",
          password: "cassandra",
          # cassandrax accepts all options you'd use in xandra

  Cassandrax automatically picks up these configs to start the pool of connections for each
  cluster. Keep in mind that all keyspaces that belong to the same cluster
  will share the same pool of connections. If you need your keyspace to have its own
  connection pool, please refer to the `Cassandrax.Keyspace` specific documentation.

  ## Schemas

  Schemas are used for table definition. Here's an example:

      defmodule UserByEmail do
        use Cassandrax.Schema

        # needs to be defined *before* defining the schema
        @primary_key [:email]

        # table name is users_by_email. Notice we don't need to set the keyspace here
        table "users_by_email" do
          field :email, :string
          field :id, :integer
          field :username, :string
        end
      end

  Cassandrax uses `Ecto.Schema` to define a struct with the schema fields:

      iex> user_by_email = %UserByEmail{email: "user@example.com"}
      iex> user_by_email.email
      "user@example.com"

  Just like with `Ecto`, this schema allows us to interact with keyspaces, like so:

      iex> user_by_email = %UserByEmail{email: "user@example.com", id: 123_456, username: "user"}
      iex> SomeKeyspace.insert!(user_by_email)
      %UserByEmail{...}

  Unlike relational databases which come with the autoincrement ID as default primary key,
  Cassandra requires you to define your own primary key. Therefore calling `Keyspace.insert/2`
  always returns the struct itself with updated metadata, but no changed fields.

  Also, bear in mind that Cassandra doesn't provice consistency guarantees the same way relational
  databases do, so the returned values of, for instance, deleting a record that doesn't exist
  anymore is the same as deleting an existing one:

      iex> user_by_email = %UserByEmail{email: "user@example.com", id: 123_456, username: "user"}

      # Store the result in a variable
      iex> result = SomeKeyspace.insert!(user_by_email)
      %UserByEmail{...}

      # Now delete the recently inserted record...
      iex> SomeKeyspace.delete!(result)
      %UserByEmail{...}

      # And if you try to delete a record that doesn't exist, no error is returned
      iex> SomeKeyspace.delete!(result)
      %UserByEmail{...}

  ## Queries

  Cassandrax provides you with a DSL so you can write queries in Elixir, lowering the chances
  of writing invalid CQL statements. In some occasions, `Cassandrax.Query` will validate your
  query at compile time and fail as soon as possible if your query is invalid:

      import Ecto.Query

      query = UserByEmail |> where(:email == "user@example.com")

      # Returns a List of %UserByEmail{} structs matching the query
      result = SomeKeyspace.all(query)

  Specific examples and detailed documentation for all available keywords are available in
  `Cassandrax.Query` module docs, but the supported keywords are:

    * `:allow_filtering`
    * `:distinct`
    * `:group_by`
    * `:limit`
    * `:order_by`
    * `:per_partition_limit`
    * `:select`
    * `:where`

  `Cassandrax.Keyspace` provides the same API as Ecto: You have `Keyspace.all/1` which returns
  all records matching a query, `Keyspace.one/1` which returns a single entry or raises and
  `Keyspace.get/2` which fetches an entry by its primary key.
  """
  use Application

  def start(_type, _args) do
    clusters()
    |> Enum.map(fn cluster ->
      config = Application.get_env(:cassandrax, cluster)
      ensure_cluster_config!(cluster)
      Cassandrax.Supervisor.child_spec(cluster, config)
    end)
    |> start_link()
    |> wait_connection()
  end

  defp clusters() do
    Application.get_env(:cassandrax, :clusters, [])
  end

  def ensure_cluster_config!(empty, cluster) do
    if is_nil(empty) or empty == [] do
      msg = "Expected to find keyword configs for #{inspect(cluster)}, found #{inspect(empty)}"
      raise(Cassandrax.ClusterConfigError, msg)
    end
  end

  def start_link(children) do
    Supervisor.start_link(children, strategy: :one_for_one, name: Cassandrax.Supervisor)
  end

  defp wait_connection(startup) do
    case startup do
      {:ok, _supervisor} ->
        retries = Application.get_env(:cassandrax, :retries, 10)
        interval = Application.get_env(:cassandrax, :interval, 100)
        wait_connection(clusters(), retries, interval)
        startup

      error ->
        error
    end
  end

  defp wait_connection([], _retries, _interval), do: :ok

  defp wait_connection([cluster | clusters], retries, interval) do
    case GenServer.call(cluster, :checkout) do
      {:ok, _pool} ->
        wait_connection(clusters, retries, interval)

      {:error, _reason} ->
        if retries == 0 do
          raise "Cannot connect to #{inspect(cluster)}"
        else
          :timer.sleep(interval)
          wait_connection([cluster | clusters], retries - 1, interval)
        end
    end
  end

  def cql(conn, statement, values \\ [], opts \\ []) do
    case Cassandrax.Connection.prepare(conn, statement) do
      {:ok, prepared} -> Cassandrax.Connection.execute(conn, prepared, values, opts)
      {:error, error} -> {:error, error}
    end
  end
end
