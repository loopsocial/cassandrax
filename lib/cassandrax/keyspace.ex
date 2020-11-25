defmodule Cassandrax.Keyspace do
  @moduledoc """
  Defines a Keyspace.

  A keyspace acts as a repository, wrapping an underlying keyspace in CassandraDB.

  ## Setup

  Application configuration.

  ```
  config :cassandrax, Cassandrax.MyConn,
  nodes: ["127.0.0.1:9042"],
  username: "cassandra",
  password: "cassandra",
  write_options: [consistency: :one],
  read_options: [consistency: :one]
  ```

  Defining a new keyspace module.

  ```
  defmodule MyKeyspace do
    use Cassandrax.Keyspace, cluster: Cassandrax.MyConn, name: "my_keyspace"
  end
  ```

  Creating a keyspace.

  ```
  statement = \"""
  CREATE KEYSPACE IF NOT EXISTS my_keyspace
  WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
  \"""

  Cassandrax.cql(Cassandrax.MyConn, statement)
  ```

  Creating a table in the Keyspace.

  ```
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
  """

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Cassandrax.Keyspace

      @keyspace_name Keyword.fetch!(opts, :name)
      @cluster Keyword.fetch!(opts, :cluster)
      @conn_pool Keyword.get(opts, :pool, @cluster)

      def config do
        {:ok, config} = Cassandrax.Supervisor.runtime_config(@cluster, [])

        config
      end

      def __default_options__(:write), do: __MODULE__.config() |> Keyword.get(:write_options)
      def __default_options__(:read), do: __MODULE__.config() |> Keyword.get(:read_options)
      def __keyspace__, do: @keyspace_name
      def __conn__, do: @conn_pool

      ## Keyspace gains its own pool if option `conn_pool` was given
      if @conn_pool == __MODULE__ do
        def child_spec(_), do: Cassandrax.Supervisor.child_spec(__MODULE__, config())
      end

      ## Schemas

      def insert(struct, opts \\ []),
        do: Cassandrax.Keyspace.Schema.insert(__MODULE__, struct, opts)

      def update(struct, opts \\ []),
        do: Cassandrax.Keyspace.Schema.update(__MODULE__, struct, opts)

      def delete(struct, opts \\ []),
        do: Cassandrax.Keyspace.Schema.delete(__MODULE__, struct, opts)

      def insert!(struct, opts \\ []),
        do: Cassandrax.Keyspace.Schema.insert!(__MODULE__, struct, opts)

      def update!(struct, opts \\ []),
        do: Cassandrax.Keyspace.Schema.update!(__MODULE__, struct, opts)

      def delete!(struct, opts \\ []),
        do: Cassandrax.Keyspace.Schema.delete!(__MODULE__, struct, opts)

      ## Queryable

      def all(queryable, opts \\ []),
        do: Cassandrax.Keyspace.Queryable.all(__MODULE__, queryable, opts)

      def get(queryable, primary_key, opts \\ []) do
        Cassandrax.Keyspace.Queryable.get(__MODULE__, queryable, primary_key, opts)
      end

      def one(queryable, opts \\ []),
        do: Cassandrax.Keyspace.Queryable.one(__MODULE__, queryable, opts)

      ## Run Plain CQL Statements

      def cql(statement, values \\ [], opts \\ []),
        do: Cassandrax.cql(@conn_pool, statement, values, opts)

      ## Batch

      def batch(opts \\ [], fun) do
        Cassandrax.Keyspace.Batch.run(__MODULE__, fun, opts)
      end

      def batch_insert(%Cassandrax.Keyspace.Batch{} = batch, struct),
        do: Cassandrax.Keyspace.Schema.batch_insert(__MODULE__, batch, struct)

      def batch_update(%Cassandrax.Keyspace.Batch{} = batch, struct),
        do: Cassandrax.Keyspace.Schema.batch_update(__MODULE__, batch, struct)

      def batch_delete(%Cassandrax.Keyspace.Batch{} = batch, struct),
        do: Cassandrax.Keyspace.Schema.batch_delete(__MODULE__, batch, struct)
    end
  end

  ## User callbacks
  @optional_callbacks init: 2

  @doc """
  A callback executed when the keyspace starts or when configuration is read.

  The first argument is the context the callback is being invoked. If it
  is called because the Keyspace supervisor is starting, it will be `:supervisor`.
  It will be `:runtime` if it is called for reading configuration without
  actually starting a process.

  The second argument is the keyspace configuration as stored in the
  application environment. It must return `{:ok, keyword}` with the updated
  list of configuration or `:ignore` (only in the `:supervisor` case).
  """
  @callback init(context :: :supervisor | :runtime, config :: Keyword.t()) ::
              {:ok, Keyword.t()} | :ignore

  @doc """
  Accesses the consistency level that manages availability versus data accuracy.
  Consistency level is configured for per individual read or write operation.
  Pass `:read` or `:write` to access the consistency level (eg. `[consistency: :one]`).

  ## Example
  ```
  [consistency: :one] = MyKeyspace.__default_options__(:read)
  ```
  """
  @callback __default_options__(atom :: :read | :write) :: list | nil

  @doc """
  Accesses the name of the Keyspace.

  ## Example
  ```
  "my_keyspace" = MyKeyspace.__keyspace__()
  ```
  """
  @callback __keyspace__ :: String.t()

  @doc """
  Accesses the cluster that was setup in the runtime configuration.

  ## Example
  ```
  Cassandrax.MyConn = MyKeyspace.__conn__()
  ```
  """
  @callback __connection__ :: Cassandrax.Connection

  @doc """
  Inserts a struct defined in `Cassandrax.Schema` or a changeset.

  If a struct is given, the struct is converted into a changeset with all non-nil fields.

  ## Example
  ```
  {:ok, user} = MyKeyspace.insert(%User{id: 1, user_name: "bob"})
  ```
  """
  @callback insert(
              struct_or_changeset :: Ecto.Changeset.t() | Cassandrax.Schema,
              opts :: Keyword.t()
            ) :: {:ok, Cassandrax.Schema.t()} | {:error, any()}

  @doc """
  Same as `insert/2` but returns the struct or raises if the changeset is invalid.
  """
  @callback insert!(
              struct_or_changeset :: Ecto.Changeset.t() | Cassandrax.Schema,
              opts :: Keyword.t()
            ) :: Cassandrax.Schema.t()

  @doc """
  Updates a changeset using its primary key.

  Requires a changeset as it is the only way to track changes.

  If the struct has no primary key, Xandra.Error will be raised.
  In CassandraDB, UPDATE is also an upsert. If the struct cannot be found, a new entry will be created.

  It returns `{:ok, struct}` if the struct has been successfully updated or `{:error, message}`
  if there was a validation or a known constraint error.

  ## Example
  ```
  user = MyKeyspace.get(User, 1)
  changeset = Ecto.Changeset.change(user, user_name: "tom")
  MyKeyspace.update(changeset)
  ```
  """
  @callback update(changeset :: Ecto.Changeset.t(), opts :: Keyword.t()) ::
              {:ok, Cassandrax.Schema.t()} | {:error, any()}

  @doc """
  Same as `update/2` but returns the struct or raises if the changeset is invalid.
  """
  @callback update!(changeset :: Ecto.Changeset.t(), opts :: Keyword.t()) :: Cassandrax.Schema.t()

  @doc """
  Deletes a struct using its primary key.

  If the struct has no primary key, Xandra.Error will be raised.

  If the struct has been removed from db prior to call, it will still return `{:ok, Cassandrax.Schema.t()}`

  It returns `{:ok, struct}` if the struct has been successfully deleted or `{:error, message}`
  if there was a validation or a known constraint error.

  ## Example
  ```
  MyKeyspace.delete(%User(id: 1, user_name: "bob"))
  ```
  """
  @callback delete(
              struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
              opts :: Keyword.t()
            ) :: {:ok, Cassandrax.Schema.t()} | {:error, any()}

  @doc """
  Same as `delete/2` but returns the struct or raises if the changeset is invalid.
  """
  @callback delete!(
              struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
              opts :: Keyword.t()
            ) :: Cassandrax.Schema.t()

  @doc """
  Fetches all entries from the data store that matches the given query.

  May raise Xandra.Error if query validation fails.

  ## Example
  ```
  query = where(User, id: 1)
  MyKeyspace.all(query)
  ```
  """
  @callback all(queryable :: Cassandrax.Queryable.t(), opts :: Keyword.t()) :: [
              Cassandrax.Schema.t()
            ]

  @doc """

  ## Example
  ```
  MyKeyspace.get(User, 2)
  ```
  """
  @callback get(queryable :: Cassandrax.Queryable.t(), id :: term(), opts :: Keyword.t()) ::
              Cassandrax.Schema.t() | nil

  @doc """
  Fetches a single record from the query.

  Returns `nil` if no records were found. May raise Cassandrax.MultipleResultsError,
  if query returns more than one entry.

  ## Example
  ```
  query = where(User, id: 1)
  MyKeyspace.one(query)
  ```
  """
  @callback one(queryable :: Cassandrax.Queryable.t(), opts :: Keyword.t()) ::
              Cassandrax.Schema.t() | nil

  @doc """
  Runs plain CQL Statements.

  Returns `{:ok, map}` if the CQL is successfully run or `{:error, message}`
  if there was a validation or a known constraint error.

  ## Example
  ```
  statement = \"""
  SELECT * my_keyspace.user
  \"""

  Cassandrax.cql(MyConn, statement)
  ```
  """
  @callback cql(statement :: String.t() | list, values :: list, opts :: Keyword.t()) ::
              {:ok, map} | {:error, map}

  @doc """
  Runs batch queries.

  Can be used to group and execute queries as Cassandra `BATCH` query.

  ## Options
  `:logged` is the default behavior in Cassandrax. Logged batch acts like a lightweight
  transaction around a batch operation. It enforces atomicity, and fails the batch if any of the queries fail.
  Cassandra doesn't enforce any other transactional properties at batch level.

  `:unlogged` consider it when there are multiple inserts and updates for the same partition key.
  Unlogged batching will give a warning if too many operations or too many partitions are involved.

  Read the CassandraDB documents for more information logged and unlogged batch operations.

  ## Example
  ```
  user = MyKeyspace.get(User, id: 1)
  changeset = Ecto.Changeset.change(user, user_name: "trent")

  MyKeyspace.batch(fn batch ->
    batch
    |> MyKeyspace.batch_insert(%User{id: 3, user_name: "eve"})
    |> MyKeyspace.batch_insert(%User{id: 4, user_name: "mallory"})
    |> MyKeyspace.batch_update(changeset)
    |> MyKeyspace.batch_delete(user)
  end)
  ```
  """
  @callback batch(opts :: Keyword.t(), fun()) :: :ok | {:error, any()}

  @doc """
  Adds an `INSERT` query to the given batch.
  """
  @callback batch_insert(batch :: Cassandrax.Keyspace.Batch.t(), Cassandrax.Schema.t()) :: none()

  @doc """
  Adds an `UPDATE` query to the given batch.
  """
  @callback batch_update(batch :: Cassandrax.Keyspace.Batch.t(), Cassandrax.Schema.t()) :: none()

  @doc """
  Adds a `DELETE` query to the given batch.
  """
  @callback batch_delete(batch :: Cassandrax.Keyspace.Batch.t(), Cassandrax.Schema.t()) :: none()
end
