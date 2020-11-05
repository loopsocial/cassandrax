defmodule Cassandrax.Keyspace do
  @moduledoc """
  Defines a Keyspace.

  A keyspace acts as a repository, wrapping the underlying keyspace which stores
  its tables in CassandraDB.
  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Cassandrax.Keyspace

      @keyspace_name Keyword.fetch!(opts, :name)
      @cluster Keyword.fetch!(opts, :cluster)
      @cluster_config Application.get_env(:cassandrax, @cluster)
      @conn_pool Keyword.get(opts, :pool, @cluster)

      def config do
        {:ok, config} = Cassandrax.Supervisor.runtime_config(@cluster, @cluster_config)

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
end
