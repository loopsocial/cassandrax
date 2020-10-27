defmodule Cassandrax.Keyspace do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Cassandrax.Keyspace
      @otp_app Keyword.fetch!(opts, :otp_app)
      @keyspace_name Keyword.fetch!(opts, :name)
      @default_write_consistency Keyword.get(opts, :default_write_consistency, :one)
      @default_read_consistency Keyword.get(opts, :default_read_consistency, :one)

      def config do
        {:ok, config} = Cassandrax.Keyspace.Supervisor.runtime_config(__MODULE__, @otp_app, [])

        config
      end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      def __keyspace__, do: @keyspace_name

      def __defaults__(:write_consistency) do
        write_consistency = __MODULE__.config() |> Keyword.get(:default_write_consistency)
        [consistency: write_consistency]
      end

      def __defaults__(:read_consistency) do
        read_consistency = __MODULE__.config() |> Keyword.get(:default_read_consistency)
        [consistency: read_consistency]
      end

      def start_link(opts \\ []) do
        Cassandrax.Keyspace.Supervisor.start_link(
          __MODULE__,
          Module.concat(__MODULE__, Supervisor),
          @otp_app,
          opts
        )
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

      def get(queryable, primary_key, opts \\ []),
        do: Cassandrax.Keyspace.Queryable.get(__MODULE__, queryable, primary_key, opts)

      def one(queryable, opts \\ []),
        do: Cassandrax.Keyspace.Queryable.one(__MODULE__, queryable, opts)

      def cql(statement, values \\ [], opts \\ []),
        do: Cassandrax.Keyspace.Queryable.cql(__MODULE__, statement, values, opts)

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
