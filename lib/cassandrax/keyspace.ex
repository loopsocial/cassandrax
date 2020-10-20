defmodule Cassandrax.Keyspace do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Cassandrax.Keyspace
      @otp_app Keyword.fetch!(opts, :otp_app)
      @keyspace_name Keyword.fetch!(opts, :name)

      # TODO @tdtadeu Why create the following function?
      # def config do
      #   {:ok, config} =
      #     Cassandrax.Keyspace.Supervisor.runtime_config(:runtime, __MODULE__, @otp_app, [])

      #   config
      # end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      def start_link(opts \\ []) do
        Cassandrax.Keyspace.Supervisor.start_link(__MODULE__, @otp_app, opts)
      end
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
