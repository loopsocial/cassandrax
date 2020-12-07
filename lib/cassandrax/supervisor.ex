defmodule Cassandrax.Supervisor do
  @moduledoc false

  use Supervisor

  @defaults [
    timeout: 5_000,
    pool_size: 50,
    write_options: [consistency: :one],
    read_options: [consistency: :one]
  ]

  def child_spec(cluster_or_keyspace, config) do
    %{
      id: make_ref(),
      start: {__MODULE__, :start_link, [cluster_or_keyspace, config]},
      type: :supervisor
    }
  end

  def start_link(cluster_or_keyspace, config) do
    opts = [name: Module.concat(cluster_or_keyspace, Supervisor)]
    init_args = {cluster_or_keyspace, config}
    Supervisor.start_link(__MODULE__, init_args, opts)
  end

  # By default the runtime configs do not include a after_connect function to call
  # USE keyspace_name, so you can use a keyspace agnostic connection to run migrations that
  # actually create the keyspace
  def runtime_config(cluster_or_keyspace, config) do
    cluster_config =
      Application.get_env(:cassandrax, cluster_or_keyspace, []) |> Keyword.merge(config)

    {authentication, config} = pop_authentication_config(cluster_config)

    config =
      @defaults
      |> Keyword.merge(config)
      |> Keyword.merge(authentication)
      |> Keyword.merge(name: cluster_or_keyspace)

    custom_init(cluster_or_keyspace, config)
  end

  defp pop_authentication_config(config) do
    {username, config} = Keyword.pop(config, :username)
    {password, config} = Keyword.pop(config, :password)

    authentication =
      if username && password do
        [
          authentication: {
            Xandra.Authenticator.Password,
            [
              username: username,
              password: password
            ]
          }
        ]
      else
        []
      end

    {authentication, config}
  end

  # This allows Users to override configs on runtime by defining `c:init/2` callback in the module
  # given to start_link as `name` argument
  defp custom_init(cluster_or_keyspace, config) do
    if Code.ensure_loaded?(cluster_or_keyspace) and
         function_exported?(cluster_or_keyspace, :init, 2) do
      cluster_or_keyspace.init(config)
    else
      {:ok, config}
    end
  end

  ## Callbacks

  @impl true
  def init({cluster_or_keyspace, config}) do
    {:ok, opts} = runtime_config(cluster_or_keyspace, config)

    child_specs = [Cassandrax.Connection.child_spec(opts)]

    Supervisor.init(child_specs, strategy: :one_for_one)
  end
end
