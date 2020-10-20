defmodule Cassandrax.Keyspace.Supervisor do
  @moduledoc false

  use Supervisor

  @defaults [timeout: 100, pool_size: 50]

  def start_link(keyspace, otp_app, opts) do
    supervisor_opts = [name: :"#{keyspace}Supervisor"]
    init_args = {keyspace, otp_app, opts}

    Supervisor.start_link(__MODULE__, init_args, supervisor_opts)
  end

  def runtime_config(keyspace, otp_app, opts) do
    config = Application.get_env(otp_app, keyspace, [])

    {authentication, config} = pop_authentication_config(config)

    config =
      @defaults
      |> Keyword.merge(config)
      |> Keyword.merge(opts)
      |> Keyword.merge(authentication)
      |> Keyword.merge(name: keyspace)

    keyspace_init(keyspace, config)
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

  # This allows Users to override configs on runtime by defining `c:init/2` keyspace callback
  defp keyspace_init(keyspace, config) do
    if Code.ensure_loaded?(keyspace) and function_exported?(keyspace, :init, 2) do
      keyspace.init(config)
    else
      {:ok, config}
    end
  end

  ## Callbacks

  @doc false
  def init({keyspace, otp_app, opts}) do
    {:ok, opts} = runtime_config(keyspace, otp_app, opts)
    child_spec = %{id: make_ref(), start: {Xandra.Cluster, :start_link, [opts]}}
    Supervisor.init([child_spec], strategy: :one_for_one)
  end
end
