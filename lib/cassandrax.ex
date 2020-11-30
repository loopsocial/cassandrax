defmodule Cassandrax do
  @moduledoc """
  Cassandrax is a Cassandra ORM built on top of [Xandra](https://github.com/lexhide/xandra) library and
  [Ecto](https://github.com/elixir-ecto/ecto) data mapping.

  Cassandrax is inspired by the [Triton](https://github.com/blitzstudios/triton) and
  [Ecto](https://github.com/elixir-ecto/ecto) projects.

  It allows you to build and run CQL statements as well as map results to Elixir structs.
  """
  use Application

  def start(_type, _args) do
    Application.get_env(:cassandrax, :clusters, [])
    |> Enum.map(fn cluster ->
      config = Application.get_env(:cassandrax, cluster) |> ensure_cluster_config!(cluster)
      Cassandrax.Supervisor.child_spec(cluster, config)
    end)
    |> start_link()
  end

  def ensure_cluster_config!(empty, cluster) when is_nil(empty) or empty == [] do
    raise(
      Cassandrax.ClusterConfigError,
      "Expected to find keyword configs for #{inspect(cluster)}, found #{inspect(empty)}"
    )
  end

  def ensure_cluster_config!(config, _cluster), do: config

  def start_link(children) do
    Supervisor.start_link(children, strategy: :one_for_one, name: Cassandrax.Supervisor)
  end

  def cql(conn, statement, values \\ [], opts \\ []) do
    case Cassandrax.Connection.prepare(conn, statement) do
      {:ok, prepared} -> Cassandrax.Connection.execute(conn, prepared, values, opts)
      {:error, error} -> {:error, error}
    end
  end
end
