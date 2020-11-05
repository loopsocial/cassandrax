defmodule Cassandrax do
  @moduledoc """
  Cassandrax is a [Triton](https://github.com/blitzstudios/triton) and
  [Ecto](https://github.com/elixir-ecto/ecto) inspired ORM and query runner.

  It allows you to build and run CQL statements as well as map results to Elixir structs.
  """
  use Application

  def start(_type, _args) do
    children =
      for cluster <- Application.get_env(:cassandrax, :clusters, []), into: [] do
        config = Application.get_env(:cassandrax, cluster) |> ensure_cluster_config!(cluster)
        Cassandrax.Supervisor.child_spec(cluster, config)
      end

    opts = [strategy: :one_for_one, name: Cassandrax.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def ensure_cluster_config!(empty, cluster) when is_nil(empty) or empty == [],
    do:
      raise(
        Cassandrax.ClusterConfigError,
        "Expected to find keyword configs for " <>
          "#{inspect(cluster)}, found #{inspect(empty)}"
      )

  def ensure_cluster_config!(config, _cluster), do: config
end
