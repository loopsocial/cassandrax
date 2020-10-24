defmodule Cassandrax.Keyspace.Queryable do
  @doc """
  Implementation for `Cassandrax.Keyspace.all/2`.
  """
  def all(keyspace, queryable, opts) when is_list(opts) do
    # TODO this is where the magic is assembled
  end

  @doc """
  Implementation for `Cassandrax.Keyspace.get/3`.
  """
  def get(keyspace, queryable, primary_key, opts) when is_list(primary_key) do
    all(keyspace, query_for_get(queryable, primary_key), opts)
  end

  def get(keyspace, queryable, primary_key, opts) when is_map(primary_key),
    do: get(keyspace, queryable, Keyword.new(primary_key), opts)

  defp query_for_get(queryable, primary_key) do
    # Transform queryable + primary_key in a queryable
  end
end
