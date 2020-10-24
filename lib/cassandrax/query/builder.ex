defmodule Cassandrax.Query.Builder do
  @moduledoc """
  Converts a data structure into an `Cassandrax.Query`.
  """

  @doc """
  Converts the given `data` into an `Cassandrax.Query`.
  """
  def build(type, queryable, value) do
    quote do
      query = Cassandrax.Queryable.to_query(unquote(queryable))
      Cassandrax.Query.Builder.add_fragment(unquote(type), unquote(value), query)
    end
  end

  def add_fragment(:where, filter, query),
    do: %{query | wheres: Keyword.merge(filter, query.wheres)}

  def add_fragment(type, value, query), do: %{query | type => value}
end
