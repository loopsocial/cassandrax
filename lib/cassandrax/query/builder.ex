defmodule Cassandrax.Query.Builder do
  @moduledoc """
  Converts a data structure into an `Cassandrax.Query`.
  """

  @doc """
  Converts the given `data` into an `Cassandrax.Query`.
  """
  def build(type, queryable, value) do
    fragment = build_fragment(type, value)

    quote do
      query = Cassandrax.Queryable.to_query(unquote(queryable))
      Cassandrax.Query.Builder.add_fragment(unquote(type), unquote(fragment), query)
    end
  end

  defp build_fragment(:where, {operator, _, [field, value]}), do: [field, operator, value]
  defp build_fragment(:where, [{field, value}]) when is_list(value), do: [field, :in, value]
  defp build_fragment(:where, [{field, value}]), do: [field, :==, value]
  defp build_fragment(_type, value), do: value

  def add_fragment(:where, filter, query) do
    %{query | wheres: [filter | query.wheres]}
  end

  def add_fragment(type, value, query), do: %{query | type => value}
end
