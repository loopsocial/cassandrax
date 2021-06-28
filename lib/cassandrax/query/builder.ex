defmodule Cassandrax.Query.Builder do
  @moduledoc """
  Builds query clauses and adds them to a `Cassandrax.Query`
  """

  @doc """
  Converts the given `data` into a query clause and adds it to the given `Cassandrax.Query`.
  """
  def build(type, queryable, {:^, _, [var]}) do
    quote do
      fragment = Cassandrax.Query.Builder.build_fragment(unquote(type), unquote(var))
      query = Cassandrax.Queryable.to_query(unquote(queryable))
      Cassandrax.Query.Builder.add_fragment(unquote(type), fragment, query)
    end
  end

  def build(type, queryable, [{_, value}] = expression) when is_tuple(value) do
    quote do
      query = Cassandrax.Queryable.to_query(unquote(queryable))
      fragment = Cassandrax.Query.Builder.build_fragment(unquote(type), unquote(expression))
      Cassandrax.Query.Builder.add_fragment(unquote(type), fragment, query)
    end
  end

  def build(type, queryable, value) do
    fragment = build_fragment(type, value)

    quote do
      query = Cassandrax.Queryable.to_query(unquote(queryable))
      Cassandrax.Query.Builder.add_fragment(unquote(type), unquote(fragment), query)
    end
  end

  # TODO fix DSL so contains and contains_key work without having to define a custom where function
  @allowed_operators [
    :==,
    :!=,
    :>,
    :<,
    :>=,
    :<=,
    :in
    # :contains,
    # :contains_key
  ]

  def build_fragment(:where, {operator, _, [field, value]}) when operator in @allowed_operators do
    [field, operator, value]
  end

  def build_fragment(:where, [{field, value}]) when is_list(value) do
    [field, :in, value]
  end

  def build_fragment(:where, [{field, value}]) do
    [field, :==, value]
  end

  def build_fragment(:where, conditions) when is_list(conditions) do
    Enum.map(conditions, &build_fragment(:where, [&1]))
  end

  def build_fragment(_type, value), do: value

  def add_fragment(:where, [], query), do: query

  def add_fragment(:where, [field, operator, _] = filter, query)
      when is_atom(field) and is_atom(operator),
      do: %{query | wheres: [filter | query.wheres]}

  def add_fragment(:where, [filter | filters], query),
    do: add_fragment(:where, filters, add_fragment(:where, filter, query))

  def add_fragment(type, value, query), do: %{query | type => value}
end
