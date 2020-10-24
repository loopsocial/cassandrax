defmodule Cassandrax.Query.Builder do
  @moduledoc """
  Converts a data structure into an `Ecto.Query`.
  """

  @doc """
  Converts the given `data` into an `Ecto.Query`.
  """
  def build(type, queryable, value) do
    quote do
      query = Cassandrax.Query.Builder.query(unquote(queryable))
      Cassandrax.Query.Builder.add_fragment(unquote(type), unquote(value), query)
    end
  end

  def query(%Cassandrax.Query{} = query), do: query

  def query(schema) when is_atom(schema) do
    try do
      schema.__schema__(:query)
    rescue
      UndefinedFunctionError ->
        description =
          if :code.is_loaded(schema) do
            "the given module does not provide a schema"
          else
            "the given module does not exist"
          end

        raise CompileError, description: description
    end
  end

  def add_fragment(:where, filter, query),
    do: %{query | wheres: Keyword.merge(filter, query.wheres)}

  def add_fragment(type, value, query), do: %{query | type => value}
end
