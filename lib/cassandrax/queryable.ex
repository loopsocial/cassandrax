defprotocol Cassandrax.Queryable do
  @moduledoc """
  Converts a data structure into an `Cassandrax.Query`.

  ## Example

    ```
    defmodule User do
      use Cassandrax.Schema

      @primary_key [:id]

      table "users" do
        field :id, :integer
      end
    end

    %Cassandrax.Query{from: "users", schema: User} = Cassandrax.Queryable.to_query(User)
    ```
  """

  @doc """
  Converts the given `data` into an `Cassandrax.Query`.

  May raise a CompileError if the data is invalid.
  """
  def to_query(data)
end

defimpl Cassandrax.Queryable, for: Cassandrax.Query do
  def to_query(query), do: query
end

defimpl Cassandrax.Queryable, for: Atom do
  def to_query(schema) do
    try do
      schema.__schema__(:queryable)
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
end
