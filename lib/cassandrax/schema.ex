defmodule Cassandrax.Schema do
  @moduledoc """
  Defines a schema.

  This schema is used to map data fetched from a CassandraDB node into an Elixir struct.

  `Cassandrax.Schema` mixin uses `Ecto.Schema` mixin.
  """
  @type t :: struct()

  @doc false
  defmacro __using__(_opts) do
    quote do
      # First we import the Schema macros
      import Cassandrax.Schema

      # Use Ecto.Schema to leverage the struct and other helpers
      use Ecto.Schema

      # Include the custom types available in CassandraDB, but not mapped by Ecto.Schema
      alias Cassandrax.Schema.MapSetType

      Module.register_attribute(__MODULE__, :partition_key, accumulate: true)
    end
  end

  @doc """
  Defines an embedded schema for the Cassandra table with the given field definitions.

  In order to create a schema, you must define a `@primary_key` before the schema definition.
  Unlike `Ecto.Schema`, `Cassandrax.Schema` won't automatically generate a primary key which is
  named `id`.  `@primary_key` configures the schema primary key and it expects a list of key(s).
  You can set a single primary key which is the partition key in Cassandra or a list of keys
  where the first key is the partition key and the rest are the clustering keys which are
  responsible for sorting data within the partition.

  You can use Ecto's schema to leverage field definitions and metadata.

  ## Example

    ```
    defmodule User do
      use Cassandrax.Schema

      @primary_key [:id]

      table "users" do
        field :id, :integer
        field :user_name, :string
      end
    end
    ```
  """
  defmacro table(source, do: block) do
    quote do
      pk = @primary_key

      if !pk or pk == [] do
        raise(Cassandrax.SchemaError,
          message: "You must define a @primary_key before the schema definition"
        )
      end

      [partition_keys | clustering_keys] = pk
      partition_keys = List.flatten([partition_keys])

      @primary_key [partition_keys, clustering_keys]

      def __schema__(:queryable), do: %Cassandrax.Query{from: unquote(source), schema: __MODULE__}
      def __schema__(:pk), do: @primary_key

      # Set it to false to bypass Ecto primary_key verification
      @primary_key false

      # Use Ecto's schema to leverage field definitions and metadata
      schema(unquote(source), do: unquote(block))

      # This fetches the defined fields within the schema
      schema_fields = Keyword.keys(@ecto_changeset_fields)

      for partition_key <- partition_keys do
        if partition_key not in schema_fields do
          raise Cassandrax.SchemaError,
            message:
              "@primary_key defines a partition key that wasn't defined in the schema: #{inspect(partition_key)}"
        end

        Module.put_attribute(__MODULE__, :partition_key, partition_key)
      end

      if @partition_key == [] do
        raise(Cassandrax.SchemaError,
          message: "@primary_key cannot define an empty partition_key"
        )
      end

      for clustering_key <- clustering_keys do
        if clustering_key not in schema_fields do
          raise Cassandrax.SchemaError,
            message:
              "@primary_key defines a clustering key that wasn't defined in the schema: #{inspect(clustering_key)}"
        end
      end

      def convert(nil), do: nil

      def convert(data) when is_map(data) do
        sanitized_map =
          apply(__MODULE__, :__schema__, [:fields])
          |> Enum.map(fn key -> {key, Map.get(data, to_string(key))} end)
          |> Map.new()

        struct(__MODULE__, sanitized_map)
      end
    end
  end

  @doc """
  Converts a map of data into a struct for this module.
  """
  @callback convert(data :: map | nil) :: struct | nil
end
