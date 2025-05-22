defmodule Cassandrax.Keyspace.Queryable do
  @moduledoc false

  alias Cassandrax.{Query, Queryable}

  require Cassandrax.Query

  @doc """
  Implementation for `Cassandrax.Keyspace.all/2`.
  """
  def all(keyspace, queryable, opts) when is_list(opts) do
    conn = keyspace.__conn__
    opts = keyspace.__default_options__(:read) |> Keyword.merge(opts)
    queryable = Queryable.to_query(queryable)

    {statement, values} = Cassandrax.Connection.all(keyspace, queryable)

    case Cassandrax.cql(conn, statement, values, opts) do
      {:ok, results} -> convert_results(queryable, results)
      {:error, error} -> raise error
    end
  end

  @doc """
  Implementation for `Cassandrax.Keyspace.delete_all/2`.
  """
  def delete_all(keyspace, queryable, opts) when is_list(opts) do
    conn = keyspace.__conn__
    opts = keyspace.__default_options__(:read) |> Keyword.merge(opts)
    query = build_query_for_delete_all(queryable)
    {statement, values} = Cassandrax.Connection.delete_all(keyspace, query)

    case Cassandrax.cql(conn, statement, values, opts) do
      {:ok, _} -> :ok
      {:error, error} -> raise error
    end
  end

  defp convert_results(%{schema: schema}, results) do
    results
    |> Stream.map(&schema.convert/1)
    |> Enum.map(&loaded_metadata/1)
  end

  defp loaded_metadata(%{__meta__: meta} = struct),
    do: %{struct | __meta__: %{meta | state: :loaded}}

  @doc """
  Implementation for `Cassandrax.Keyspace.stream/2`.
  """
  def stream(keyspace, queryable, opts) when is_list(opts) do
    conn = keyspace.__conn__
    opts = keyspace.__default_options__(:read) |> Keyword.merge(opts)
    queryable = Queryable.to_query(queryable)

    with {statement, values} <- Cassandrax.Connection.all(keyspace, queryable),
         page_stream <- Cassandrax.stream_cql(conn, statement, values, opts) do
      Stream.flat_map(page_stream, fn page -> convert_results(queryable, page) end)
    end
  end

  @doc """
  Implementation for `Cassandrax.Keyspace.get/3`.
  """
  def get(keyspace, queryable, primary_key, opts) when is_list(primary_key) do
    one(keyspace, build_query_for_get(queryable, primary_key), opts)
  end

  def get(keyspace, queryable, primary_key, opts) when is_map(primary_key),
    do: get(keyspace, queryable, Keyword.new(primary_key), opts)

  @doc """
  Implementation for `Cassandrax.Keyspace.one/2`.
  """
  def one(keyspace, queryable, opts) do
    case all(keyspace, queryable, opts) do
      [one] -> one
      [] -> nil
      other -> raise Cassandrax.MultipleResultsError, queryable: queryable, count: length(other)
    end
  end

  defp build_query_for_delete_all(queryable) do
    query = Queryable.to_query(queryable)

    filters_input =
      List.foldr(query.wheres, [], fn [key, operator, value], acc ->
        if operator == :==, do: [{key, value} | acc], else: acc
      end)

    build_query_for_function(:delete_all, query, filters_input)
  end

  defp build_query_for_get(queryable, primary_key),
    do: build_query_for_function(:get, Queryable.to_query(queryable), primary_key)

  defp build_query_for_function(action, _query, empty) when is_nil(empty) or empty == [] do
    raise ArgumentError, "cannot perform #{function_name(action)} with an empty primary key"
  end

  defp build_query_for_function(_action, query, primary_key) when is_list(primary_key) do
    %{allow_filtering: allow_filtering} = query
    schema = assert_schema!(query)

    [partition_keys | clustering_keys] = schema.__schema__(:pk)
    {partition_filters, other_filters} = Keyword.split(primary_key, partition_keys)

    partition_filters = filters_for_partition(allow_filtering, partition_keys, partition_filters)
    other_filters = filters_for_others(allow_filtering, clustering_keys, other_filters)

    filters = Keyword.merge(partition_filters, other_filters)
    query |> Query.where(^filters) |> Map.update!(:wheres, &Enum.uniq/1)
  end

  defp build_query_for_function(action, _query, value) do
    raise ArgumentError,
          "#{function_name(action)} requires a Keyword primary_key, " <>
            "got: #{inspect(value)}"
  end

  defp filters_for_partition(true, _partition_keys, filters), do: filters

  defp filters_for_partition(false, partition_keys, partition_filters) do
    for partition_key <- partition_keys, into: [] do
      case Keyword.get(partition_filters, partition_key) do
        nil ->
          raise ArgumentError,
                "Cannot perform Cassandrax.get/2 with a partial partition key. " <>
                  "If you need data filtering, use `allow_filtering/0` to enable slow queries."

        value ->
          {partition_key, value}
      end
    end
  end

  defp filters_for_others(true, _clustering_keys, filters), do: filters

  defp filters_for_others(false, [], filters), do: filters

  defp filters_for_others(false, [clustering_keys], filters) do
    for {clustering_key, value} <- filters, into: [] do
      unless Enum.member?(clustering_keys, clustering_key) do
        raise ArgumentError,
              "Cannot perform Cassandrax.get/2 with non-primary key filters. " <>
                "If you need data filtering, use `allow_filtering/0` to enable slow queries."
      end

      {clustering_key, value}
    end
  end

  defp assert_schema!(%Query{schema: schema}), do: schema

  defp assert_schema!(query),
    do:
      raise(Cassandrax.QueryError,
        message: "Expected a query with a schema, got: #{inspect(query)}"
      )

  defp function_name(:get), do: "Cassandrax.Keyspace.get/2"
  defp function_name(:delete_all), do: "Cassandrax.Keyspace.delete_all/2"
end
