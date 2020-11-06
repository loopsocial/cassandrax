defmodule Cassandrax.Keyspace.Queryable do
  @doc """
  Implementation for `Cassandrax.Keyspace.all/2`.
  """
  require Cassandrax.Query

  def all(keyspace, queryable, opts) when is_list(opts) do
    conn = keyspace.__conn__
    queryable = Cassandrax.Queryable.to_query(queryable)

    {statement, values} = Cassandrax.Connection.all(keyspace, queryable)

    case Cassandrax.cql(conn, statement, values, opts) do
      {:ok, results} -> convert_results(queryable, results)
      {:error, error} -> raise error
    end
  end

  defp convert_results(%{schema: schema}, results), do: Enum.map(results, &schema.convert/1)

  @doc """
  Implementation for `Cassandrax.Keyspace.get/3`.
  """
  def get(keyspace, queryable, primary_key, opts) when is_list(primary_key) do
    one(keyspace, query_for_get(queryable, primary_key), opts)
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

  defp query_for_get(_queryable, empty) when is_nil(empty) or empty == [] do
    raise ArgumentError, "cannot perform Cassandrax.Keyspace.get/2 with an empty primary key"
  end

  defp query_for_get(queryable, primary_key) when is_list(primary_key) do
    query = Cassandrax.Queryable.to_query(queryable)
    %{allow_filtering: allow_filtering} = query
    schema = assert_schema!(query)

    [partition_keys | clustering_keys] = schema.__schema__(:primary_key)
    {partition_filters, other_filters} = Keyword.split(primary_key, partition_keys)

    partition_filters = filters_for_partition(allow_filtering, partition_keys, partition_filters)
    other_filters = filters_for_others(allow_filtering, clustering_keys, other_filters)
    filters = Keyword.merge(partition_filters, other_filters)
    Cassandrax.Query.where(query, ^filters)
  end

  defp query_for_get(_queryable, value) do
    raise ArgumentError,
          "Cassandrax.Keyspace.get/2 requires a Keyword primary_key, " <>
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

  defp filters_for_others(false, clustering_keys, filters) do
    for {clustering_key, value} <- filters, into: [] do
      unless Enum.member?(clustering_keys, clustering_key) do
        raise ArgumentError,
              "Cannot perform Cassandrax.get/2 with non-primary key filters. " <>
                "If you need data filtering, use `allow_filtering/0` to enable slow queries."
      end

      {clustering_key, value}
    end
  end

  defp assert_schema!(%Cassandrax.Query{schema: schema}), do: schema

  defp assert_schema!(query),
    do:
      raise(Cassandrax.QueryError,
        message: "Expected a query with a schema, got: #{inspect(query)}"
      )
end
