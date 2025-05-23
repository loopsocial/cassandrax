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
    opts = keyspace.__default_options__(:write) |> Keyword.merge(opts)
    query = validate_queryable!(:delete_all, queryable)
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
    one(keyspace, validate_queryable!(:get, queryable, primary_key), opts)
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

  defp validate_queryable!(action, queryable),
    do: do_validate_queryable!(action, Queryable.to_query(queryable))

  defp validate_queryable!(action, queryable, primary_key) when is_list(primary_key) do
    query = queryable |> Queryable.to_query() |> Query.where(^primary_key)
    do_validate_queryable!(action, query)
  end

  defp validate_queryable!(action, _queryable, value) do
    msg = "#{function_name(action)} requires a Keyword primary_key, got: #{inspect(value)}"
    raise ArgumentError, msg
  end

  defp do_validate_queryable!(action, %{wheres: []}) do
    raise ArgumentError, "cannot perform #{function_name(action)} with an empty primary key"
  end

  defp do_validate_queryable!(action, %{wheres: wheres} = query) do
    %{allow_filtering: allow_filtering} = query
    schema = assert_schema!(query)

    [partition_keys | clustering_keys] = schema.__schema__(:pk)
    group = Enum.group_by(wheres, &(hd(&1) in partition_keys))

    partition_filters =
      group |> Map.get(true, []) |> filters_for_partition(partition_keys, allow_filtering, action)

    other_filters =
      group |> Map.get(false, []) |> filters_for_others(clustering_keys, allow_filtering, action)

    %{query | wheres: partition_filters ++ other_filters}
  end

  defp filters_for_partition(wheres, _partition_keys, true, _action), do: wheres

  defp filters_for_partition(wheres, partition_keys, false, action) do
    wheres_by_key = Map.new(wheres, &{hd(&1), &1})

    error_msg =
      "Cannot perform #{function_name(action)} with a partial partition key. " <>
        "If you need data filtering, use `allow_filtering/0` to enable slow queries."

    for partition_key <- partition_keys, into: [] do
      case Map.get(wheres_by_key, partition_key) do
        nil -> raise ArgumentError, error_msg
        [_, _, nil] -> raise ArgumentError, error_msg
        where -> where
      end
    end
  end

  defp filters_for_others(wheres, _clustering_keys, true, _action), do: wheres

  defp filters_for_others(wheres, [], false, _action), do: wheres

  defp filters_for_others(wheres, [clustering_keys], false, action) do
    wheres_by_key = Map.new(wheres, &{hd(&1), &1})

    for {key, where} <- wheres_by_key, into: [] do
      unless Enum.member?(clustering_keys, key) do
        raise ArgumentError,
              "Cannot perform #{function_name(action)} with non-primary key filters. " <>
                "If you need data filtering, use `allow_filtering/0` to enable slow queries."
      end

      where
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
