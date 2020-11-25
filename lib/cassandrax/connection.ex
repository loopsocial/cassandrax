defmodule Cassandrax.Connection do
  def child_spec(opts) do
    %{id: make_ref(), start: {Xandra.Cluster, :start_link, [opts]}}
  end

  def all(keyspace, queryable) do
    select = select(queryable)
    from = from(queryable, keyspace.__keyspace__)
    {where, filters} = where(queryable)
    group_by = group_by(queryable)
    order_by = order_by(queryable)
    {per_partition_limit, per_partition_limit_value} = per_partition_limit(queryable)
    {limit, limit_value} = limit(queryable)
    allow_filtering = allow_filtering(queryable)

    statement = [
      select,
      from,
      where,
      group_by,
      order_by,
      per_partition_limit,
      limit,
      allow_filtering
    ]

    values = filters ++ per_partition_limit_value ++ limit_value
    {statement, values}
  end

  defp select(%{select: [], distinct: []}), do: ["SELECT *"]

  defp select(%{select: fields, distinct: []}) when is_list(fields),
    do: ["SELECT ", intersperse_map(fields, ", ", &quote_name(&1))]

  defp select(%{distinct: fields}) when is_list(fields),
    do: ["SELECT DISTINCT ", intersperse_map(fields, ", ", &quote_name(&1))]

  defp from(%{from: table}, keyspace), do: [" FROM ", quote_table(keyspace, table)]

  defp where(%{wheres: []}), do: {[], []}

  defp where(%{wheres: wheres}) when is_list(wheres) do
    values = Enum.map(wheres, fn [_, _, value] -> value end)

    where = [
      " WHERE ",
      intersperse_map(wheres, " AND ", fn [field, operator, _value] ->
        [?(, quote_name(field), boolean_operator(operator), ??, ?)]
      end)
    ]

    {where, values}
  end

  defp boolean_operator(:==), do: " = "
  defp boolean_operator(:!=), do: " != "
  defp boolean_operator(:>), do: " > "
  defp boolean_operator(:<), do: " < "
  defp boolean_operator(:>=), do: " <= "
  defp boolean_operator(:<=), do: " >= "
  defp boolean_operator(:in), do: " IN "
  defp boolean_operator(:contains), do: " CONTAINS "
  defp boolean_operator(:contains_key), do: " CONTAINS KEY "

  defp group_by(%{group_bys: []}), do: []

  defp group_by(%{group_bys: group_bys}) when is_list(group_bys) do
    [" GROUP BY (", intersperse_map(group_bys, ", ", &quote_name(&1)), ?)]
  end

  defp order_by(%{order_bys: []}), do: []

  defp order_by(%{order_bys: order_bys}) when is_list(order_bys) do
    [" ORDER BY (", intersperse_map(order_bys, ", ", &quote_name(&1)), ?)]
  end

  defp per_partition_limit(%{per_partition_limit: nil}), do: {[], []}

  defp per_partition_limit(%{per_partition_limit: per_partition_limit}),
    do: {[" PER PARTITION LIMIT ?"], [per_partition_limit]}

  defp limit(%{limit: nil}), do: {[], []}
  defp limit(%{limit: limit}), do: {[" LIMIT ?"], [limit]}

  defp allow_filtering(%{allow_filtering: false}), do: []
  defp allow_filtering(%{allow_filtering: true}), do: [" ALLOW FILTERING"]

  def insert(keyspace, table, changes) do
    changes = Enum.to_list(changes)

    field_names =
      intersperse_map(changes, ", ", fn {field, _} ->
        field |> Atom.to_string() |> quote_name()
      end)

    placeholders = intersperse_map(changes, ", ", fn _ -> ?? end)

    values = [?\s, ?(, field_names, ") VALUES (", placeholders, ?)]

    ["INSERT INTO ", quote_table(keyspace, table) | values]
  end

  def update(keyspace, table, changes, filters) do
    fields =
      changes
      |> Enum.to_list()
      |> intersperse_map(", ", fn {field, _} ->
        [quote_name(field), " = ?"]
      end)

    filters = assemble_filters(filters)

    ["UPDATE ", quote_table(keyspace, table), " SET ", fields, " WHERE " | filters]
  end

  def delete(keyspace, table, filters) do
    filters = assemble_filters(filters)

    ["DELETE FROM ", quote_table(keyspace, table), " WHERE " | filters]
  end

  defp assemble_filters(filters) do
    intersperse_map(filters, " AND ", fn field ->
      field = field |> Atom.to_string() |> quote_name()
      "#{field} = ?"
    end)
  end

  def prepare(conn, iodata) when is_list(iodata) do
    statement = IO.iodata_to_binary(iodata)
    prepare(conn, statement)
  end

  def prepare(%Cassandrax.Keyspace.Batch{conn: conn}, query_statement)
      when is_binary(query_statement),
      do: Xandra.prepare(conn, query_statement)

  def prepare(conn, query_statement) when is_binary(query_statement),
    do: Xandra.Cluster.prepare(conn, query_statement)

  def execute(conn, %Xandra.Prepared{} = prepared, values, opts),
    do: Xandra.Cluster.execute(conn, prepared, values, opts)

  def execute(%Cassandrax.Keyspace.Batch{conn: conn, xandra_batch: xandra_batch}, opts),
    do: Xandra.execute(conn, xandra_batch, opts)

  defp quote_name(atom) when is_atom(atom), do: quote_name(Atom.to_string(atom))
  defp quote_name(string) when is_binary(string), do: [?", string, ?"]

  defp quote_table(keyspace, table), do: [quote_name(keyspace), ?., quote_name(table)]

  defp intersperse_map(enum, separator, mapper), do: do_intersperse_map(enum, separator, mapper)
  defp do_intersperse_map([element | []], _, mapper), do: [mapper.(element)]

  defp do_intersperse_map([element | rest], separator, mapper),
    do: [mapper.(element), separator | do_intersperse_map(rest, separator, mapper)]
end
