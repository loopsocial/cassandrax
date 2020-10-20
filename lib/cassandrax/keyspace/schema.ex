defmodule Cassandrax.Keyspace.Schema do
  @moduledoc """
  This module is invoked by User defined keyspaces for schema related functionality
  """

  alias Ecto.Changeset

  @doc """
  Implementation for `Cassandrax.Keyspace.insert/2`.
  """
  def insert(keyspace, %Changeset{} = changeset, opts) do
    do_insert(keyspace, changeset, opts)
  end

  def insert(keyspace, %{__struct__: _} = struct, opts) do
    do_insert(keyspace, Ecto.Changeset.change(struct), opts)
  end

  defp do_insert(keyspace, changeset, opts) do
    {statement, values} = insert_statement(changeset, keyspace)
    prepared_statement = prepare(keyspace, statement)

    execute(keyspace, prepared_statement, values, opts)
  end

  defp insert_statement(changeset, keyspace) do
    struct = changeset.data
    schema = struct.__struct__
    fields = schema.__schema__(:fields)
    table = [keyspace.__keyspace__, ?., schema.__schema__(:source)]
    changeset = apply_defaults(changeset, struct, fields)
    changes = changeset.changes

    field_names =
      Enum.map(changes, fn {field, _val} -> field |> Atom.to_string() end)
      |> Enum.join(", ")

    placeholders = Enum.map(changes, fn _ -> "?" end) |> Enum.join(", ")
    field_values = Enum.map(changeset.changes, fn {_, value} -> value end)

    values = [?\s, ?(, field_names, ") VALUES (", placeholders, ?)]

    {["INSERT INTO ", table, values], field_values}
  end

  defp apply_defaults(%{changes: changes, types: types} = changeset, struct, fields) do
    changes =
      Enum.reduce(fields, changes, fn field, changes ->
        case {struct, changes, types} do
          # User has explicitly changed this field
          {_, %{^field => _}, _} ->
            changes

          # Struct has a non nil value (probably a default)
          {%{^field => value}, _, %{^field => _}} when value != nil ->
            Map.put(changes, field, value)

          {_, _, _} ->
            changes
        end
      end)

    %{changeset | changes: changes}
  end

  @doc """
  Implementation for `Cassandrax.Keyspace.delete/2`.
  """
  def delete(keyspace, %Ecto.Changeset{} = changeset, opts),
    do: do_delete(keyspace, changeset, opts)

  def delete(keyspace, %{__struct__: _} = struct, opts),
    do: do_delete(keyspace, Ecto.Changeset.change(struct), opts)

  defp do_delete(keyspace, changeset, opts) do
    {statement, values} = delete_statement(keyspace, changeset)
    prepared_statement = prepare(keyspace, statement)

    execute(keyspace, prepared_statement, values, opts)
  end

  defp delete_statement(keyspace, changeset) do
    struct = changeset.data
    schema = struct.__struct__
    partition_key = schema.__schema__(:partition_key)
    clustering_key = schema.__schema__(:clustering_key)
    table = [keyspace.__keyspace__, ?., schema.__schema__(:source)]

    pk = partition_key ++ clustering_key

    filters = Enum.map(pk, fn field -> "#{field} = ?" end) |> Enum.join(" AND ")
    values = Enum.map(pk, fn field -> Map.get(struct, field) end)

    {["DELETE FROM ", table, " WHERE ", filters], values}
  end

  defp prepare(keyspace, iodata) when is_list(iodata) do
    statement = IO.iodata_to_binary(iodata)
    prepare(keyspace, statement)
  end

  defp prepare(keyspace, query_statement) when is_binary(query_statement),
    do: Xandra.Cluster.prepare!(keyspace, query_statement)

  defp prepare(keyspace, schema, operation) when is_atom(schema) and is_atom(operation) do
    query_statement = apply(schema, :query_statement, [operation])
    Xandra.Cluster.prepare!(keyspace, query_statement)
  end

  defp execute(keyspace, prepared_statement, values, opts) do
    default_opts = keyspace.__defaults__(:write_consistency)
    opts = default_opts |> Keyword.merge(opts)

    Xandra.Cluster.execute!(keyspace, prepared_statement, values, opts)
  end
end
