defmodule Cassandrax.Keyspace.Schema do
  @moduledoc """
  This module is invoked by User defined keyspaces for schema related functionality
  """

  alias Ecto.Changeset

  @doc """
  Implementation for `Cassandrax.Keyspace.insert!/2`.
  """
  def insert!(keyspace, struct, opts) do
    case insert(keyspace, struct, opts) do
      {:ok, struct} ->
        struct

      {:error, %Changeset{} = changeset} ->
        raise Ecto.InvalidChangesetError, action: :insert, changeset: changeset

      {:error, xandra_error} ->
        raise xandra_error
    end
  end

  @doc """
  Implementation for `Cassandrax.Keyspace.update!/2`.
  """
  def update!(keyspace, struct, opts) do
    case update(keyspace, struct, opts) do
      {:ok, struct} ->
        struct

      {:error, %Changeset{} = changeset} ->
        raise Ecto.InvalidChangesetError, action: :update, changeset: changeset

      {:error, xandra_error} ->
        raise xandra_error
    end
  end

  @doc """
  Implementation for `Cassandrax.Keyspace.delete!/2`.
  """
  def delete!(keyspace, struct, opts) do
    case delete(keyspace, struct, opts) do
      {:ok, struct} ->
        struct

      {:error, %Changeset{} = changeset} ->
        raise Ecto.InvalidChangesetError, action: :delete, changeset: changeset

      {:error, xandra_error} ->
        raise xandra_error
    end
  end

  @doc """
  Implementation for `Cassandrax.Keyspace.truncate/2`.
  """
  def truncate(keyspace, schema_module_or_table_name) do
    conn = keyspace.__conn__
    keyspace_name = keyspace.__keyspace__

    with {:ok, table} <- table_name_from_module_or_atom(schema_module_or_table_name),
         statement = Cassandrax.Connection.truncate(keyspace_name, table),
         {:ok, results} <- Cassandrax.cql(conn, statement) do
      {:ok, results}
    else
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Implementation for `Cassandrax.Keyspace.insert/2`.
  """
  def insert(keyspace, %Changeset{} = changeset, opts) do
    conn = keyspace.__conn__
    opts = keyspace.__default_options__(:write) |> Keyword.merge(opts)

    with {:ok, {statement, values, changeset}} <- setup_insert(keyspace, changeset),
         {:ok, prepared} <- Cassandrax.Connection.prepare(conn, statement) do
      case Cassandrax.Connection.execute(conn, prepared, values, opts) do
        {:ok, _void_response} -> load_changes(changeset, :loaded)
        {:error, error} -> {:error, error}
      end
    end
  end

  def insert(keyspace, %{__struct__: _} = struct, opts),
    do: insert(keyspace, Ecto.Changeset.change(struct), opts)

  defp setup_insert(keyspace, %Changeset{valid?: true} = changeset) do
    struct = changeset.data
    schema = struct.__struct__
    fields = schema.__schema__(:fields)
    table = schema.__schema__(:source)
    keyspace_name = keyspace.__keyspace__

    changeset = apply_defaults(changeset, struct, fields)
    statement = Cassandrax.Connection.insert(keyspace_name, table, changeset.changes)

    case type_check(schema, Map.to_list(changeset.changes)) do
      {:ok, values} -> {:ok, {statement, values, changeset}}
      other -> other
    end
  end

  defp setup_insert(_keyspace, %Changeset{valid?: false} = changeset),
    do: {:error, changeset}

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
  Implementation for `Cassandrax.Keyspace.update/2`.
  """
  def update(keyspace, %Ecto.Changeset{} = changeset, opts) do
    conn = keyspace.__conn__
    opts = keyspace.__default_options__(:write) |> Keyword.merge(opts)

    with {:ok, {statement, values, changeset}} <- setup_update(keyspace, changeset),
         {:ok, prepared} <- Cassandrax.Connection.prepare(conn, statement) do
      case Cassandrax.Connection.execute(conn, prepared, values, opts) do
        {:ok, _void_response} -> load_changes(changeset, :loaded)
        {:error, error} -> {:error, error}
      end
    end
  end

  def update(keyspace, %{__struct__: _} = struct, opts),
    do: update(keyspace, Ecto.Changeset.change(struct), opts)

  defp setup_update(keyspace, %Changeset{valid?: true} = changeset) do
    struct = changeset.data
    schema = struct.__struct__
    primary_key = schema.__schema__(:pk) |> List.flatten()
    table = schema.__schema__(:source)
    keyspace_name = keyspace.__keyspace__

    statement = Cassandrax.Connection.update(keyspace_name, table, changeset.changes, primary_key)
    primary_key_fields = Enum.map(primary_key, fn field -> {field, Map.get(struct, field)} end)

    with {:ok, changeset_values} <- type_check(schema, Map.to_list(changeset.changes)),
         {:ok, primary_key_values} <- type_check(schema, primary_key_fields) do
      values = changeset_values ++ primary_key_values
      {:ok, {statement, values, changeset}}
    end
  end

  defp setup_update(_keyspace, %Changeset{valid?: false} = changeset),
    do: {:error, changeset}

  @doc """
  Implementation for `Cassandrax.Keyspace.delete/2`.
  """
  def delete(keyspace, %Ecto.Changeset{} = changeset, opts) do
    conn = keyspace.__conn__
    opts = keyspace.__default_options__(:write) |> Keyword.merge(opts)

    with {:ok, {statement, values, changeset}} <- setup_delete(keyspace, changeset),
         {:ok, prepared} <- Cassandrax.Connection.prepare(conn, statement) do
      case Cassandrax.Connection.execute(conn, prepared, values, opts) do
        {:ok, _void_response} -> load_changes(changeset, :deleted)
        {:error, error} -> {:error, error}
      end
    end
  end

  def delete(keyspace, %{__struct__: _} = struct, opts),
    do: delete(keyspace, Ecto.Changeset.change(struct), opts)

  defp setup_delete(keyspace, %Changeset{valid?: true} = changeset) do
    struct = changeset.data
    schema = struct.__struct__
    primary_key = schema.__schema__(:pk) |> List.flatten()
    table = schema.__schema__(:source)
    keyspace_name = keyspace.__keyspace__

    statement = Cassandrax.Connection.delete(keyspace_name, table, primary_key)
    primary_key_fields = Enum.map(primary_key, fn field -> {field, Map.get(struct, field)} end)

    case type_check(schema, primary_key_fields) do
      {:ok, values} -> {:ok, {statement, values, changeset}}
      other -> other
    end
  end

  defp setup_delete(_keyspace, %Changeset{valid?: false} = changeset),
    do: {:error, changeset}

  @doc """
  Implementation for `Cassandrax.Keyspace.batch_insert/2`
  """
  def batch_insert(keyspace, batch, %Changeset{} = changeset) do
    {:ok, {statement, values, _changeset}} = setup_insert(keyspace, changeset)
    Cassandrax.Keyspace.Batch.add(batch, statement, values)
  end

  def batch_insert(keyspace, batch, %{__struct__: _} = struct),
    do: batch_insert(keyspace, batch, Ecto.Changeset.change(struct))

  @doc """
  Implementation for `Cassandrax.Keyspace.batch_update/2`
  """
  def batch_update(keyspace, batch, %Changeset{} = changeset) do
    {:ok, {statement, values, _changeset}} = setup_update(keyspace, changeset)
    Cassandrax.Keyspace.Batch.add(batch, statement, values)
  end

  def batch_update(keyspace, batch, %{__struct__: _} = struct),
    do: batch_update(keyspace, batch, Ecto.Changeset.change(struct))

  @doc """
  Implementation for `Cassandrax.Keyspace.batch_delete/2`
  """
  def batch_delete(keyspace, batch, %Changeset{} = changeset) do
    {:ok, {statement, values, _changeset}} = setup_delete(keyspace, changeset)
    Cassandrax.Keyspace.Batch.add(batch, statement, values)
  end

  def batch_delete(keyspace, batch, %{__struct__: _} = struct),
    do: batch_delete(keyspace, batch, Ecto.Changeset.change(struct))

  defp load_changes(%{data: struct, changes: changes}, state) do
    changes =
      Enum.reduce(changes, changes, fn {key, _value}, changes ->
        if Map.has_key?(struct, key), do: changes, else: Map.delete(changes, key)
      end)

    loaded_changes =
      struct
      |> Map.merge(changes)
      |> update_metadata(state)

    {:ok, loaded_changes}
  end

  defp update_metadata(%{__meta__: meta} = struct, state),
    do: %{struct | __meta__: %{meta | state: state}}

  defp type_check(_, []), do: {:ok, []}

  defp type_check(schema, [{field, value} | remaining_changes]) do
    type = schema.__schema__(:type, field)

    case Ecto.Type.dump(type, value) do
      {:ok, _dumped_value} ->
        case type_check(schema, remaining_changes) do
          {:ok, rest} -> {:ok, [value | rest]}
          other -> other
        end

      :error ->
        {:error,
         %Ecto.ChangeError{
           message:
             "value `#{inspect(value)}` for `#{inspect(schema)}`.`#{field}` does not match type #{type}"
         }}
    end
  end

  defp table_name_from_module_or_atom(module_or_atom) do
    is_module = function_exported?(module_or_atom, :__info__, 1)

    if is_module do
      is_cassandrax_schema =
        Keyword.has_key?(module_or_atom.__info__(:functions), :__cassandrax_source__)

      if is_cassandrax_schema do
        {:ok, module_or_atom.__cassandrax_source__}
      else
        {:error, "module #{module_or_atom} does not use Cassandrax.Schema"}
      end
    else
      {:ok, module_or_atom}
    end
  end
end
