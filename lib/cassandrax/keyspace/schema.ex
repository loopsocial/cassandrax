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
  Implementation for `Cassandrax.Keyspace.insert/2`.
  """
  def insert(keyspace, %Changeset{} = changeset, opts), do: do_insert(keyspace, changeset, opts)

  def insert(keyspace, %{__struct__: _} = struct, opts),
    do: do_insert(keyspace, Ecto.Changeset.change(struct), opts)

  defp do_insert(keyspace, %Changeset{valid?: true} = changeset, opts) do
    struct = changeset.data
    schema = struct.__struct__
    fields = schema.__schema__(:fields)
    table = schema.__schema__(:source)
    keyspace_name = keyspace.__keyspace__

    changeset = apply_defaults(changeset, struct, fields)
    statement = Cassandrax.Connection.insert(keyspace_name, table, changeset.changes)
    {:ok, prepared_statement} = Cassandrax.Connection.prepare(keyspace, statement)

    values = Enum.map(changeset.changes, fn {_field, value} -> value end)

    case Cassandrax.Connection.execute(keyspace, prepared_statement, values, opts) do
      {:ok, _void_response} -> load_changes(changeset)
      {:error, error} -> {:error, error}
    end
  end

  defp do_insert(_keyspace, %Changeset{valid?: false} = changeset, _opts), do: {:error, changeset}

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
  def update(keyspace, %Ecto.Changeset{} = changeset, opts),
    do: do_update(keyspace, changeset, opts)

  def update(keyspace, %{__struct__: _} = struct, opts),
    do: do_update(keyspace, Ecto.Changeset.change(struct), opts)

  defp do_update(keyspace, %Changeset{valid?: true} = changeset, opts) do
    struct = changeset.data
    schema = struct.__struct__
    primary_key = schema.__schema__(:primary_key) |> List.flatten()
    table = schema.__schema__(:source)
    keyspace_name = keyspace.__keyspace__

    statement = Cassandrax.Connection.update(keyspace_name, table, changeset.changes, primary_key)
    {:ok, prepared_statement} = Cassandrax.Connection.prepare(keyspace, statement)

    values = Enum.map(changeset.changes, fn {_field, value} -> value end)
    values = values ++ Enum.map(primary_key, fn field -> Map.get(struct, field) end)

    case Cassandrax.Connection.execute(keyspace, prepared_statement, values, opts) do
      {:ok, _void_response} -> load_changes(changeset)
      {:error, error} -> {:error, error}
    end
  end

  defp do_update(_keyspace, %Changeset{valid?: false} = changeset, _opts), do: {:error, changeset}

  @doc """
  Implementation for `Cassandrax.Keyspace.delete/2`.
  """
  def delete(keyspace, %Ecto.Changeset{} = changeset, opts),
    do: do_delete(keyspace, changeset, opts)

  def delete(keyspace, %{__struct__: _} = struct, opts),
    do: do_delete(keyspace, Ecto.Changeset.change(struct), opts)

  defp do_delete(keyspace, %Changeset{valid?: true} = changeset, opts) do
    struct = changeset.data
    schema = struct.__struct__
    primary_key = schema.__schema__(:primary_key) |> List.flatten()
    table = schema.__schema__(:source)
    keyspace_name = keyspace.__keyspace__

    statement = Cassandrax.Connection.delete(keyspace_name, table, primary_key)
    {:ok, prepared_statement} = Cassandrax.Connection.prepare(keyspace, statement)

    values = Enum.map(primary_key, fn field -> Map.get(struct, field) end)

    case Cassandrax.Connection.execute(keyspace, prepared_statement, values, opts) do
      {:ok, _void_response} -> load_changes(changeset)
      {:error, error} -> {:error, error}
    end
  end

  defp do_delete(_keyspace, %Changeset{valid?: false} = changeset, _opts), do: {:error, changeset}

  defp load_changes(%{data: struct, changes: changes}) do
    changes =
      Enum.reduce(changes, changes, fn {key, _value}, changes ->
        if Map.has_key?(struct, key), do: changes, else: Map.delete(changes, key)
      end)

    loaded_changes = Map.merge(struct, changes)

    {:ok, loaded_changes}
  end
end
