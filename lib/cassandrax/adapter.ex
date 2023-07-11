# TODOS:
#   - integrate connection pooling
#   - implement loaders/dumpers
defmodule Cassandrax.Adapter do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migration

  @impl true
  defmacro __before_compile__(_), do: nil

  @impl true
  def init(config) do
    IO.puts "init(#{inspect config})"
    # should return child_spec for Cassandrax.Supervisor, I think
    raise "unimplemented"
  end

  @impl true
  def ensure_all_started(config, type) do
    IO.puts "ensure_all_started(#{inspect config}, #{inspect type})"
    raise "unimplemented"
  end

  @impl true
  def checkout(meta, config, fun) do
    IO.puts "checkout(#{inspect meta}, #{inspect config}, #{inspect fun})"
    raise "unimplemented"
  end

  @impl true
  def dumpers(primitive_type, ecto_type) do
    IO.puts "dumpers(#{inspect primitive_type}, #{inspect ecto_type})"
    raise "unimplemented"
  end

  @impl true
  def loaders(primitive_type, ecto_type) do
    IO.puts "loaders(#{inspect primitive_type}, #{inspect ecto_type})"
    raise "unimplemented"
  end

  @impl true
  def supports_ddl_transaction? do
    IO.puts "supports_ddl_transaction?()"
    false
  end

  # tmp
  defp join_commas([]), do: []
  defp join_commas([x]), do: [x]
  defp join_commas([h|t]), do: [h,","|join_commas(t)]

  # TODO: respect prefix and other table options
  # TODO: clustering, ordering
  # TODO: respect timeout and log options
  @impl true
  def execute_ddl(keyspace, {:create_if_not_exists, table, fields}, opts) do
    if !table.primary_key do
      raise "cassandra tables must have a primary key"
    end

    # TMP -- this should actually call other functions that already exist somewhere
    cassandra_type = fn
      :bigint -> "bigint"
      :naive_datetime -> "date"
    end

    field_declarations = Enum.map(fields, fn
      {:add, name, type, _opts} -> [Atom.to_string(name), cassandra_type.(type)]
    end)

    # forgive me lord, for I have sinned
    # wtb filter_map
    primary_keys =
      fields
      |> Enum.map(fn
        {:add, name, _, opts} -> {name, Keyword.get(opts, :primary_key, false)}
      end)
      |> Enum.filter(fn {_, primary_key?} -> primary_key? end)
      |> Enum.map(fn {name, _} -> Atom.to_string(name) end)

    IO.inspect table
    IO.inspect fields
    IO.inspect [
      "CREATE TABLE IF NOT EXISTS",
      # [keyspace, ".", table.name]
      "#{keyspace.__keyspace__}.#{table.name}",
      "(",
      join_commas(
        field_declarations
        ++ [
          "PRIMARY KEY",
          "(", primary_keys, ")"
        ]
      ),
      ")"
    ]

    require IEx
    IEx.pry()

    raise "unimplented"
  end

  def execute_ddl(meta, command, opts) do
    IO.puts "execute_ddl(#{inspect meta}, #{inspect command}, #{inspect opts})"
    raise "unimplemented"
  end

  @impl true
  def lock_for_migrations(meta, query, opts, fun) do
    IO.puts "lock_for_migrations(#{inspect meta}, #{inspect query}, #{inspect opts}, #{inspect fun})"
    adapter_opts = meta.config()
    if lock = Keyword.get(adapter_opts, :migration_lock) do
      query |> Map.put(:lock, lock) |> fun.()
    else
      fun.(query)
    end
  end

  def storage_up(opts) do
    IO.puts "storage_up(#{inspect opts})"
    {:error, "not implemented"}
  end
end
