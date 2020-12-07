# partial adapter implementation for migrations only
defmodule Cassandrax.Adapter do
  @behaviour Ecto.Adapter.Migration

  @impl true
  def supports_ddl_transaction? do
    IO.puts "supports_ddl_transaction?()"
    false
  end

  @impl true
  def execute_ddl(meta, command, opts) do
    IO.puts "execute_ddl(#{inspect meta}, #{inspect command}, #{inspect opts})"
    nil
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
