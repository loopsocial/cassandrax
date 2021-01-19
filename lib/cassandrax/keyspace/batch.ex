defmodule Cassandrax.Keyspace.Batch do
  @moduledoc false

  defstruct [:xandra_batch, :conn]

  @doc """
  Implementation for `Cassandrax.Keyspace.batch/2`
  """
  def run(keyspace, fun, opts) do
    type = if Keyword.get(opts, :logged, false), do: :logged, else: :unlogged

    Xandra.Cluster.run(keyspace.__conn__, fn conn ->
      xandra_batch = Xandra.Batch.new(type)
      batch = %Cassandrax.Keyspace.Batch{conn: conn, xandra_batch: xandra_batch} |> fun.()
      opts = keyspace.__default_options__(:write) |> Keyword.merge(opts)

      case Cassandrax.Connection.execute(batch, opts) do
        {:ok, %Xandra.Void{}} -> :ok
        {:error, error} -> {:error, error}
      end
    end)
  end

  def add(%Cassandrax.Keyspace.Batch{xandra_batch: xandra_batch} = batch, statement, values) do
    {:ok, prepared} = Cassandrax.Connection.prepare(batch, statement)
    xandra_batch = Xandra.Batch.add(xandra_batch, prepared, values)
    %{batch | xandra_batch: xandra_batch}
  end
end
