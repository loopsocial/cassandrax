defmodule Cassandrax.Schema.MapSetType do
  @moduledoc """
  Defines the SET collection type used by CassandraDB
  """

  use Ecto.Type
  def type, do: {:array, :string}

  def cast(map) when is_map(map), do: {:ok, parse(map)}
  def cast(list) when is_list(list), do: {:ok, parse(list)}
  def cast(_), do: :error

  defp parse(value) when is_list(value) or is_map(value), do: MapSet.new(value)

  def load(nil), do: {:ok, MapSet.new()}
  def load(list) when is_list(list), do: {:ok, MapSet.new(list)}
  def load(_), do: :error

  def dump(mapset) when is_map(mapset), do: {:ok, MapSet.to_list(mapset)}
  def dump(_), do: :error
end
