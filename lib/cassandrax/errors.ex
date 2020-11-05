defmodule Cassandrax.ClusterConfigError do
  @moduledoc """
  Raised at runtime when a cluster is listed in the env, but no config for it is found
  """
  defexception [:message]
end

defmodule Cassandrax.QueryError do
  @moduledoc """
  Raised at runtime when a query is invalid
  """
  defexception [:message]
end

defmodule Cassandrax.MultipleResultsError do
  @moduledoc """
  Raised at runtime when a query expects one result, but many are returned
  """
  defexception [:message]

  def exception(opts) do
    query = Keyword.fetch!(opts, :queryable) |> Cassandrax.Queryable.to_query()
    count = Keyword.fetch!(opts, :count)

    msg = """
    expected at most one result but got #{count} in query:
    #{inspect(query)}
    """

    %__MODULE__{message: msg}
  end
end
