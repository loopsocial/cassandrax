defmodule Cassandrax.QueryError do
  @moduledoc """
  Raised at runtime when a query is invalid
  """
  defexception [:message]
end
