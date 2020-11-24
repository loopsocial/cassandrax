defmodule Cassandrax.Query do
  @moduledoc """
    Provides the query macros.

    Queries are used to retrieve or manipulate data from a repository (see Cassandrax.Keyspace).
  """
  alias Cassandrax.Query.Builder

  @limit_default 100
  @per_partition_limit_default 100

  defstruct schema: nil,
            select: [],
            distinct: [],
            from: nil,
            wheres: [],
            limit: nil,
            per_partition_limit: nil,
            group_bys: [],
            order_bys: [],
            allow_filtering: false

  defmacro select(queryable, select \\ []), do: Builder.build(:select, queryable, select)
  defmacro where(queryable, where \\ []), do: Builder.build(:where, queryable, where)
  defmacro limit(queryable, limit \\ @limit_default), do: Builder.build(:limit, queryable, limit)
  defmacro order_by(queryable, order_by \\ []), do: Builder.build(:order_bys, queryable, order_by)
  defmacro group_by(queryable, group_by \\ []), do: Builder.build(:group_bys, queryable, group_by)
  defmacro distinct(queryable, distinct \\ []), do: Builder.build(:distinct, queryable, distinct)
  defmacro allow_filtering(queryable), do: Builder.build(:allow_filtering, queryable, true)

  defmacro per_partition_limit(queryable, per_partition_limit \\ @per_partition_limit_default),
    do: Builder.build(:per_partition_limit, queryable, per_partition_limit)
end
