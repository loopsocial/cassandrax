defmodule Cassandrax.Query do
  @moduledoc """
    Provides the query macros.

    Queries are used to retrieve or manipulate data from a repository (see Cassandrax.Keyspace).
  """
  alias Cassandrax.Query.Builder

  @type t :: %__MODULE__{}

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

  @doc """
  A select query expression.

  Selects the fields from the schema and any transformations that should be performed on the fields.
  Any expression that is accepted in a query can be a select field.

  Allows a list, tuple or a map. A full schema can also be selected. Only one select expression
  allowed in a query.  If there is no select expression, the full schema will be selected by
  default.

  Accepts a list of atoms where atoms refer to fields.

  ## Example
  ```
  query = select(User, [:id])
  %Cassandrax.Query{from: "users", schema: Cassandrax.User, select: [:id]} = query
  ```
  """
  @callback select(queryable :: Cassandrax.Queryable.t(), select :: Keyword.t()) ::
              Cassandrax.Query.t()

  @doc """
  A where query expression that works like an `AND` operator.

  Used to filter the results. You can chain `where` expressions.

  ## Example
  Single where clause.
  ```
  query = where(User, id: 1)

  ```
  You can chain where clauses.
  ```
  query = User |> where(:id > 1) |> where(:user_name != "alice")
  ```
  CassandraDB doesn't allow certain queries to be executed for performance reasons, such as `where`.
  You may need to use `allow_filtering\0` to bypass this.
  ```
  query = User |> allow_filtering() |> where(:id > 1) |> where(:user_name != "alice")
  ```
  """
  @callback where(queryable :: Cassandrax.Queryable.t(), where :: Keyword.t()) ::
              Cassandrax.Query.t()

  @doc """
  A limit query expression.

  Limits the number of rows to be returned from the result. Requires an integer, fields cannot be included.
  Default limit is 100.

  Limit expressions are chainable, however, the last limit expression will take precedence.

  ## Example
  ```
  query = limit(User, 200)
  ```
  """
  @callback limit(queryable :: Cassandrax.Queryable.t(), limit :: Keyword.t()) ::
              Cassandrax.Query.t()

  @doc """
  An order by query expression.

  Orders the fields based on a given key or list of keys. Order by needs to be paired with a where clause, specifically with where clauses that have equality or `in`. You also need to setup the table correctly to be able to perform order by queries.

  ## Example Table Setup

  ```
  statement = [
      "CREATE TABLE IF NOT EXISTS ",
      "MyKeyspace.ordered_(",
      "id int, ",
      "device_id int, ",
      "value text, ",
      "PRIMARY KEY (id, device_id))",
      "WITH CLUSTERING ORDER BY (device_id DESC)"
    ]

  Cassandrax.cql(MyConn, statement)
  ```

  ## Example
  ```
  query = User |> allow_filtering() |> where(:id == 1) |> order_by([:device_id])
  ```
  """
  @callback order_by(queryable :: Cassandrax.Queryable.t(), order_by :: Keyword.t()) ::
              Cassandrax.Query.t()

  @doc """
  A group by query expression.

  Allows to condense into a single row all selected rows that share the same values for a set of columns.
  Only available for partition key level or at a clustering column level.

  ## Example
  ```
  query = User |> allow_filtering() |> group_by([:id])
  ```
  """
  @callback group_by(queryable :: Cassandrax.Queryable.t(), order_by :: Keyword.t()) ::
              Cassandrax.Query.t()

  @doc """
  A distinct query expression.

  Only returns the distinct records from the result. Only works with a list of partition_key(s).

  ##Example
  ```
  query = distinct(TestSchema, [:id])
  ```
  """
  @callback distinct(queryable :: Cassandrax.Queryable.t(), distinct :: Keyword.t()) ::
              Cassandrax.Query.t()

  @doc """
  A query expression that enables filtering in certain Cassandra queries.

  CassandraDB doesn't allow certain queries to be executed for performance reasons, such as `where`.
  You need to set `ALLOW FILTERING` to bypass this block. More details in CassandraDB docs.

  ## Example
  ```
  query = User |> allow_filtering() |> where(:id > 1) |> where(:user_name != "alice")
  ```
  """
  @callback allow_filtering(queryable :: Cassandrax.Queryable.t(), allow_filtering :: Keyword.t()) ::
              Cassandrax.Query.t()

  @doc """
  A per partition limit expression controls the number of results return from each partition.

  Cassandra will then return only the first number of rows given in the `per_partition_limit`
  (clustered by the partition key) from that partition, regardless of how many ocurences of when may be present.
  More details in CassandraDB docs.


  ## Example

  Default `per_partition_limit` is 100.
  ```
  query = per_partition_limit(User)
  ```
  Or you can set a custom `per_partition_limit`
  ```
  query = per_partition_limit(User, 10)
  ```
  """
  @callback per_partition_limit(
              queryable :: Cassandrax.Queryable.t(),
              per_partition_limit :: integer()
            ) :: Cassandrax.Query.t()
end
