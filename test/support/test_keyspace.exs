defmodule TestKeyspace do
  use Cassandrax.Keyspace, cluster: Cassandrax.TestConn, name: "test_keyspace"
end
