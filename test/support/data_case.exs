Code.require_file("test_keyspace.exs", __DIR__)

defmodule Cassandrax.DataCase do
  use ExUnit.CaseTemplate
  alias Cassandrax.TestConn

  using do
    quote do
      setup_all do
        hostname = System.get_env("CASSANDRA_HOST") || "127.0.0.1"
        port = System.get_env("CASSANDRA_PORT") || "9042"
        config = [nodes: ["#{hostname}:#{port}"], protocol_version: :v4]

        Application.put_env(:cassandrax, :clusters, [Cassandrax.TestConn])
        Application.put_env(:cassandrax, Cassandrax.TestConn, config)

        # Use mix task instead of `Application.start/1` to respect logger level
        Mix.Task.run("app.start")
        :ok
      end
    end
  end

  setup do
    create_keyspace()
    on_exit(fn -> drop_keyspace() end)
    :ok
  end

  defp create_keyspace do
    statement = """
    CREATE KEYSPACE IF NOT EXISTS test_keyspace
    WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1}
    """

    Cassandrax.cql(TestConn, statement)
  end

  defp drop_keyspace do
    statement = """
    DROP KEYSPACE IF EXISTS test_keyspace
    """

    Cassandrax.cql(TestConn, statement)
  end
end
