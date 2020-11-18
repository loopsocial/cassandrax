Code.require_file("test_keyspace.exs", __DIR__)

defmodule Cassandrax.DataCase do
  use ExUnit.CaseTemplate

  alias Cassandrax.TestConn

  using do
    import Cassandrax.Query

    quote do
      setup_all do
        start_test_connection()
        :ok
      end

      defp start_test_connection do
        child = Cassandrax.Supervisor.child_spec(Cassandrax.TestConn, nodes: ["127.0.0.1:9043"])
        Cassandrax.start_link([child])
        await_connected(TestConn, "USE system")
      end

      # We need to wait for the connection to start executing statements
      defp await_connected(cluster, statement, tries \\ 4)

      defp await_connected(_cluster, _statement, 0),
        do: raise("exceeded maximum number of attempts")

      defp await_connected(cluster, statement, tries) do
        Process.sleep(50)

        case Cassandrax.cql(cluster, statement) do
          {:error, %Xandra.ConnectionError{}} -> await_connected(cluster, statement, tries - 1)
          response -> response
        end
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
