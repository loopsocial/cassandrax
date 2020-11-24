Code.require_file("test_keyspace.exs", __DIR__)

defmodule Cassandrax.DataCase do
  use ExUnit.CaseTemplate

  alias Cassandrax.TestConn

  using do
    quote do
      setup_all do
        start_test_connection()
        :ok
      end

      defp start_test_connection do
        child =
          Cassandrax.Supervisor.child_spec(
            Cassandrax.TestConn,
            Application.get_env(:cassandrax, Cassandrax.TestConn)
          )

        Cassandrax.start_link([child])
        await_connected(Cassandrax.TestConn, "USE system")
      end

      # We need to wait for the connection to start executing statements
      defp await_connected(cluster, statement, tries \\ 4, last_error \\ nil)

      defp await_connected(_cluster, _statement, 0, last_error),
        do:
          raise(
            "timed out waiting for connection to cassandra; connection error: #{
              inspect(last_error)
            }"
          )

      defp await_connected(cluster, statement, tries, _) do
        Process.sleep(50)

        case Cassandrax.cql(cluster, statement) do
          {:error, %Xandra.ConnectionError{} = error} ->
            await_connected(cluster, statement, tries - 1, error)

          response ->
            response
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
