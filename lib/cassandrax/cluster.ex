defmodule Cassandrax.Cluster do
  use GenServer

  def init(config) do
    IO.puts(
      IO.ANSI.green_background() <>
        "*******************************************" <> IO.ANSI.reset()
    )

    IO.inspect(config, label: "CONFIG")
    {:ok, %{}}
  end

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end
end
