defmodule CassandraxTest do
  use ExUnit.Case
  doctest Cassandrax

  test "greets the world" do
    assert Cassandrax.hello() == :world
  end
end
