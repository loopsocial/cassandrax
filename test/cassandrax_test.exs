defmodule CassandraxTest do
  use ExUnit.Case

  describe "ensure_cluster_config!/2" do
    test "raises an error when config is nil" do
      assert_raise(Cassandrax.ClusterConfigError, ~r/Expected to find keyword configs/, fn ->
        Cassandrax.ensure_cluster_config!(nil, %{})
      end)
    end

    test "raises an error when config is []" do
      assert_raise(Cassandrax.ClusterConfigError, ~r/Expected to find keyword configs/, fn ->
        Cassandrax.ensure_cluster_config!([], %{})
      end)
    end
  end
end
