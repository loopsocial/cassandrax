import Mix.Config

config :cassandrax, Cassandrax.TestConn,
  nodes: ["127.0.0.1:9042"],
  username: "cassandra",
  password: "cassandra",
  write_options: [consistency: :one],
  read_options: [consistency: :one]
