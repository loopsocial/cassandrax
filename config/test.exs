import Mix.Config

config :cassandrax, Cassandrax.TestConn,
  username: "cassandra",
  password: "cassandra",
  write_options: [consistency: :one],
  read_options: [consistency: :one]
