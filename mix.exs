defmodule Cassandrax.MixProject do
  use Mix.Project

  @version "0.3.1"
  @url "https://github.com/loopsocial/cassandrax"
  @maintainers ["Thiago Dias", "Doga Tuncay"]

  def project do
    [
      app: :cassandrax,
      version: @version,
      elixir: "~> 1.9",
      elixirc_paths: elixirc_paths(Mix.env()),
      source_url: @url,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),

      # Hexpm
      package: package(),
      description:
        "A Cassandra data mapping built on top of Ecto and query runner on top of Xandra.",

      # Docs
      name: "Cassandrax",
      homepage_url: "https://hexdocs.pm/cassandrax",
      docs: [main: "Cassandrax", extras: ["README.md"]]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Cassandrax, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:xandra, "~> 0.19.0"},
      {:ecto, "~> 3.12"},
      {:ex_doc, "~> 0.23", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: @maintainers,
      licenses: ["MIT"],
      links: %{github: @url},
      files: ~w(lib) ++ ~w(CHANGELOG.md LICENSE mix.exs README.md)
    ]
  end

  defp aliases do
    [
      test: "test --no-start"
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
