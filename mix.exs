defmodule TDex.MixProject do
  use Mix.Project

  def project do
    [
      app: :tdex,
      version: "1.0.0",
      elixir: "~> 1.15",
      elixirc_paths: [:lib],
      start_permanent: Mix.env() == :prod,
      compilers: [:elixir_make] ++ Mix.compilers(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {TDex, []},
      extra_applications: [
        :logger,
        :jason,
        :skn_lib,
        :runtime_tools,
        :observer_cli,
      ] ++ extra_apps(Mix.env),
    ]
  end

  defp extra_apps(:prod), do: []
  defp extra_apps(_), do: [:observer, :wx]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:skn_lib, git: "git@github.com:skygroup2/skn_lib.git", branch: "main"},
      {:jason, "~> 1.4"},
      {:db_connection, "~> 2.7"},
      {:observer_cli, "~> 1.7"},
      {:elixir_make, "~> 0.8.4", runtime: false},
      {:benchee, "~> 1.0", only: :dev}
    ]
  end
end
