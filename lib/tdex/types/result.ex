defmodule TDex.Result do
  @moduledoc """
  `TDex.Result` parse raw result to struct
  """
  defstruct [:precision, :affected_rows, :rows]

end
