defmodule TDex.Result do
  @moduledoc """
  `TDex.Result` parse raw result to struct
  """
  defstruct [:code, :header, :data, message: nil]

end
