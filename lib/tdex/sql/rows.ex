defmodule Tdex.Sql.Rows do
  alias Tdex.{Wrapper, Binary}

  def read_row(res, fieldNames, data) do
    { :ok, numOfRows, bin } = Wrapper.taos_fetch_raw_block(res)
    if numOfRows == 0 do
      {:ok, List.flatten(data)}
    else
      padding = <<0::size(128)>>
      dataBlock = <<padding::binary, bin::binary>>
      result = Binary.parse_block(dataBlock, fieldNames)
      read_row(res, fieldNames, [result|data])
    end
  end
end