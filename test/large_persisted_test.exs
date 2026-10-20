# SPDX-FileCopyrightText: 2026 spark contributors <https://github.com/ash-project/spark/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule Spark.LargePersistedTest do
  # Changes the `:spark` application environment.
  use ExUnit.Case, async: false

  defmodule Extension do
    @moduledoc false
    use Spark.Dsl.Extension, sections: [%Spark.Dsl.Section{name: :settings, schema: []}]
  end

  defmodule Dsl do
    @moduledoc false
    use Spark.Dsl, default_extensions: [extensions: Spark.LargePersistedTest.Extension]
  end

  setup do
    previous = Application.fetch_env(:spark, :persisted_attribute_threshold)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:spark, :persisted_attribute_threshold, value)
        :error -> Application.delete_env(:spark, :persisted_attribute_threshold)
      end
    end)
  end

  defp define(module, payload) do
    {:module, ^module, bytecode, _} =
      Module.create(
        module,
        quote do
          use Spark.LargePersistedTest.Dsl
          @persist {:payload, unquote(Macro.escape(payload))}
        end,
        Macro.Env.location(__ENV__)
      )

    {:ok, {^module, [attributes: attributes]}} = :beam_lib.chunks(bytecode, [:attributes])
    attributes
  end

  defp payload(size), do: Enum.map(1..size, &%{id: &1, name: "item #{&1}"})

  test "persisted state over the threshold is stored in the attributes chunk" do
    Application.put_env(:spark, :persisted_attribute_threshold, 1_000)
    module = Module.concat(__MODULE__, Large)
    payload = payload(200)

    attributes = define(module, payload)

    assert [%{payload: ^payload}] = Keyword.fetch!(attributes, :spark_persisted)
    assert module.persisted().payload == payload
    assert module.persisted(:payload, nil) == payload
    assert module.persisted(:missing, :default) == :default
    assert module.fetch_persisted(:payload) == {:ok, payload}
    assert module.fetch_persisted(:missing) == :error
    assert module.spark_dsl_config().persist.payload == payload

    assert Spark.Dsl.Extension.get_persisted(module, :payload) ==
             payload
  end

  test "persisted state under the threshold is compiled into persisted/0" do
    Application.put_env(:spark, :persisted_attribute_threshold, 1_000)
    module = Module.concat(__MODULE__, Small)
    payload = payload(2)

    attributes = define(module, payload)

    refute Keyword.has_key?(attributes, :spark_persisted)
    assert module.persisted().payload == payload
    assert module.fetch_persisted(:payload) == {:ok, payload}
  end

  test "the default threshold keeps ordinary persisted state in persisted/0" do
    Application.delete_env(:spark, :persisted_attribute_threshold)

    attributes = define(Spark.LargePersistedTest.Default, payload(200))

    refute Keyword.has_key?(attributes, :spark_persisted)
  end

  test "an :infinity threshold never uses the attributes chunk" do
    Application.put_env(:spark, :persisted_attribute_threshold, :infinity)

    attributes = define(Spark.LargePersistedTest.Infinity, payload(200))

    refute Keyword.has_key?(attributes, :spark_persisted)
  end

  test "a recompiled module returns its new persisted state" do
    Application.put_env(:spark, :persisted_attribute_threshold, 1_000)
    module = Module.concat(__MODULE__, Recompiled)
    first = payload(200)
    second = payload(300)

    define(module, first)
    assert module.persisted(:payload, nil) == first

    ExUnit.CaptureIO.capture_io(:stderr, fn ->
      define(module, second)
    end)

    assert module.persisted(:payload, nil) == second
  end

  test "concurrent reads return the same value" do
    Application.put_env(:spark, :persisted_attribute_threshold, 1_000)
    module = Module.concat(__MODULE__, Concurrent)
    payload = payload(200)
    define(module, payload)

    results =
      1..20
      |> Task.async_stream(fn _ -> module.persisted(:payload, nil) end)
      |> Enum.map(fn {:ok, value} -> value end)

    assert Enum.all?(results, &(&1 == payload))
  end

  test "an invalid threshold raises" do
    Application.put_env(:spark, :persisted_attribute_threshold, "1MB")

    assert_raise ArgumentError, ~r/persisted_attribute_threshold/, fn ->
      define(Spark.LargePersistedTest.Invalid, payload(2))
    end
  end
end
