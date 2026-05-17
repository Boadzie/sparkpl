import polars as pl
import pytest

from sparkpl.converter import (
    DataFrameConverter,
    DataFrameConverterError,
    polars_to_spark,
    spark_to_polars,
)


def test_spark_to_polars_round_trip(spark):
    spark_df = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"]
    )
    out = spark_to_polars(spark_df)
    assert out.height == 3
    assert out.columns == ["id", "name"]
    assert out.sort("id")["name"].to_list() == ["Alice", "Bob", "Charlie"]


def test_polars_to_spark_round_trip(spark):
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    out = polars_to_spark(df, spark_session=spark)
    assert out.count() == 3
    assert sorted(out.columns) == ["id", "name"]


def test_polars_to_spark_arrow_multi_chunk_regression(spark):
    """Regression test for #1.

    A multi-chunk Polars DataFrame (produced by vstack/concat) is exported as a
    multi-batch Arrow table. Before the fix, Spark's createDataFrame(arrow_table)
    only read the first record batch, silently dropping rows. With combine_chunks()
    in the converter, both rows must survive.
    """
    base = pl.DataFrame({"name": ["Scorpion"], "year": [1992]})
    new = pl.DataFrame({"name": ["Nightwolf"], "year": [1993]})
    combined = base.vstack(new)
    assert combined.n_chunks() > 1, "precondition: multi-chunk frame required"

    out = polars_to_spark(combined, spark_session=spark, use_arrow=True)
    assert out.count() == 2
    assert sorted(r["name"] for r in out.collect()) == ["Nightwolf", "Scorpion"]


def test_polars_to_spark_arrow_matches_native(spark):
    base = pl.DataFrame({"x": [1]})
    combined = base.vstack(pl.DataFrame({"x": [2]})).vstack(pl.DataFrame({"x": [3]}))
    arrow_out = polars_to_spark(combined, spark_session=spark, use_arrow=True)
    native_out = polars_to_spark(combined, spark_session=spark, use_arrow=False)
    assert arrow_out.count() == native_out.count() == 3


def test_empty_polars_to_spark(spark):
    df = pl.DataFrame(schema={"a": pl.Int64, "b": pl.Utf8})
    out = polars_to_spark(df, spark_session=spark)
    assert out.count() == 0
    assert sorted(out.columns) == ["a", "b"]


def test_empty_spark_to_polars(spark):
    spark_df = spark.createDataFrame([], "a: int, b: string")
    out = spark_to_polars(spark_df)
    assert out.height == 0
    assert sorted(out.columns) == ["a", "b"]


def test_validate_conversion_matches(spark):
    converter = DataFrameConverter(spark)
    spark_df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
    polars_df = converter.spark_to_polars(spark_df)
    assert converter.validate_conversion(spark_df, polars_df) is True


def test_validate_conversion_detects_row_mismatch(spark):
    converter = DataFrameConverter(spark)
    spark_df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
    smaller = pl.DataFrame({"id": [1], "v": ["a"]})
    assert converter.validate_conversion(spark_df, smaller) is False


def test_converter_without_active_session_raises(monkeypatch):
    from pyspark.sql import SparkSession

    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: None))
    with pytest.raises(DataFrameConverterError):
        DataFrameConverter()
