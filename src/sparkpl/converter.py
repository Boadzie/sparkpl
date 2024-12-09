import polars as pl
from pyspark.sql import DataFrame as SparkDataFrame


class DataFrameConverter:
    @staticmethod
    def spark_to_polars(spark_df: SparkDataFrame) -> pl.DataFrame:
        # Get data and column names
        data = spark_df.collect()
        columns = spark_df.columns

        # Convert to list of dictionaries
        dict_data = [row.asDict() for row in data]

        # Create Polars DataFrame
        return pl.DataFrame(dict_data)

    @staticmethod
    def polars_to_spark(polars_df: pl.DataFrame, spark_session) -> SparkDataFrame:
        # Convert using native methods
        data = polars_df.rows()
        return spark_session.createDataFrame(data, schema=polars_df.columns)
