"""
SparkPL - Robust Polars-Spark DataFrame Converter Package

A comprehensive package for converting DataFrames between Polars and Apache Spark
with proper type mapping, error handling, and performance optimization.
"""

import polars as pl
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    FloatType, DoubleType, BooleanType, DateType, TimestampType,
    ArrayType, MapType, NullType, DecimalType, BinaryType
)
from typing import Optional, Dict, Any, List, Union
from loguru import logger
from datetime import datetime, date
import pyarrow as pa


class DataFrameConverterError(Exception):
    """Custom exception for DataFrame conversion errors"""
    pass


class DataFrameConverter:
    """
    A robust converter between Polars and Spark DataFrames with comprehensive
    type mapping and error handling.
    """
    
    # Type mapping dictionaries
    POLARS_TO_SPARK_TYPES = {
        pl.Utf8: StringType(),
        pl.String: StringType(),
        pl.Int8: IntegerType(),
        pl.Int16: IntegerType(),
        pl.Int32: IntegerType(),
        pl.Int64: LongType(),
        pl.UInt8: IntegerType(),
        pl.UInt16: IntegerType(),
        pl.UInt32: LongType(),
        pl.UInt64: LongType(),
        pl.Float32: FloatType(),
        pl.Float64: DoubleType(),
        pl.Boolean: BooleanType(),
        pl.Date: DateType(),
        pl.Datetime: TimestampType(),
        pl.Time: StringType(),  # Spark doesn't have native Time type
        pl.Duration: LongType(),  # Convert to microseconds
        pl.Binary: BinaryType(),
        pl.Null: NullType(),
    }
    
    SPARK_TO_POLARS_TYPES = {
        'string': pl.Utf8,
        'integer': pl.Int32,
        'long': pl.Int64,
        'float': pl.Float32,
        'double': pl.Float64,
        'boolean': pl.Boolean,
        'date': pl.Date,
        'timestamp': pl.Datetime,
        'binary': pl.Binary,
        'null': pl.Null,
    }
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the converter with optional SparkSession.
        
        Args:
            spark_session: SparkSession instance. If None, will try to get active session.
        """
        self.spark = spark_session or SparkSession.getActiveSession()
        if not self.spark:
            raise DataFrameConverterError(
                "No active SparkSession found. Please provide a SparkSession instance."
            )
    
    def spark_to_polars(
        self, 
        spark_df: SparkDataFrame,
        use_arrow: bool = True,
        batch_size: Optional[int] = None
    ) -> pl.DataFrame:
        """
        Convert Spark DataFrame to Polars DataFrame.
        
        Args:
            spark_df: Input Spark DataFrame
            use_arrow: Whether to use Arrow for conversion (faster)
            batch_size: Batch size for processing large datasets
            
        Returns:
            Polars DataFrame
            
        Raises:
            DataFrameConverterError: If conversion fails
        """
        try:
            logger.info(f"Converting Spark DataFrame to Polars (rows: {spark_df.count()}, cols: {len(spark_df.columns)})")
            
            if spark_df.count() == 0:
                logger.debug("Handling empty DataFrame conversion")
                return self._create_empty_polars_df(spark_df.schema)
            
            if use_arrow:
                logger.debug("Using Arrow conversion method")
                return self._spark_to_polars_arrow(spark_df)
            else:
                logger.debug("Using native Spark collection method")
                return self._spark_to_polars_native(spark_df, batch_size)
                
        except Exception as e:
            logger.error(f"Failed to convert Spark to Polars: {str(e)}")
            raise DataFrameConverterError(f"Failed to convert Spark to Polars: {str(e)}")
    
    def polars_to_spark(
        self, 
        polars_df: pl.DataFrame,
        use_arrow: bool = True,
        table_name: Optional[str] = None
    ) -> SparkDataFrame:
        """
        Convert Polars DataFrame to Spark DataFrame.
        
        Args:
            polars_df: Input Polars DataFrame
            use_arrow: Whether to use Arrow for conversion (faster)
            table_name: Optional table name for the resulting Spark DataFrame
            
        Returns:
            Spark DataFrame
            
        Raises:
            DataFrameConverterError: If conversion fails
        """
        try:
            logger.info(f"Converting Polars DataFrame to Spark (rows: {polars_df.height}, cols: {polars_df.width})")
            
            if polars_df.height == 0:
                logger.debug("Handling empty DataFrame conversion")
                return self._create_empty_spark_df(polars_df.schema)
            
            if use_arrow:
                logger.debug("Using Arrow conversion method")
                return self._polars_to_spark_arrow(polars_df, table_name)
            else:
                logger.debug("Using native Polars method")
                return self._polars_to_spark_native(polars_df, table_name)
                
        except Exception as e:
            logger.error(f"Failed to convert Polars to Spark: {str(e)}")
            raise DataFrameConverterError(f"Failed to convert Polars to Spark: {str(e)}")
    
    def _spark_to_polars_arrow(self, spark_df: SparkDataFrame) -> pl.DataFrame:
        """Convert using Arrow (direct method)"""
        try:
            # Use Spark's native toArrow() method (Spark 4.0+)
            try:
                arrow_table = spark_df.toArrow()
                result = pl.from_arrow(arrow_table)
                logger.success("Direct Arrow conversion completed successfully")
                return result
            except AttributeError:
                # Fallback for older Spark versions
                logger.debug("toArrow() not available, using native collection")
                return self._spark_to_polars_native(spark_df)
        except Exception as e:
            logger.error(f"Arrow conversion failed: {str(e)}")
            raise DataFrameConverterError(f"Arrow conversion failed: {str(e)}")
    
    def _spark_to_polars_native(
        self, 
        spark_df: SparkDataFrame, 
        batch_size: Optional[int] = None
    ) -> pl.DataFrame:
        """Convert using native Spark collection (fallback method)"""
        if batch_size and spark_df.count() > batch_size:
            logger.info(f"Processing large dataset in batches (batch_size: {batch_size})")
            return self._process_spark_in_batches(spark_df, batch_size)
        else:
            # Collect Spark DataFrame and convert to Polars manually
            data = spark_df.collect()
            columns = spark_df.columns
            
            # Convert Row objects to dictionaries
            dict_data = [row.asDict() for row in data]
            
            result = pl.DataFrame(dict_data)
            logger.success("Native Spark collection conversion completed successfully")
            return result
    
    def _polars_to_spark_arrow(
        self, 
        polars_df: pl.DataFrame, 
        table_name: Optional[str] = None
    ) -> SparkDataFrame:
        """Convert using Arrow (direct method)"""
        try:
            # Convert Polars to Arrow table
            arrow_table = polars_df.to_arrow()
            
            # Use Spark's createDataFrame with Arrow table (Spark 4.0+)
            try:
                spark_df = self.spark.createDataFrame(arrow_table)
            except TypeError:
                # Fallback for older Spark versions
                logger.debug("createDataFrame(arrow_table) not supported, using native method")
                return self._polars_to_spark_native(polars_df, table_name)
            
            if table_name:
                spark_df.createOrReplaceTempView(table_name)
                logger.debug(f"Created temporary view: {table_name}")
            
            logger.success("Direct Arrow conversion completed successfully")
            return spark_df
        except Exception as e:
            logger.error(f"Arrow conversion failed: {str(e)}")
            raise DataFrameConverterError(f"Arrow conversion failed: {str(e)}")
    
    def _polars_to_spark_native(
        self, 
        polars_df: pl.DataFrame, 
        table_name: Optional[str] = None
    ) -> SparkDataFrame:
        """Convert using native Polars methods"""
        # Convert Polars DataFrame to list of tuples
        data = polars_df.rows()
        columns = polars_df.columns
        
        # Create Spark DataFrame
        spark_df = self.spark.createDataFrame(data, columns)
        
        if table_name:
            spark_df.createOrReplaceTempView(table_name)
            logger.debug(f"Created temporary view: {table_name}")
        
        logger.success("Native Polars conversion completed successfully")
        return spark_df
    
    def _process_spark_in_batches(
        self, 
        spark_df: SparkDataFrame, 
        batch_size: int
    ) -> pl.DataFrame:
        """Process large Spark DataFrames in batches"""
        total_count = spark_df.count()
        num_batches = (total_count + batch_size - 1) // batch_size
        
        logger.info(f"Processing {total_count} rows in {num_batches} batches")
        
        polars_dfs = []
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_count)
            
            logger.debug(f"Processing batch {i+1}/{num_batches} (rows {start_idx}-{end_idx})")
            
            # Create batch using limit and offset
            batch_df = spark_df.limit(end_idx).offset(start_idx)
            
            # Convert batch to Polars using native methods
            batch_data = batch_df.collect()
            batch_dict_data = [row.asDict() for row in batch_data]
            batch_polars = pl.DataFrame(batch_dict_data)
            polars_dfs.append(batch_polars)
        
        result = pl.concat(polars_dfs)
        logger.success(f"Batch processing completed: {len(polars_dfs)} batches processed")
        return result
    
    def _create_empty_polars_df(self, spark_schema: StructType) -> pl.DataFrame:
        """Create empty Polars DataFrame with proper schema"""
        polars_schema = {}
        for field in spark_schema.fields:
            spark_type = type(field.dataType).__name__.lower().replace('type', '')
            polars_type = self.SPARK_TO_POLARS_TYPES.get(spark_type, pl.Utf8)
            polars_schema[field.name] = polars_type
        
        logger.debug(f"Created empty Polars DataFrame with schema: {polars_schema}")
        return pl.DataFrame(schema=polars_schema)
    
    def _create_empty_spark_df(self, polars_schema: Dict[str, pl.DataType]) -> SparkDataFrame:
        """Create empty Spark DataFrame with proper schema"""
        spark_fields = []
        for col_name, polars_type in polars_schema.items():
            spark_type = self.POLARS_TO_SPARK_TYPES.get(polars_type, StringType())
            spark_fields.append(StructField(col_name, spark_type, True))
        
        spark_schema = StructType(spark_fields)
        result = self.spark.createDataFrame([], spark_schema)
        logger.debug(f"Created empty Spark DataFrame with {len(spark_fields)} columns")
        return result
    
    def convert_data_types(
        self, 
        df: Union[pl.DataFrame, SparkDataFrame],
        type_mapping: Dict[str, Union[pl.DataType, Any]]
    ) -> Union[pl.DataFrame, SparkDataFrame]:
        """
        Convert specific columns to desired data types.
        
        Args:
            df: Input DataFrame (Polars or Spark)
            type_mapping: Dictionary mapping column names to desired types
            
        Returns:
            DataFrame with converted types
        """
        logger.debug(f"Converting data types: {type_mapping}")
        
        if isinstance(df, pl.DataFrame):
            result = df.with_columns([
                pl.col(col).cast(dtype) for col, dtype in type_mapping.items()
                if col in df.columns
            ])
            logger.success(f"Type conversion completed for {len(type_mapping)} columns")
            return result
        elif isinstance(df, SparkDataFrame):
            for col, dtype in type_mapping.items():
                if col in df.columns:
                    df = df.withColumn(col, df[col].cast(dtype))
            logger.success(f"Type conversion completed for {len(type_mapping)} columns")
            return df
        else:
            raise DataFrameConverterError("Unsupported DataFrame type")
    
    def validate_conversion(
        self, 
        original_df: Union[pl.DataFrame, SparkDataFrame],
        converted_df: Union[pl.DataFrame, SparkDataFrame],
        check_data: bool = False
    ) -> bool:
        """
        Validate that conversion was successful.
        
        Args:
            original_df: Original DataFrame
            converted_df: Converted DataFrame
            check_data: Whether to compare actual data (expensive for large datasets)
            
        Returns:
            True if validation passes
        """
        try:
            logger.debug("Starting conversion validation")
            
            # Check row counts
            if isinstance(original_df, pl.DataFrame):
                orig_count = original_df.height
            else:
                orig_count = original_df.count()
            
            if isinstance(converted_df, pl.DataFrame):
                conv_count = converted_df.height
            else:
                conv_count = converted_df.count()
            
            if orig_count != conv_count:
                logger.error(f"Row count mismatch: {orig_count} vs {conv_count}")
                return False
            
            # Check column counts
            if isinstance(original_df, pl.DataFrame):
                orig_cols = len(original_df.columns)
            else:
                orig_cols = len(original_df.columns)
            
            if isinstance(converted_df, pl.DataFrame):
                conv_cols = len(converted_df.columns)
            else:
                conv_cols = len(converted_df.columns)
            
            if orig_cols != conv_cols:
                logger.error(f"Column count mismatch: {orig_cols} vs {conv_cols}")
                return False
            
            if check_data:
                logger.debug("Performing data content validation")
                result = self._validate_data_content(original_df, converted_df)
                if result:
                    logger.success("Validation passed: data content matches")
                else:
                    logger.error("Validation failed: data content mismatch")
                return result
            
            logger.success("Validation passed: row and column counts match")
            return True
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return False
    
    def _validate_data_content(
        self, 
        original_df: Union[pl.DataFrame, SparkDataFrame],
        converted_df: Union[pl.DataFrame, SparkDataFrame]
    ) -> bool:
        """Validate actual data content (expensive operation)"""
        # Convert both to Polars for comparison
        if isinstance(original_df, SparkDataFrame):
            original_data = original_df.collect()
            original_dict_data = [row.asDict() for row in original_data]
            orig_pl = pl.DataFrame(original_dict_data)
        else:
            orig_pl = original_df
        
        if isinstance(converted_df, SparkDataFrame):
            converted_data = converted_df.collect()
            converted_dict_data = [row.asDict() for row in converted_data]
            conv_pl = pl.DataFrame(converted_dict_data)
        else:
            conv_pl = converted_df
        
        # Compare DataFrames using Polars operations
        try:
            return orig_pl.equals(conv_pl)
        except Exception:
            # If direct comparison fails, compare shapes and sample data
            return (orig_pl.shape == conv_pl.shape and 
                    orig_pl.head().equals(conv_pl.head()))


# Convenience functions
def spark_to_polars(
    spark_df: SparkDataFrame, 
    spark_session: Optional[SparkSession] = None,
    **kwargs
) -> pl.DataFrame:
    """
    Convenience function to convert Spark DataFrame to Polars.
    
    Args:
        spark_df: Input Spark DataFrame
        spark_session: Optional SparkSession instance
        **kwargs: Additional arguments passed to converter
        
    Returns:
        Polars DataFrame
    """
    converter = DataFrameConverter(spark_session)
    return converter.spark_to_polars(spark_df, **kwargs)


def polars_to_spark(
    polars_df: pl.DataFrame, 
    spark_session: Optional[SparkSession] = None,
    **kwargs
) -> SparkDataFrame:
    """
    Convenience function to convert Polars DataFrame to Spark.
    
    Args:
        polars_df: Input Polars DataFrame
        spark_session: Optional SparkSession instance
        **kwargs: Additional arguments passed to converter
        
    Returns:
        Spark DataFrame
    """
    converter = DataFrameConverter(spark_session)
    return converter.polars_to_spark(polars_df, **kwargs)


# Example usage and testing
if __name__ == "__main__":
    # Configure loguru for demonstration
    logger.remove()  # Remove default handler
    logger.add(
        "sparkpl.log",
        rotation="10 MB",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} | {message}"
    )
    logger.add(
        lambda record: print(record["message"]),
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{message}</cyan>"
    )
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SparkPL") \
        .getOrCreate()
    
    # Create sample data
    data = [
        ("Alice", 25, 85000.50, True, date(2023, 1, 15)),
        ("Bob", 30, 75000.00, False, date(2023, 2, 20)),
        ("Charlie", 35, 95000.75, True, date(2023, 3, 10))
    ]
    
    columns = ["name", "age", "salary", "is_active", "hire_date"]
    
    # Create Spark DataFrame
    spark_df = spark.createDataFrame(data, columns)
    logger.info("Created original Spark DataFrame")
    
    # Initialize converter
    converter = DataFrameConverter(spark)
    
    # Convert Spark to Polars
    polars_df = converter.spark_to_polars(spark_df)
    logger.info("Converted to Polars DataFrame")
    
    # Convert back to Spark
    spark_df_converted = converter.polars_to_spark(polars_df, table_name="employees")
    logger.info("Converted back to Spark DataFrame")
    
    # Validate conversion
    is_valid = converter.validate_conversion(spark_df, spark_df_converted, check_data=True)
    logger.info(f"Conversion validation result: {is_valid}")
    
    spark.stop()
    logger.info("Spark session stopped")