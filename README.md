# sparkpl

> A lightweight, pandas-free Python package for seamless conversion between PySpark and Polars DataFrames.

## Installation

```bash
pip install sparkpl
```

## Features

- 🚀 **Direct Arrow conversion** - Uses native Arrow for maximum performance (Spark 4.0+)
- ⚡ **Zero pandas dependency** - Pure Polars ↔ Spark conversion
- 🔄 **Bidirectional conversion** - Seamless data exchange between frameworks
- 🛡️ **Type preservation** - Maintains data types during conversion
- 📊 **Batch processing** - Handles large datasets efficiently
- 🔍 **Smart logging** - Structured logging with loguru
- 🎯 **Simple API** - Both functional and class-based interfaces
- 💾 **Minimal footprint** - Lightweight with essential dependencies only

## Quick Start

```python
import polars as pl
from pyspark.sql import SparkSession
from sparkpl import spark_to_polars, polars_to_spark

# Initialize Spark
spark = SparkSession.builder.appName("example").getOrCreate()

# Create sample data
spark_df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

# Convert Spark → Polars
polars_df = spark_to_polars(spark_df)
print(polars_df)

# Convert Polars → Spark
spark_df_back = polars_to_spark(polars_df)
spark_df_back.show()
```

## Advanced Usage

### Class-based API

```python
from sparkpl import DataFrameConverter

converter = DataFrameConverter(spark)

# With Arrow optimization (default)
polars_df = converter.spark_to_polars(spark_df, use_arrow=True)

# Native fallback for compatibility
polars_df = converter.spark_to_polars(spark_df, use_arrow=False)

# Batch processing for large datasets
polars_df = converter.spark_to_polars(large_spark_df, batch_size=100000)

# Create temporary view
spark_df = converter.polars_to_spark(polars_df, table_name="my_table")
```

### Error Handling

```python
from sparkpl import DataFrameConverterError

try:
    polars_df = spark_to_polars(spark_df)
except DataFrameConverterError as e:
    print(f"Conversion failed: {e}")
```

### Logging Configuration

```python
from loguru import logger

# Configure structured logging
logger.add("sparkpl.log", rotation="10 MB", level="INFO")

# Conversions automatically log progress
polars_df = spark_to_polars(spark_df)  # Logs conversion metrics
```

## Performance

SparkPL automatically selects the optimal conversion method:

- **Spark 4.0+**: Direct Arrow conversion (`toArrow()` → `createDataFrame(arrow_table)`)
- **Older versions**: Native collection methods with fallback
- **Large datasets**: Automatic batching to manage memory

## Type Support

| Polars Type | Spark Type | Notes |
|-------------|------------|-------|
| `pl.Utf8` | `StringType` | |
| `pl.Int32` | `IntegerType` | |
| `pl.Int64` | `LongType` | |
| `pl.Float32` | `FloatType` | |
| `pl.Float64` | `DoubleType` | |
| `pl.Boolean` | `BooleanType` | |
| `pl.Date` | `DateType` | |
| `pl.Datetime` | `TimestampType` | |
| `pl.Binary` | `BinaryType` | |
| `pl.Time` | `StringType` | Converted to string |
| `pl.Duration` | `LongType` | Microseconds |

## Requirements

- Python >=3.8
- polars >=0.18.0
- pyspark >=3.0.0
- pyarrow >=5.0.0
- loguru >=0.6.0

## API Reference

### Functions

- `spark_to_polars(spark_df, **kwargs)` - Convert Spark DataFrame to Polars
- `polars_to_spark(polars_df, **kwargs)` - Convert Polars DataFrame to Spark

### DataFrameConverter Class

- `spark_to_polars(spark_df, use_arrow=True, batch_size=None)`
- `polars_to_spark(polars_df, use_arrow=True, table_name=None)`
- `validate_conversion(original_df, converted_df, check_data=False)`

## Why No Pandas?

SparkPL eliminates pandas dependency for:
- **Reduced footprint** - Fewer dependencies to manage
- **Better performance** - Direct conversion without intermediate steps
- **Simplified deployment** - No pandas version conflicts
- **Pure workflow** - Stay within Polars/Spark ecosystem

## Examples

### Basic Conversion

```python
# Sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
spark_df = spark.createDataFrame(data, ["name", "age"])

# Convert and process
polars_df = spark_to_polars(spark_df)
filtered = polars_df.filter(pl.col("age") > 28)
result_spark = polars_to_spark(filtered)
```

### Working with Large Data

```python
# Process large dataset in chunks
converter = DataFrameConverter(spark)
large_polars = converter.spark_to_polars(
    huge_spark_df, 
    batch_size=50000  # Process 50k rows at a time
)
```

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Make changes with tests
4. Commit: `git commit -am 'Add feature'`
5. Push: `git push origin feature/my-feature`
6. Create pull request

### Development Setup

```bash
git clone https://github.com/yourusername/sparkpl.git
cd sparkpl
pip install -e ".[dev]"
pytest tests/
```

## License

MIT License - see [LICENSE](LICENSE) file.

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/sparkpl/issues)
- **Documentation**: Coming soon
- **Community**: Discussions welcome

---

Built with ❤️ for the Python data community.