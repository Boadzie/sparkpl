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
- 🧱 **Multi-chunk safe** - Correctly handles Polars frames produced by `vstack`/`concat`
- 🔍 **Smart logging** - Structured logging with loguru
- 🎯 **Simple API** - Both functional and class-based interfaces
- 💾 **Minimal footprint** - Lightweight with essential dependencies only

## Quick Start

```python
import polars as pl
from pyspark.sql import SparkSession
from sparkpl.converter import spark_to_polars, polars_to_spark

spark = SparkSession.builder.appName("example").getOrCreate()

# Spark → Polars
spark_df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
polars_df = spark_to_polars(spark_df)

# Polars → Spark
spark_df_back = polars_to_spark(polars_df)
spark_df_back.show()
```

## Advanced Usage

### Class-based API

```python
from sparkpl.converter import DataFrameConverter

converter = DataFrameConverter(spark)

# Arrow path (default, fastest)
polars_df = converter.spark_to_polars(spark_df, use_arrow=True)

# Native fallback (no Arrow)
polars_df = converter.spark_to_polars(spark_df, use_arrow=False)

# Batched collection for very large Spark frames
polars_df = converter.spark_to_polars(large_spark_df, batch_size=100_000)

# Register the resulting Spark DataFrame as a temp view
spark_df = converter.polars_to_spark(polars_df, table_name="my_table")
```

### Error Handling

```python
from sparkpl.converter import DataFrameConverterError

try:
    polars_df = spark_to_polars(spark_df)
except DataFrameConverterError as e:
    print(f"Conversion failed: {e}")
```

### Logging

`sparkpl` uses [loguru](https://github.com/Delgan/loguru). Configure once and conversion progress is logged automatically:

```python
from loguru import logger
logger.add("sparkpl.log", rotation="10 MB", level="INFO")

polars_df = spark_to_polars(spark_df)  # progress is logged
```

## Performance & Robustness

- **Arrow path (default)** — `polars_df.to_arrow()` → `spark.createDataFrame(arrow_table)` and the reverse via `spark_df.toArrow()`. Zero-copy where possible.
- **Multi-chunk safety** — Polars frames produced by `vstack` / `concat` are multi-chunk Arrow tables. `sparkpl` calls `combine_chunks()` before handing the table to Spark so no rows are silently dropped (`createDataFrame(arrow_table)` only reads the first record batch otherwise).
- **Native fallback** — `use_arrow=False` uses `collect()` + row-dict construction. Slower, but useful as a sanity check or in environments where Arrow IPC is restricted.
- **Batched Spark→Polars** — pass `batch_size=N` to stream a large Spark frame through `limit/offset` slices and concatenate in Polars.

## Type Support

| Polars Type | Spark Type | Notes |
|-------------|------------|-------|
| `pl.Utf8` / `pl.String` | `StringType` | |
| `pl.Int8` / `Int16` / `Int32` | `IntegerType` | |
| `pl.Int64` | `LongType` | |
| `pl.UInt8` / `UInt16` | `IntegerType` | |
| `pl.UInt32` / `UInt64` | `LongType` | |
| `pl.Float32` | `FloatType` | |
| `pl.Float64` | `DoubleType` | |
| `pl.Boolean` | `BooleanType` | |
| `pl.Date` | `DateType` | |
| `pl.Datetime` | `TimestampType` | |
| `pl.Binary` | `BinaryType` | |
| `pl.Time` | `StringType` | Spark has no native Time type |
| `pl.Duration` | `LongType` | Microseconds |

## Requirements

- Python >=3.11
- polars >=1.40.1
- pyspark >=4.1.1
- pyarrow >=24.0.0
- loguru >=0.7.3

## API Reference

### Module functions

- `spark_to_polars(spark_df, spark_session=None, **kwargs) -> pl.DataFrame`
- `polars_to_spark(polars_df, spark_session=None, **kwargs) -> SparkDataFrame`

### `DataFrameConverter`

- `DataFrameConverter(spark_session=None)` — uses the active session if `None`.
- `spark_to_polars(spark_df, use_arrow=True, batch_size=None)`
- `polars_to_spark(polars_df, use_arrow=True, table_name=None)`
- `validate_conversion(original_df, converted_df, check_data=False)`
- `convert_data_types(df, type_mapping)`

### Exception

- `DataFrameConverterError` — raised for any failure during conversion or validation.

## Why No Pandas?

- **Reduced footprint** — fewer transitive dependencies.
- **Better performance** — no intermediate pandas DataFrame on the conversion path.
- **Simpler deployment** — no pandas version conflicts to debug.
- **Pure workflow** — stay within the Polars / Arrow / Spark ecosystem.

## Examples

### Convert, filter, write back

```python
spark_df = spark.createDataFrame([("Alice", 25), ("Bob", 30), ("Charlie", 35)], ["name", "age"])

polars_df = spark_to_polars(spark_df)
filtered = polars_df.filter(pl.col("age") > 28)
result_spark = polars_to_spark(filtered)
```

### Append rows with `vstack` (multi-chunk → Spark)

```python
base = pl.DataFrame({"name": ["Alice"], "age": [25]})
new  = pl.DataFrame({"name": ["Bob"],   "age": [30]})
combined = base.vstack(new)  # multi-chunk

# Both rows land in Spark — no dropped batches.
polars_to_spark(combined, use_arrow=True).show()
```

### Stream a large Spark frame in batches

```python
converter = DataFrameConverter(spark)
large_polars = converter.spark_to_polars(huge_spark_df, batch_size=50_000)
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make changes with tests
4. Commit and push
5. Open a pull request

### Development setup

```bash
git clone https://github.com/Boadzie/sparkpl.git
cd sparkpl
uv venv --python 3.11 && uv pip install -e .
```

## Changelog

### 2.0.2

- **Fix:** `polars_to_spark(..., use_arrow=True)` previously dropped rows when the input Polars DataFrame had multiple chunks (e.g., produced by `vstack` / `concat`). The Arrow table is now consolidated with `combine_chunks()` before being passed to Spark. ([#1](https://github.com/Boadzie/sparkpl/issues/1))
- Bumped dependency floors to latest tested: `polars >= 1.40.1`, `pyspark >= 4.1.1`, `pyarrow >= 24.0.0`.

### 2.0.1

- Documentation updates reflecting the pandas-free architecture.

## License

MIT License — see [LICENSE](LICENSE).

## Support

- **Issues:** [GitHub Issues](https://github.com/Boadzie/sparkpl/issues)
- **Source:** [github.com/Boadzie/sparkpl](https://github.com/Boadzie/sparkpl)
