# sparkpl

> A lightweight Python package for seamless conversion between PySpark and Polars DataFrames.

## Installation

```bash
pip install sparkpl
```

## Features

- Direct conversion from PySpark to Polars DataFrames
- Direct conversion from Polars to PySpark DataFrames
- No pandas dependency
- Simple API
- Preserves data types
- Minimal memory footprint

## Usage

```python
from pyspark.sql import SparkSession
from sparkpl import DataFrameConverter

# Initialize Spark
spark = SparkSession.builder.appName("example").getOrCreate()

# Create converter
converter = DataFrameConverter()

# Spark to Polars
polars_df = converter.spark_to_polars(spark_df)

# Polars to Spark
spark_df = converter.polars_to_spark(polars_df, spark)
```

## Requirements

- Python >=3.11
- polars >=0.20.0
- pyspark >=3.0.0

## License

MIT

## Contributing

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/my-feature`
3. Commit your changes: `git commit -am 'Add my feature'`
4. Push to the branch: `git push origin feature/my-feature`
5. Submit a pull request

## Support

For issues and feature requests, please use the [issue tracker](https://github.com/Boadzie/sparkpl/issues).
