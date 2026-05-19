import json
from pyspark.sql import types
import datetime

def table_exists(spark, catalog, database, table_name):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND table_name = '{table_name}'")
                .count()
            )
    return count == 1