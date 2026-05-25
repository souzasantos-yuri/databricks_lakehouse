import json
from pyspark.sql import types
import datetime


# Import sql queries to file
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

# Check if table already exists
def table_exists(spark, catalog, database, table_name):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND table_name = '{table_name}'")
                .count()
            )
    return count == 1


# Import schema from dictionary instead of inferring schema
def import_schema(tablename:str):
    with open(f"{tablename}.json", "r") as open_file:
        schema_json = json.load(open_file) # dictionary

    schema_df = types.StructType.fromJson(schema_json)
    return schema_df

# Manipulates strings to get the "from" path
def extract_from(query:str):
    tablename = (query.lower()
                      .split("from")[-1]
                      .strip(" ")
                      .split(" ")[0]
                      .split("\n")[0]
                      .strip(" "))
    return tablename

###################################################
### Manipulates a query string to make it generic #
### so the query writer does not need to add the ##
### metadata fields from delta tables. ############          
###################################################

# Replace table_name with generic DF "select from" to be read/written
def add_generic_from(query:str, generic_from="df"):
    tablename = extract_from(query)
    query = query.replace(tablename, generic_from)
    return query

# Add a list of fields to a query (delta meta fields)
def add_fields(query:str, fields:list):
    select = query.split("FROM")[0].strip(" \n")
    fields = ",\n".join(fields)
    from_query = f"\n\nFROM{query.split('FROM')[-1]}"
    query_new = f"{select},\n{fields}{from_query}"
    return query_new

# Final function to format query to be used in CDF
def format_query_cdf(query:str, from_table:str):
    fields = ["_change_type", "_commit_version", "_commit_timestamp"]
    query = add_fields(query=query, fields=fields)
    query = add_generic_from(query=query, generic_from=from_table)
    return query