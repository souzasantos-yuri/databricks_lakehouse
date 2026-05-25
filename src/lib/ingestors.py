class Ingestor:

    def __init__(self, catalog, schema_naem, table_name, data_format):
        self.spark = spark
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.data_format = data_format
        self.set_schema()

    def set_schema(self):
        self.data_schema = utils.import_schema(self.table_name)

    def load(self, path):
        df = (self.spark
                .read
                .format(self.data_format)
                .schema(self.data_schema)
                .load(path)
        )
        return df
    
    def save(self, df):
        (df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{self.catalog}.{self.schema_name}.{self.table_name}"))
        return True
    
    def execute(self, path):
        df = self.load(path)
        return self.save(df)

class IngestorCDC(Ingestor):
    def __init__(self, catalog, schema_name, table_name, data_format, id_field, timestamp_field):
        super().__init__(catalog, schema_name, table_name, data_format)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_schema()
        self.set_delta_table()
        
    def set_delta_table(self):
        table_name = f"{self.catalog}.{self.schema_name}.{self.table_name}"
        self.delta_table = delta.DeltaTable.forName(self.spark, table_name)

    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"view_{self.table_name}"
                                         
        query = f'''
            SELECT * FROM global_temp.view_{self.table_name}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) = 1
        ''')

        df_cdc = spark.sql(query)
        
        (self.delta_table
            .alias("b")
            .merge(df_cdc.alias("d"), f"b.{self.id_field} = d.{self.id_field}")
            .whenMatchedDelete(condition = "d.OP = 'D'")
            .whenMatchedUpdateAll(condition = "d.OP = 'U'")
            .whenNotMatchedInsertAll(condition = "d.OP = 'I' OR d.OP = 'U'")
            .execute())
        
    def load(self, path):
        df = (self.spark
                .readStream
                .format("cloudFiles")
                .option("cloudFiles.format", self.data_schema)
                .schema(self.data_schema)
                .load(path))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                  .option("checkpointLocation", f"/Volumes/raw/{self.schema_name}/cdc/{self.table_name}_checkpoint/")
                  .forEachBatch(lambda df, batchID: self.upsert(df))
                  .trigger(availableNow=True))
        return stream.start()
    
class IngestorCDF(IngestorCDC):

    def __init__(self, spark, catalog, schemaname, tablename, id_field, idfield_old):
        
        super().__init__(spark=spark,
                         catalog=catalog,
                         schemaname=schemaname,
                         tablename=tablename,
                         data_format='delta',
                         id_field=id_field,
                         timestamp_field='_commit_timestamp') #delta timestamp
        
        self.idfield_old = idfield_old #old ID from bronze prior to name changes in silver
        self.set_query()
        self.checkpoint_location = f"/Volumes/raw/{schemaname}/cdc/{catalog}_{tablename}_checkpoint/"

    # Does nothing just inherits normally
    def set_schema(self):
        return
    
    # Gets generic query and sets the CDF settings
    def set_query(self):
        query = utils.import_query(f"{self.tablename}.sql")
        self.from_table = utils.extract_from(query=query)
        self.original_query = query
        self.query = utils.format_query_cdf(query, "{df}")

    # Loads data in CDF format
    def load(self):
        df = (self.spark.readStream
                   .format('delta')
                   .option("readChangeFeed", "true")
                   .table(self.from_table))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                    .option("checkpointLocation", self.checkpoint_location)
                    .foreachBatch(lambda df, batchID: self.upsert(df) )
                    .trigger(availableNow=True))
        return stream.start()
    
    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"silver_{self.tablename}")

        #Gets only changed data (ignores preimage)
        query_last = f"""
        SELECT *
        FROM global_temp.silver_{self.tablename}
        WHERE _change_type <> 'update_preimage'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.idfield_old} ORDER BY _commit_timestamp DESC) = 1
        """
        df_last = self.spark.sql(query_last)
        df_upsert = self.spark.sql(self.query, df=df_last)

        (self.deltatable
             .alias("s")
             .merge(df_upsert.alias("d"), f"s.{self.id_field} = d.{self.id_field}") 
             .whenMatchedDelete(condition = "d._change_type = 'delete'")
             .whenMatchedUpdateAll(condition = "d._change_type = 'update_postimage'")
             .whenNotMatchedInsertAll(condition = "d._change_type = 'insert' OR d._change_type = 'update_postimage'")
               .execute())

    def execute(self):
        df = self.load()
        return self.save(df)