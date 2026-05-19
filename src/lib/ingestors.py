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
    
