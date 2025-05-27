
from delta.tables import DeltaTable
from .cluster_utils import cluster_uses_glue_metastore
from .crawler import create_crawler

def readCSVData(spark, path):
    print("Reading in csv data from {}".format(path))
    return spark.read.option("header", "true") \
        .csv(path)


def readCSVCustomData(spark, path, delimeter):
    print("Reading in csv data from {}".format(path))
    return spark.read.option("header", "true") \
        .option("delimiter", delimeter) \
        .csv(path)


def readParquetData(spark, path):
    print("Reading in parquet data from {}".format(path))
    return spark.read.parquet(path)
#
def read_data_delta(spark, path):
    print("Reading in delta data from {}".format(path))
    return spark.read.format("delta").load(path)


def write_data_delta(spark, df, coalesce=1, partition_column=None, path=None, mode="overwrite"):
    print("Writing delta data to {}".format(path))
    if partition_column is not None:
        df.coalesce(coalesce).write.format("delta").mode(mode) \
            .option("header", "true") \
            .option("mergeSchema", "true")\
            .partitionBy(partition_column) \
            .save(path)
    else:
        df.coalesce(coalesce).write.format("delta").mode(mode) \
            .option("header", "true") \
            .option("mergeSchema", "true")\
            .save(path)
    
    if cluster_uses_glue_metastore():
        crawler = create_crawler(spark)
        crawler.crawl_by_path(path)



def setBucketOwnerFullAccess(spark):
    spark.conf.set("fs.s3.canned.acl", "BucketOwnerFullControl")


def saveParquet(spark, df, coalesce=1, partition_column=None, path=None, mode="overwrite"):
    print("Writing data to {}".format(path))
    if partition_column is not None:
        df.coalesce(coalesce).write.mode(mode) \
            .option("header", "true") \
            .partitionBy(partition_column) \
            .parquet(path)
    else:
        df.write.mode(mode) \
            .option("header", "true") \
            .parquet(path)


def saveCSV(spark, df,  path=None, mode="overwrite"):
    print("Writing data to {}".format(path))
    df.coalesce(1).write.mode(mode) \
        .option("header", "true") \
        .csv(path)


def changeName(spark, file_path, new_name):
    spark.sparkContext._jsc.hadoopConfiguration() \
        .set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
    URI = spark.sparkContext._gateway.jvm.java.net.URI
    Path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI("s3://is24-data-pro-lake-restricted"), spark.sparkContext._jsc.hadoopConfiguration())
    created_file_path = fs.globStatus(Path(file_path + "part*"))[0].getPath()
    fs.rename(created_file_path, Path(file_path + new_name))


def generate_delta_table(spark, schema_name, table_name, s3location):
    if cluster_uses_glue_metastore():
        spark.sql(f"create database if not exists {schema_name} location 's3://is24-data-hive-warehouse/{schema_name}.db'")
    else:
        spark.sql("create database if not exists {}".format(schema_name))
    
    qualified_table_name = f"""{schema_name}.{table_name}"""
    DeltaTable.createIfNotExists(spark) \
        .tableName(qualified_table_name) \
        .location(s3location) \
        .execute()
    print(f"Delta table {qualified_table_name} generated")
    
    if cluster_uses_glue_metastore():
        crawler = create_crawler(spark)
        crawler.process_table(schema_name, table_name, s3location)