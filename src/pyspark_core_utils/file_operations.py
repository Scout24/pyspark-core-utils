import logging
from delta.tables import DeltaTable
from .cluster_utils import cluster_uses_glue_metastore
from .crawler import create_crawler

logger = logging.getLogger(__name__)


def read_csv_data(spark, path):
    """Read CSV data from the specified path."""
    print(f"Reading in csv data from {path}")
    return spark.read.option("header", "true").csv(path)


def read_csv_custom_data(spark, path, delimiter):
    """Read CSV data with custom delimiter from the specified path."""
    print(f"Reading in csv data from {path}")
    return (
        spark.read.option("header", "true")
        .option("delimiter", delimiter)
        .csv(path)
    )


def read_parquet_data(spark, path):
    """Read Parquet data from the specified path."""
    print(f"Reading in parquet data from {path}")
    return spark.read.parquet(path)


def read_data_delta(spark, path):
    """Read Delta data from the specified path."""
    print(f"Reading in delta data from {path}")
    return spark.read.format("delta").load(path)


def write_data_delta(
    spark, df, coalesce=1, partition_column=None, path=None, mode="overwrite"
):
    """Write DataFrame as Delta format to the specified path."""
    print(f"Writing delta data to {path}")
    
    writer = (
        df.coalesce(coalesce)
        .write.format("delta")
        .mode(mode)
        .option("header", "true")
        .option("mergeSchema", "true")
    )
    
    if partition_column is not None:
        writer = writer.partitionBy(partition_column)
    
    writer.save(path)

    if cluster_uses_glue_metastore():
        crawler = create_crawler(spark)
        crawler.crawl_by_path(path)


def set_bucket_owner_full_access(spark):
    """Set S3 bucket owner full access configuration."""
    spark.conf.set("fs.s3.canned.acl", "BucketOwnerFullControl")


def save_parquet(
    spark, df, coalesce=1, partition_column=None, path=None, mode="overwrite"
):
    """Save DataFrame as Parquet format to the specified path."""
    print(f"Writing data to {path}")
    
    if partition_column is not None:
        (
            df.coalesce(coalesce)
            .write.mode(mode)
            .option("header", "true")
            .partitionBy(partition_column)
            .parquet(path)
        )
    else:
        df.write.mode(mode).option("header", "true").parquet(path)


def save_csv(spark, df, path=None, mode="overwrite"):
    """Save DataFrame as CSV format to the specified path."""
    print(f"Writing data to {path}")
    df.coalesce(1).write.mode(mode).option("header", "true").csv(path)


def change_name(spark, file_path, new_name):
    """Change the name of a file in S3."""
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter"
    )
    
    URI = spark.sparkContext._gateway.jvm.java.net.URI
    Path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    
    fs = FileSystem.get(
        URI("s3://is24-data-pro-lake-restricted"),
        spark.sparkContext._jsc.hadoopConfiguration(),
    )
    
    created_file_path = fs.globStatus(Path(file_path + "part*"))[0].getPath()
    fs.rename(created_file_path, Path(file_path + new_name))


def generate_delta_table(spark, schema_name, table_name, s3_location):
    """Generate a Delta table with the specified schema, table name, and S3 location."""
    if cluster_uses_glue_metastore():
        spark.sql(
            f"create database if not exists `{schema_name}` "
            f"location 's3://is24-data-hive-warehouse/{schema_name}.db'"
        )
    else:
        spark.sql(f"create database if not exists `{schema_name}`")

    qualified_table_name = f"`{schema_name}`.`{table_name}`"
    
    logger.info(f"Creating Delta table {qualified_table_name}")
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(qualified_table_name)
        .location(s3_location)
        .execute()
    )
    

    
    descr = spark.sql(f"DESCRIBE TABLE {qualified_table_name}").collect()
    logger.info(f"Description of table {qualified_table_name}: {descr}")
    logger.info(f"Delta table {qualified_table_name} generated successfully")

    if cluster_uses_glue_metastore():
        crawler = create_crawler(spark)
        crawler.process_table(schema_name, table_name, s3_location)