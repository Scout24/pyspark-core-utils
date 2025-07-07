from delta.tables import DeltaTable
import re
from .cluster_utils import cluster_uses_glue_metastore
from .crawler import create_crawler

def write_partitioned_data_delta(self, dataframe, partition_name, partition_dates_to_override, write_mode,
                                 target_base_path):
    dataframe \
        .write.partitionBy(partition_name) \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("__partition_columns", partition_name) \
        .option("replaceWhere", "{} in ({})".format(partition_name, ', '.join(
        map(lambda x: "'{}'".format(x), partition_dates_to_override)))) \
        .mode(write_mode) \
        .save(target_base_path)
        
    if cluster_uses_glue_metastore():
        crawler = create_crawler(self.spark)
        crawler.crawl_by_path(target_base_path)

def write_nonpartitioned_data_delta(self, dataframe, write_mode, target_base_path):
    return dataframe \
        .write.format("delta") \
        .option("mergeSchema", "true") \
        .mode(write_mode) \
        .save(target_base_path)


def compact_delta_table_partitions(self, sparkSession, base_path, partition_name, dates, num_files):
    return sparkSession.read \
        .format("delta") \
        .load(base_path) \
        .where(f"{partition_name} in (', '.join(map(lambda x : "'{}'".format(x), dates)))") \
        .repartition(num_files) \
        .write \
        .option("dataChange", "false") \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", "{} in ({})".format(partition_name, ', '.join(map(lambda x: "'{}'".format(x), dates)))) \
        .save(base_path)


def generate_delta_table(self, sparkSession, schema_name, table_name, s3location):
    if cluster_uses_glue_metastore():
        self.spark.sql(
            f"create database if not exists {schema_name} "
            f"location 's3://is24-data-hive-warehouse/{schema_name}.db'"
        )
    else:
        self.spark.sql("create database if not exists {}".format(schema_name))

    qualified_table_name = f"""{schema_name}.{table_name}"""
    DeltaTable.createIfNotExists(sparkSession) \
        .tableName(qualified_table_name) \
        .location(s3location) \
        .execute()
    print(f"Delta table {qualified_table_name} generated")
        
    if cluster_uses_glue_metastore():
        crawler = create_crawler(self.spark)
        crawler.process_table(schema_name, table_name, s3location)


def extract_delta_info_from_path(self, paths):
    path = paths[0]
    path_reg_exp = """(.*)/(.*)=(.*)"""
    try:
        match_pattern_to_path = re.match(path_reg_exp, path)
    except:
        raise Exception("Can not read {}: base path can not be extracted".format(paths.mkString(",")))

    base_path = match_pattern_to_path.group(1)
    partition_name = match_pattern_to_path.group(2)
    dates = map(lambda path: re.match(path_reg_exp, path).group(3), paths)
    print(base_path)
    print(partition_name)
    print(dates)
    return (base_path, partition_name, dates)


def read_delta_from_s3(self, sparkSession, paths):
    (base_path, partition_name, dates) = extract_delta_info_from_path(self, paths)
    df = sparkSession.read \
        .format("delta") \
        .load(base_path) \
        .where("{} in ({})".format(partition_name, ', '.join(map(lambda x: "'{}'".format(x), dates))))
    print(df.count())
    return df


def delta_read_from_basepath(self, sparkSession, base_path):
    return sparkSession.read \
        .format("delta") \
        .load(base_path)


def read_delta_table(self, sparkSession, schema_name, table_name, partition_name, partition_dates):
    qualified_table_name = f"""{schema_name}.{table_name}"""
    return sparkSession.read.table(qualified_table_name) \
        .where("{} in ({})".format(partition_name, ', '.join(map(lambda x: "'{}'".format(x), partition_dates))))
