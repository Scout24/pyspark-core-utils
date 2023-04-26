from delta.tables import DeltaTable
import pyspark.sql as sql
import pyspark.sql.types as T
import pyspark.sql.functions as F
import re

from typing import Optional, List, Literal, Tuple, Union, Any
from .apps import SparkApp


SparkWriteModes = Literal["overwrite", "append", "ignore", "error"]


def write_partitioned_data_delta(
    self: SparkApp,
    dataframe: sql.DataFrame,
    partition_name: str,
    partition_dates_to_override: List[str],
    write_mode: SparkWriteModes,
    target_base_path: str,
):
    return (
        dataframe.write.partitionBy(partition_name)
        .format("delta")
        .option("mergeSchema", "true")
        .option("__partition_columns", partition_name)
        .option(
            "replaceWhere",
            "{} in ({})".format(
                partition_name,
                ", ".join(map(lambda x: "'{}'".format(x), partition_dates_to_override)),
            ),
        )
        .mode(write_mode)
        .save(target_base_path)
    )


def write_nonpartitioned_data_delta(
    self: SparkApp,
    dataframe: sql.DataFrame,
    write_mode: SparkWriteModes,
    target_base_path: str,
):
    return (
        dataframe.write.format("delta")
        .option("mergeSchema", "true")
        .mode(write_mode)
        .save(target_base_path)
    )


def compact_delta_table_partitions(
    self: SparkApp,
    sparkSession: sql.SparkSession,
    base_path: str,
    partition_name: str,
    dates: List[str],
    num_files: int,
):
    return (
        sparkSession.read.format("delta")
        .load(base_path)
        .where(
            f"{partition_name} in (', '.join(map(lambda x : "
            "{}"
            ".format(x), dates)))"
        )
        .repartition(num_files)
        .write.option("dataChange", "false")
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            "{} in ({})".format(
                partition_name, ", ".join(map(lambda x: "'{}'".format(x), dates))
            ),
        )
        .save(base_path)
    )


def generate_delta_table(
    self: SparkApp,
    sparkSession: sql.SparkSession,
    schema_name: str,
    table_name: str,
    s3location: str,
):
    self.spark.sql("create database if not exists {}".format(schema_name))
    qualified_table_name = f"""{schema_name}.{table_name}"""
    DeltaTable.createIfNotExists(sparkSession).tableName(qualified_table_name).location(
        s3location
    ).execute()
    print(f"Delta table {qualified_table_name} generated")


def extract_delta_info_from_path(
    self: SparkApp, paths: List[str]
) -> Tuple[str, str, List[str]]:
    path = paths[0]
    path_reg_exp = """(.*)/(.*)=(.*)"""
    try:
        match_pattern_to_path = re.match(path_reg_exp, path)
        assert match_pattern_to_path
        base_path = match_pattern_to_path.group(1)
        partition_name = match_pattern_to_path.group(2)
        dates = []
        for path in paths:
            if reg_match := re.match(path_reg_exp, path):
                dates.append(reg_match.group(3))
    except:
        raise Exception(
            f"Can not read {",".join(paths)}: base path can not be extracted"
        )
    print(base_path)
    print(partition_name)
    print(dates)
    return (base_path, partition_name, dates)


def apply_struct_schema(
    df: sql.DataFrame, schema_struct: T.StructType
) -> sql.DataFrame:
    """
    Apply a specified schema, in StructType[StructField] form, to a loaded dataframe

    Args:
        - `df`[required]: The existing dataframe
        - `schema_struct`[required]: The `StructType` containing the metadata for the required fields
    """
    df = df.selectExpr(
        *[
            f"cast({field.name} as {field.dataType.simpleString()}) {field.name}"
            for field in schema_struct
        ]
    )

    for non_nullable_field in [
        field.name for field in schema_struct if not field.nullable
    ]:
        df = df.where(F.col(non_nullable_field).isNotNull())

    return df


def read_delta_from_s3(
    self: SparkApp,
    sparkSession: sql.SparkSession,
    paths: List[str],
    schema_struct: Optional[T.StructType] = None,
) -> sql.DataFrame:
    base_path, partition_name, dates = extract_delta_info_from_path(self, paths)
    df = (
        sparkSession.read.format("delta")
        .load(base_path)
        .where(
            "{} in ({})".format(
                partition_name, ", ".join(map(lambda x: "'{}'".format(x), dates))
            )
        )
    )
    df = apply_struct_schema(df, schema_struct) if schema_struct else df
    print(df.count())
    return df


def delta_read_from_basepath(
    self: SparkApp,
    sparkSession: sql.SparkSession,
    base_path: str,
    schema_struct: Optional[T.StructType] = None,
) -> sql.DataFrame:
    df = sparkSession.read.format("delta").load(base_path)
    df = apply_struct_schema(df, schema_struct) if schema_struct else df
    return df


def read_delta_table(
    self: SparkApp,
    sparkSession: sql.SparkSession,
    schema_name: str,
    table_name: str,
    partition_name: str,
    partition_dates: List[str],
) -> sql.DataFrame:
    qualified_table_name = f"""{schema_name}.{table_name}"""
    return sparkSession.read.table(qualified_table_name).where(
        "{} in ({})".format(
            partition_name, ", ".join(map(lambda x: "'{}'".format(x), partition_dates))
        )
    )
