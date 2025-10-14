import boto3
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from delta.tables import DeltaTable
from .cluster_utils import get_current_cluster_catalog_id

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Crawler:
    """A class to handle AWS Glue table creation and updates for Delta tables.
    
    This crawler can discover existing Glue tables by S3 path and create or update
    Glue table definitions based on Delta table schemas.
    """
    
    def __init__(self, spark: SparkSession, glue_client=None, max_retries=3, retry_delay=2, catalog_id=None):
        """Initialize the Crawler with Spark session and Glue client.
        
        Args:
            spark: SparkSession instance for reading Delta tables
            glue_client: Optional boto3 Glue client, creates default if None
            max_retries: Maximum number of retries for Glue API calls (default: 3)
            retry_delay: Delay in seconds between retries (default: 2)
            catalog_id: Glue catalog ID.
        """
        logger.info("Initializing Crawler with Spark session")
        self.spark = spark
        self.glue = glue_client or boto3.client('glue', region_name='eu-west-1')
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        if catalog_id:
            self.catalog_id = catalog_id
            logger.info(f"Using provided catalog ID: {catalog_id}")
        else:
            self.catalog_id = get_current_cluster_catalog_id()
            if self.catalog_id:
                logger.info(f"Retrieved catalog ID from cluster: {self.catalog_id}")
            else:
                logger.info("No catalog ID configured - will use default AWS account catalog")
        self.spark_to_glue_type = {
            "integer": "int",
            "long": "bigint",
            "string": "string",
            "double": "double",
            "boolean": "boolean",
            "timestamp": "timestamp"
        }
        logger.info("Crawler initialization complete")

    def _get_glue_kwargs(self, **kwargs):
        """Get Glue API kwargs with CatalogId if available.
        
        Args:
            **kwargs: Additional keyword arguments for the Glue API call
            
        Returns:
            dict: API kwargs with CatalogId included
        """
        if self.catalog_id:
            kwargs['CatalogId'] = self.catalog_id
            logger.debug(f"Adding CatalogId {self.catalog_id} to Glue API call")
        return kwargs

    def _convert_data_type(self, data_type):
        """Convert Spark data types to Glue-compatible data types.
        
        Args:
            data_type: Spark data type to convert
            
        Returns:
            str: Glue-compatible data type string
        """
        logger.debug(f"Converting Spark data type: {data_type}")
        if isinstance(data_type, ArrayType):
            element_type = self._convert_data_type(data_type.elementType)
            if isinstance(data_type.elementType, StructType):
                fields = ",".join(
                    f"{f.name}:{self._convert_data_type(f.dataType)}"
                    for f in data_type.elementType.fields
                )
                result = f"array<struct<{fields}>>"
                logger.debug(f"Converted array of struct type to: {result}")
                return result
            return f"array<{element_type}>"
        elif isinstance(data_type, StructType):
            fields = ",".join(
                f"{f.name}:{self._convert_data_type(f.dataType)}"
                for f in data_type.fields
            )
            return f"struct<{fields}>"
        else:
            return self.spark_to_glue_type.get(data_type.simpleString(), data_type.simpleString())

    def _create_glue_column(self, field):
        """Create a Glue column definition from a Spark field.
        
        Args:
            field: Spark StructField
            
        Returns:
            dict: Glue column definition with Name and Type
        """
        logger.debug(f"Creating Glue column for field: {field.name}")
        return {
            'Name': field.name,
            'Type': self._convert_data_type(field.dataType)
        }

    def _retry_boto3_call(self, func, *args, **kwargs):
        """Retry a boto3 call with simple counter-based retry logic.
        
        Args:
            func: The boto3 function to call
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            The result of the boto3 call
        """

        non_retryable_exceptions = (
            self.glue.exceptions.AlreadyExistsException,
            self.glue.exceptions.InvalidInputException,
        )
        
        last_exception = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except non_retryable_exceptions as e:
                logger.debug(f"Non-retryable error encountered: {e}")
                raise
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries:
                    logger.warning(f"Attempt {attempt}/{self.max_retries} failed: {e}. Retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"All {self.max_retries} attempts failed. Last error: {e}")
        raise last_exception

    def _ensure_database_exists(self, db_name):
        """Ensure that the Glue database exists, create it if it doesn't.
        
        Args:
            db_name: Name of the database to check/create
            
        Returns:
            bool: True if database exists or was created successfully
        """
        try:
            get_db_kwargs = self._get_glue_kwargs(Name=db_name)
            self._retry_boto3_call(self.glue.get_database, **get_db_kwargs)
            logger.info(f"Database {db_name} already exists")
            return True
        except self.glue.exceptions.EntityNotFoundException:
            logger.info(f"Database {db_name} does not exist, attempting to create it")
            try:
                create_db_kwargs = self._get_glue_kwargs(
                    DatabaseInput={
                        'Name': db_name,
                        'LocationUri': f's3://is24-data-hive-warehouse/{db_name}.db'
                    }
                )
                self._retry_boto3_call(
                    self.glue.create_database,
                    **create_db_kwargs
                )
                logger.info(f"Successfully created database {db_name} at s3://is24-data-hive-warehouse/{db_name}.db")
                return True
            except self.glue.exceptions.AlreadyExistsException:
                logger.info(f"Database {db_name} was created by another process")
                return True
        except Exception as e:
            logger.error(f"Error ensuring database {db_name} exists: {e}")
            raise

    def _create_table_input(self, table_name, path, columns, is_create=True):
        """Create table input configuration for Glue table creation/update.
        
        Args:
            table_name: Name of the table
            path: S3 path to the Delta table
            columns: List of Glue column definitions
            is_create: Whether this is for creating a new table (True) or updating (False)
            
        Returns:
            dict: Table input configuration for Glue API
        """
        logger.debug(f"Creating table input for {table_name} at {path} with {len(columns)} columns")
        
        # Base parameters that are always included
        parameters = {
            'EXTERNAL': 'true',
            'classification': 'delta',
            'delta.checkpoint.location': f'{path}/_delta_log',
            'spark.sql.sources.provider': 'delta',
            'spark.sql.partitionProvider': 'catalog'
        }
        
        if is_create:
            parameters['table_type'] = 'delta'
        
        return {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': path,
                'InputFormat': 'org.apache.hadoop.mapred.SequenceFileInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat',
                'Compressed': False,
                'NumberOfBuckets': 0,
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Parameters': {
                        'serialization.format': '1',
                        'path': path
                    }
                }
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': parameters,
        }

    def crawl_by_path(self, path):
        """Find and process existing Glue table by S3 path.
        
        Searches through all databases and tables in Glue to find a table
        with a matching S3 location, then processes it if found.
        
        Args:
            path: S3 path to search for
            
        Returns:
            str: Result message when table found and processed
            
        Raises:
            ValueError: If no existing Glue table is found for the given path
        """
        logger.info(f"Starting crawl for path: {path}")
        target_path = path.rstrip('/')

        paginator = self.glue.get_paginator('get_databases')
        db_paginate_kwargs = self._get_glue_kwargs()
        for page in paginator.paginate(**db_paginate_kwargs):
            for db in page['DatabaseList']:
                db_name = db['Name']
                table_paginator = self.glue.get_paginator('get_tables')
                table_paginate_kwargs = self._get_glue_kwargs(DatabaseName=db_name)
                for table_page in table_paginator.paginate(**table_paginate_kwargs):
                    for table in table_page['TableList']:
                        location = table.get('StorageDescriptor', {}).get('Location', '')
                        if location.rstrip('/') == target_path:
                            logger.info(f"Found table {db_name}.{table['Name']} at {path}")
                            return self.process_table(db_name, table['Name'], path)
        logger.error(f"No existing Glue table found for path {path}")
        raise ValueError(f"No existing Glue table found with location '{path}'.")

    def process_table(self, db_name, table_name, path):
        """Process a Delta table and create/update corresponding Glue table.
        
        Reads the Delta table schema and creates or updates a Glue table
        with the appropriate metadata and configuration.
        
        Args:
            db_name: Database name in Glue
            table_name: Table name in Glue
            path: S3 path to the Delta table
            
        Returns:
            str: Success message
        """
        logger.info(f"Processing table {db_name}.{table_name} at path {path}")
        
        if not path.startswith("s3://"):
            msg = f"Invalid S3 path: {path}"
            logger.error(msg)
            raise ValueError(msg)

        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            schema = delta_table.toDF().schema
            columns = [self._create_glue_column(f) for f in schema.fields]

            # Ensure database exists before creating table
            self._ensure_database_exists(db_name)

            try:
                logger.info(f"Attempting to create Glue table {db_name}.{table_name}")
                table_input = self._create_table_input(table_name, path, columns, is_create=True)
                create_table_kwargs = self._get_glue_kwargs(DatabaseName=db_name, TableInput=table_input)
                self._retry_boto3_call(self.glue.create_table, **create_table_kwargs)
                logger.info(f"Successfully created Glue table {db_name}.{table_name}")
                return f"Created Glue table {db_name}.{table_name}"
            except self.glue.exceptions.AlreadyExistsException:
                logger.info(f"Table {db_name}.{table_name} already exists, attempting update")
                update_table_input = self._create_table_input(table_name, path, columns, is_create=False)
                update_table_kwargs = self._get_glue_kwargs(DatabaseName=db_name, TableInput=update_table_input)
                self._retry_boto3_call(self.glue.update_table, **update_table_kwargs)
                logger.info(f"Successfully updated Glue table {db_name}.{table_name}")
                return f"Updated Glue table {db_name}.{table_name}"
        except Exception as e:
            logger.error(f"Failed to process table {db_name}.{table_name}: {e}")
            raise


def create_crawler(spark, catalog_id=None):
    """Create a Crawler instance with default Glue client.
    
    Args:
        spark: SparkSession instance
        catalog_id: Optional Glue catalog ID. If None, will attempt to get from cluster config
        
    Returns:
        Crawler: Configured Crawler instance with eu-west-1 Glue client
    """
    logger.info("Creating Crawler with default Glue client")
    glue_client = boto3.client("glue", region_name="eu-west-1")
    return Crawler(spark, glue_client, catalog_id=catalog_id)
