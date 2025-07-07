from importlib_resources import Package
from pyspark.sql import SparkSession

import pyspark_core_utils.configuration as configuration_service


class SparkApp:
    def __init__(self, app_package: Package):
        self.app_package = app_package
        #self.environment = configuration_service.get_environment()
        #self.config = configuration_service.get_configuration(self.app_package,
        #                                                      f'config/app-config-{self.environment}.yaml')
        #self.logger = configuration_service.get_logger(self.app_package, log_into_file=True)
        self.spark = self._init_spark()

    def execute(self):
        self.init()
        self.run()
        self.cleanup()

    def init(self):
        self.logger.info("Starting INIT")

    def run(self):
        self.logger.info("Starting RUN")

    def cleanup(self):
        self.logger.info("Starting CLEANUP")
        self.logger.info("Stopping Spark Session")
        self.spark.stop()

    def _init_spark(self):
        self.logger.debug("Initialising Spark Session")
        spark_conf = configuration_service.get_spark_properties(self.app_package,
                                                                f'config/spark-properties-{self.environment}.yaml')

        return SparkSession \
            .builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir","s3://is24-data-hive-warehouse/") \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()
