from logging import getLogger, INFO

import pytest
from dotmap import DotMap
from pyspark import SparkConf
from testfixtures import LogCapture

from pyspark_core_utils import configuration
from pyspark_core_utils.apps import SparkApp

DEV_ENVIRONMENT = "dev"
SPARK_APP_NAME = "spark.app.name"
STUB_LOGGER = getLogger("test-logger")
PACKAGE_NAME = "package_name"


@pytest.fixture(autouse=True)
def before_each(monkeypatch):
    monkeypatch.setattr("sys.argv", ["something.py",
                                     "--doesnt_matter=it_is_mocked"])
    monkeypatch.setattr(configuration, "get_configuration", _mock_and_assert_app_config)
    monkeypatch.setattr(configuration, "get_spark_properties", _mock_and_assert_spark_properties)
    monkeypatch.setattr(configuration, "get_logger", _mock_and_assert_logger)
    monkeypatch.setattr(configuration, "get_environment", _mock_get_environment)


def test_spark_app_loads_correctly():
    app = SparkApp(PACKAGE_NAME)

    assert app.environment == DEV_ENVIRONMENT
    assert app.logger == STUB_LOGGER
    assert app.spark.conf.get(SPARK_APP_NAME) == "just a test"
    assert app.config.hello == "world"


def test_spark_app_execute_all_steps():
    app = SparkApp(PACKAGE_NAME)

    with LogCapture() as capture:
        app.execute()

    capture.check_present(
        ('test-logger', 'INFO', 'Starting INIT'),
        ('test-logger', 'INFO', 'Starting RUN'),
        ('test-logger', 'INFO', 'Starting CLEANUP'),
    )


def _mock_and_assert_app_config(*args, **kwargs):
    assert args[0] == PACKAGE_NAME
    assert args[1] == "config/app-config-dev.yaml"

    return DotMap({"hello": "world"})


def _mock_and_assert_spark_properties(*args, **kwargs):
    assert args[0] == PACKAGE_NAME
    assert args[1] == "config/spark-properties-dev.yaml"

    return SparkConf().set(key=SPARK_APP_NAME, value="just a test")


def _mock_and_assert_logger(*args, **kwargs):
    assert args[0] == PACKAGE_NAME
    assert kwargs["log_into_file"] is True
    STUB_LOGGER.setLevel(INFO)
    return STUB_LOGGER


def _mock_get_environment(*args, **kwargs):
    return DEV_ENVIRONMENT
