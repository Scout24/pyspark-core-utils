from pyspark_core_utils.configuration import get_logger, get_configuration, get_spark_properties, get_environment
from testfixtures import LogCapture
from dotmap import DotMap
import pytest
import os

CONFIGURATION_FILE = "config/test_config.yaml"
SPARK_PROPERTIES_FILE = "config/test_spark_properties.yaml"
APP_PACKAGE = "app-package"

LOGS_FOLDER_PATH = "logs_folder"
LOGS_FILE_PATH = LOGS_FOLDER_PATH + "/test-logger.log"


def test_get_environment_from_cli(monkeypatch):
    monkeypatch.setattr("sys.argv", ["test_parse_cli_args.py",
                                     "--step_module=transform",
                                     "--environment=dev"])

    environment = get_environment()

    assert environment == "dev"


def test_system_exit_when_no_environment_provided_in_cli(monkeypatch):
    monkeypatch.setattr("sys.argv", ["test_parse_cli_args.py",
                                     "--step_module=transform",
                                     "--something_else=hello"])

    with pytest.raises(SystemExit):
        get_environment()


def test_create_app_set_config_from_cli_and_config_file(monkeypatch):
    monkeypatch.setattr("sys.argv", ["test_parse_cli_args.py",
                                     "--step_module=transform",
                                     "--environment=dev",
                                     "--input_path=some_path/folder/date=2020-01-01",
                                     "--start_date=2020-12-01"])

    config = get_configuration(__name__, CONFIGURATION_FILE)

    expected_config = {
        "name": "this-is-a-test",
        "something": DotMap(nested=DotMap(here="Hello")),
        "cli_args": {"step_module": "transform",
                     "start_date": "2020-12-01",
                     "input_path": "some_path/folder/date=2020-01-01",
                     "environment": "dev"}
    }

    assert config == expected_config


def test_parse_args_ignore_wrong_formats(monkeypatch):
    monkeypatch.setattr("sys.argv", ["test_parse_cli_args.py",
                                     "--this_arg is_ignored",
                                     "--this_arg_is_empty=",
                                     "--complex_input_path=some_path/folder/date=2020-01-01/complex=yes",
                                     "--start_date=2020-12-01"])

    config = get_configuration(__name__, CONFIGURATION_FILE)

    assert config.cli_args == {"start_date": "2020-12-01",
                               "complex_input_path": "some_path/folder/date=2020-01-01/complex=yes",
                               "this_arg_is_empty": ""}


def test_get_spark_properties_return_spark_configuration(monkeypatch):
    spark_configuration = get_spark_properties(__name__, SPARK_PROPERTIES_FILE)

    assert spark_configuration.get("spark.master") == "yarn"
    assert spark_configuration.get("spark.app.name") == "template_spark_app_name"


def test_logger_write_logs_in_file():
    _setup_logger_folder()

    logger = get_logger("test-logger", True, "INFO", "logs_folder")
    logger.info("Hello World")

    assert logger.name == "test-logger"
    with open(LOGS_FILE_PATH, "r") as log_file:
        content = log_file.read()
        assert "INFO - Hello World" in content


def test_logger_write_logs_in_cli_only():
    _setup_logger_folder()

    logger = get_logger("test-logger", False, "INFO", "logs_folder")
    with LogCapture() as capture:
        logger.info('hello')

    capture.check(
        ('test-logger', 'INFO', 'hello'),
    )
    assert os.path.exists(LOGS_FILE_PATH) is False


def _setup_logger_folder():
    if os.path.isfile(LOGS_FILE_PATH):
        os.remove(LOGS_FILE_PATH)
        os.rmdir(LOGS_FOLDER_PATH)
