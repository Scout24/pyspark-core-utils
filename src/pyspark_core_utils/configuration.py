import sys
from argparse import ArgumentParser
from logging import getLogger, Formatter, StreamHandler, FileHandler
from os import path, makedirs
from pyspark import SparkConf
from dotmap import DotMap
from yaml import full_load
from pkgutil import get_data

SEPARATOR = "="
CLI_ARGS = 'cli_args'


def get_logger(name, log_into_file, log_level='INFO', log_dir='logs'):
    logger = getLogger(name)
    logger.setLevel(log_level)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handlers = [StreamHandler()]
    if log_into_file:
        if not path.exists(log_dir):
            makedirs(log_dir)
        handlers.append(FileHandler(f'{log_dir}/{name}.log'))
    for handler in handlers:
        handler.setLevel(log_level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def get_environment():
    parser = ArgumentParser()
    parser.add_argument('--environment',
                        help='application deployment environment, eg. dev | pro',
                        type=str,
                        required=True,
                        choices=['loc', 'dev', 'stg', 'box', 'pro'])
    return parser.parse_known_args()[0].environment


def get_configuration(app_package, config_resource):
    data = get_data(app_package, config_resource)
    config = DotMap(full_load(data))
    config = _parse_cli_args(config)
    return config


def get_spark_properties(app_package, properties_path):
    data_from_config_file = full_load(get_data(app_package, properties_path))
    return SparkConf().setAll(data_from_config_file.items())


def _parse_cli_args(config):
    for arg in sys.argv[1:]:
        if _is_wrong_format(arg):
            continue
        arg_split_by_equals = arg.strip("--").split(SEPARATOR)
        name = arg_split_by_equals[0]
        value = SEPARATOR.join(arg_split_by_equals[1:])
        config[CLI_ARGS][name] = value
    return config


def _is_wrong_format(arg):
    return "=" not in arg
