.PHONY: setup-environment test
PYTHON=$(shell brew --prefix)/opt/python@3.7/bin/python3

install-python:
	brew install python@3.7

setup-environment:
	$(PYTHON) -m pip install virtualenv==20.0.15
	$(PYTHON) -m virtualenv env
	. env/bin/activate; \
	pip3 install -r requirements.txt; \
	pip3 install pyspark==3.3.0

test: setup-environment
	. env/bin/activate; \
	SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=./src python -m pytest $(target) -v
