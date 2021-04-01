.PHONY: setup test

setup-environment:
	pip3 install virtualenv==20.0.15
	virtualenv env
	source env/bin/activate; \
	pip3 install -r requirements.txt

test:
	source env/bin/activate; \
	SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=./src python -m pytest $(target) -v
