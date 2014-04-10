all:

prepare:

check: frosted pep8

pep8:
	pep8 gevent_kafka

frosted:
	frosted -vb -r gevent_kafka

test:
	python setup.py test

build:

dist:

clean:
	git clean -fdx
