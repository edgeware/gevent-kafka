#!/usr/bin/env python
from sys import version_info
from setuptools import setup, find_packages

tests_require = ['mock==1.0.1']

if version_info < (2, 7):
    tests_require.append('unittest2==0.5.1')

setup(name='gevent-kafka',
      version='0.3.0',
      description='Apache Kafka bindings for Gevent',
      author='Johan Rydberg',
      author_email='johan.rydberg@gmail.com',
      url='https://github.com/edgeware/gevent-kafka',
      packages=find_packages(),
      test_suite='gevent_kafka.test',
      install_requires=[
          'gevent==1.0.1',
          'kazoo==1.3.1'
      ],
      tests_require=tests_require)
