#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name='gevent-kafka',
      version='0.2.2',
      description='Apache Kafka bindings for Gevent',
      author='Johan Rydberg',
      author_email='johan.rydberg@gmail.com',
      url='https://github.com/edgeware/gevent-kafka',
      packages=find_packages(),
      test_suite='gevent_kafka.test',
      install_requires=[
          'gevent==0.13.8',
          'kazoo==1.2.1'
      ],
      tests_require=[
          'mock==1.0.1'
      ])
