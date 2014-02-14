#!/usr/bin/env python
from setuptools import setup

kwargs = {
    'name': 'gevent-kafka',
    'version': '0.2.2',
    'description': 'ApacheKafka bindings for gevent',
    'author': 'Johan Rydberg',
    'author_email': 'johan.rydberg@gmail.com',
    'url': 'https://github.com/edgeware/gevent-kafka',
    'packages': ['gevent_kafka'],
    'install_requires': [
        'gevent==0.13.8',
        'kazoo==1.2.1'
    ]
}

setup(**kwargs)
