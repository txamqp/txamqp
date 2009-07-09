#!/usr/bin/env python
try:
 # Load setuptools, to build a specific source package
 import setuptools
 setup = setuptools.setup
except ImportError:
 from distutils.core import setup

from glob import glob

setup(
    name = 'txAMQP',
    version = '0.2',
    author = 'Esteve Fernandez',
    author_email = 'esteve@fluidinfo.com',
    packages = ['txamqp', 'txamqp.contrib', 'txamqp.contrib.thrift'],
    package_dir = {
        'txamqp': 'src/txamqp',
        'txamqp.contrib': 'src/txamqp/contrib',
        'txamqp.contrib.thrift': 'src/txamqp/contrib/thrift',
        },
    url = 'https://launchpad.net/txamqp',
)
