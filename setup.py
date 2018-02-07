setupdict= {
    'name': 'txAMQP',
    'version': '0.8.2',
    'author': 'Esteve Fernandez',
    'author_email': 'esteve@apache.org',
    'url': 'https://github.com/txamqp/txamqp',
    'description': 'Python library for communicating with AMQP peers and brokers using Twisted',
    'long_description': '''This project contains all the necessary code to connect, send and receive messages to/from an AMQP-compliant peer or broker (Qpid, OpenAMQ, RabbitMQ) using Twisted.

It also includes support for using Thrift RPC over AMQP in Twisted applications.

txAMQP is sponsored by the friendly folks at Fluidinfo (http://www.fluidinfo.com).'''
    }

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup
    setupdict['packages'] = ['txamqp', 'txamqp.contrib', 'txamqp.contrib.thrift']
    setupdict['package_dir'] = {
        'txamqp': 'src/txamqp',
        'txamqp.contrib': 'src/txamqp/contrib',
        'txamqp.contrib.thrift': 'src/txamqp/contrib/thrift',
    }
else:
    setupdict['packages'] = find_packages('src')
    setupdict['package_dir'] = { '': 'src' }
    setupdict['install_requires'] = ['Twisted', 'six']

setup(**setupdict)
