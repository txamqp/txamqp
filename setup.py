setupdict= {
    'name': 'txAMQP',
    'version': '0.4',
    'author': 'Esteve Fernandez',
    'author_email': 'esteve@fluidinfo.com',
    'url': 'https://launchpad.net/txamqp',
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
    setupdict['install_requires'] = ['Twisted']
        
setup(**setupdict)
