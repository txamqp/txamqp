from distutils.core import setup
from glob import glob

setup(
    name = 'txAMQP',
    version = '0.2',
    author = 'Esteve Fernandez',
    author_email = 'esteve@fluidinfo.com',
    packages = ['txamqp', 'txamqp.contrib', ],
    package_dir = {
        'txamqp': 'src/txamqp',
        'txamqp.contrib': 'src/txamqp/contrib',
        },
    url = 'https://launchpad.net/txamqp',
)
