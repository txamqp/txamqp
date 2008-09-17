from distutils.core import setup
from glob import glob

setup(
    name = 'txAMQP',
    version = '0.1',
    author = 'Esteve Fernandez',
    author_email = 'esteve@fluidinfo.com',
    packages = ['txamqp',],
    package_dir = { 'txamqp': 'src/txamqp',},
)

