
from distutils.core import setup
from glob import glob

setup(
    name = 'fluidinfo-twisted-amqp',
    version = '0.1',
    author = 'Esteve Fernandez',
    author_email = 'esteve@fluidinfo.com',
    packages = ['fluidinfo', 'fluidinfo.amqp',],
    package_dir = { 'fluidinfo': 'src/fluidinfo',
        'fluidinfo.amqp': 'src/fluidinfo/amqp',
    },
)

