import os
from setuptools import setup

def fread(filename):
  return open(os.path.join(os.path.dirname(__file__), filename)).read()

def get_version():
  return fread('VERSION')

setup(
    name='kurator',
    version=get_version(),
    author='Tom Piscitell',
    author_email='tpiscite@cisco.com',
    description='Manage your Kafka clusters better',
    long_description=fread('README.md'),
    install_requires = [
      'kafka-python==0.9.2',
      'kazoo==2.0'
      ],
    entry_points = {
      "console_scripts" : [ 'kurator = kurator.kurator_cli:main' ],
      },
    packages = [ 'kurator' ]
    )

