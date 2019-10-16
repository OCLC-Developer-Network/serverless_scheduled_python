#!/usr/bin/env python

from setuptools import setup

setup(
    name = 'serverlessPythonScheduled',
    packages = ['serverless_pyScheduled'],
    version = "0.0.2",
    license='Apache2',
    description = "Serverless Python Schedule example",
    author = 'OCLC Platform Team',
    author_email = "devnet@oclc.org",
    url = 'http://oclc.org/developer/home.en.html',
    download_url = 'git@github.com:OCLC-Developer-Network/serverless__py_scheduled.git',
    install_requires = ['boto3 >=1.9', 'datetime>=4.3', 'pathlib>=1.0', 'pycallnumber>=0.1.4']
)
