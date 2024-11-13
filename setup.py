#!/usr/bin/env python

from setuptools import setup  # type: ignore
from typing import List, Dict

def read_long_description(file_path: str) -> str:
    with open(file_path) as f:
        return f.read()

long_description: str = read_long_description('README.md')

setup(
    name="pipelinewise-target-postgres",
    version="2.1.1",
    description="Singer.io target for loading data to PostgreSQL - PipelineWise compatible",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="TransferWise",
    url='https://github.com/transferwise/pipelinewise-target-postgres',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only'
    ],
    py_modules=["target_postgres"],
    install_requires=[
        'pipelinewise-singer-python==1.*',
        'psycopg2-binary==2.9.3',
        'inflection==0.3.1',
        'joblib==0.16.0'
    ],
    extras_require={
        "test": [
            'pytest==6.2.1',
            'pylint==2.6.0',
            'pytest-cov==2.10.1',
        ]
    },
    entry_points="""
        [console_scripts]
        target-postgres-beta=target_postgres:main
        target-postgres=target_postgres:main
    """,
    packages=["target_postgres"],
    package_data={},
    include_package_data=True,
)
