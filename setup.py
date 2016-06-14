#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

""" Installation script for the phewas_utils package.
"""

from setuptools import setup, find_packages

setup(
    name="phewas_utils",
    description="PheWas Utilities",
    url='https://github.com/ini-bdds/phewas_utils/',
    version="0.1.0",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'phewas_utils = phewas_utils.phewas_utils_cli:main'
        ]
    },
    requires=[
        'argparse',
        'csv',
        'os',
        'sys',
        'platform',
        'logging',
        'time',
        'datetime',
        'shutil',
        'tempfile'],
    license='Apache 2.0',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ]
)

