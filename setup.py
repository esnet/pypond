#! /usr/bin/env python

"""
Setup file for pypond distribution.
"""

import sys
from setuptools import setup

try:
    # Use pandoc to convert .md -> .rst when uploading to pypi
    import pypandoc
    DESCRIPTION = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError, OSError):
    DESCRIPTION = open('README.md').read()

if sys.version_info[0] == 2 and sys.version_info[1] < 7:
    sys.exit('Sorry, Python 2 < 2.7 is not supported')

if sys.version_info[0] == 3 and sys.version_info[1] < 3:
    sys.exit('Sorry, Python 3 < 3.3 is not supported')

setup(
    name='pypond',
    version='0.4.8',
    description='Python implementation of the Pond JavaScript timeseries library (https://github.com/esnet/pond).',  # pylint: disable=line-too-long
    long_description=DESCRIPTION,
    author='Monte M. Goode',
    author_email='MMGoode@lbl.gov',
    url='https://github.com/esnet/pypond',
    packages=['pypond', 'pypond.processor', 'pypond.io'],
    scripts=[],
    install_requires=[
        'pyrsistent==0.11.13',
        'pytz==2016.4',
        'tzlocal==1.2.2',
        'humanize==0.5.1',
        'six==1.10.0',
        # these are for read the docs builds
        'sphinxcontrib-napoleon==0.5.1',
        'recommonmark==0.4.0',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'Environment :: Web Environment',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: JavaScript',
        'Topic :: Software Development :: Libraries',
    ],
)
