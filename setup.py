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
    sys.exit('Sorry, Python < 2.7 is not supported')

setup(
    name='pypond',
    version='0.1',
    description='Python implementation of the Pond JavaScript timeseries library (https://github.com/esnet/pond).',  # pylint: disable=line-too-long
    long_description=DESCRIPTION,
    author='Monte M. Goode',
    author_email='MMGoode@lbl.gov',
    url='https://github.com/esnet/pypond',
    packages=['pypond'],
    scripts=[],
    install_requires=['pyrsistent==0.11.12', 'pytz==2016.3'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'Environment :: Web Environment',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2',
        'Programming Language :: JavaScript',
        'Topic :: Software Development :: Libraries',
    ],
)
