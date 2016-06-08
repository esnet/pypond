#!/usr/bin/env python

"""
Utility script to rebuild all of the documentation sources.

- Rebuild the API doc index when new modules are added.

- Take the root README.md file, convert it into RST (ugh) and jam the
  resulting markup into docs/source/index.rst so that shows up on
  the hosted API docs.
"""

import os
import subprocess

import pypandoc


def main():
    """render the docs"""

    # make sure we're in the right place
    cwd = os.getcwd()
    seg = os.path.split(cwd)

    if seg[1] != 'docs':
        print 'must be run in pypond/docs as ./rebuild_docs.py'
        return -1

    # regenerate the module api index
    subprocess.call(['sphinx-apidoc', '-f', '-o', 'source', '../pypond/'])

    # generate a new index including the README.md
    readme = pypandoc.convert('../README.md', 'rst')
    print readme

if __name__ == '__main__':
    main()
