"""
Framework to test TimeSeries wire format interop between the python
and JS objects.

This is a bit of a special case for the tests and it's going to
presume that the test is being invoked with nosetests in either
the pypond source root, or in pypond/tests.

Additional requirement that node.js be found in the path to run the script.

Finally, it is going to be presumed that the pond JS library is installed
alongside of the pypond source like so:

dev_dir/
    pond/
    pypond/

Because the external interop script is going to do a relative path
import of pond.
"""

import os
import unittest
from subprocess import Popen, PIPE, call

from pypond.series import TimeSeries


class InteropException(Exception):
    """Custom Interop exception"""
    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class TestInterop(unittest.TestCase):
    """
    Test wire format rount trip
    """
    def setUp(self):
        """common setup."""

        self.test_dir = None
        self.interop_script = None

        # figure out where we are being run from and locate
        # the testing directory and external javascript program.
        cwd = os.getcwd()
        seg = os.path.split(cwd)

        if seg[1] == 'tests' and os.path.split(seg[0])[1] == 'pypond':
            # being run in the test directory
            self.test_dir = cwd
        elif seg[1] == 'pypond' and os.path.exists(os.path.join(cwd, 'tests')):
            # being run in the source root
            self.test_dir = os.path.join(cwd, 'tests')
        else:
            msg = 'Working dir not pypond root or pypond/tests - see docstring.'
            raise InteropException(msg)

        if os.path.exists(os.path.join(self.test_dir, 'interop_test.js')):
            self.interop_script = os.path.join(self.test_dir, 'interop_test.js')
        else:
            msg = 'Can not locate the external javascript program.'
            raise InteropException(msg)

        # can we find node to run things?
        try:
            devnull = open(os.devnull, 'w')
            call(['node', '--version'], stdout=devnull, stderr=devnull)
        except OSError:
            msg = 'Can not locate node to invoke the external interop script'
            raise InteropException(msg)

        # and finally run the external program with a test flag
        exitcode, out, _ = self._call_interop_script('ping')

        if exitcode != 0 or out.strip() != 'pong':
            msg = 'Could not execute external interop script'
            raise InteropException(msg)

    def _call_interop_script(self, arg1, wire=''):
        """call the external script."""

        args = ['node', self.interop_script, arg1, wire]

        proc = Popen(args, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        exitcode = proc.returncode

        return exitcode, out, err

    def test_event(self):
        """test the Event object"""
        pass

if __name__ == '__main__':
    unittest.main()
