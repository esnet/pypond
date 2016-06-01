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

import json
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
        out = self._call_interop_script('ping', as_json=False)

        # the exit code is already being checked in _call_interop_script()
        if out.strip() != 'pong':
            msg = 'Could not execute external interop script'
            raise InteropException(msg)

    def _call_interop_script(self, arg1, wire='', as_json=True):
        """call the external script."""

        args = ['node', self.interop_script, arg1, wire]

        proc = Popen(args, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        exitcode = proc.returncode

        if exitcode != 0:
            msg = 'Got non-zero exit code and error: {err}'.format(err=err)
            raise InteropException(msg)

        # this is primarily for setUp or anything else that wants
        # the raw output from the script. Otherwise, try to parse
        # it as a json blob.
        if as_json is False:
            return out

        wire = None

        try:
            wire = json.loads(out)
        except ValueError:
            msg = 'could not get a valid json object from output: {out}'.format(
                out=out.strip())

        return wire

    def _validate_points(self, orig, new):
        """
        Compare the data points from the original and reconstituted
        data structures. This check will be common to all of the
        round-trip tests any more specific tests can be done
        in the specific test methods.
        """

        # build map between the columns and the points because after
        # the data round trips, the columns might be in a different
        # order - not an error because the points will still "line up."

        col_prefixes = ('time', 'index', 'timerange',)

        col_map = dict()

        for i in enumerate(orig.get('columns')):
            if i[1] in col_prefixes:
                continue
            col_map[i[1]] = [i[0]]

        for i in enumerate(new.get('columns')):
            if i[1] in col_prefixes:
                continue

            if i[1] not in col_map:
                msg = 'no corresponding column for incoming col {col}'.format(col=i[1])
                raise InteropException(msg)

            col_map[i[1]].append(i[0])

        # now validate the data since column index mapping has been built.

        for i in enumerate(orig.get('points')):
            # first, validate the time stamps
            idx = i[0]
            orig_ts = i[1][0]
            new_ts = new.get('points')[idx][0]

            self.assertEqual(orig_ts, new_ts)

            # now, validate the columns

            orig_data = i[1]
            new_data = new.get('points')[idx]

            for v in col_map.values():
                orig_idx = v[0]
                new_idx = v[1]
                self.assertEquals(orig_data[orig_idx], new_data[new_idx])

    def test_event(self):
        """test a series that contains events."""
        event_series = dict(
            name="traffic",
            columns=["time", "value", "status"],
            points=[
                [1400425947000, 52, "ok"],
                [1400425948000, 18, "ok"],
                [1400425949000, 26, "fail"],
                [1400425950000, 93, "offline"]
            ]
        )

        series = TimeSeries(event_series)

        wire = self._call_interop_script('event', series.to_string())

        new_series = TimeSeries(wire)

        new_json = new_series.to_json()

        self._validate_points(event_series, new_json)
        self.assertTrue(new_json.get('utc'))


if __name__ == '__main__':
    unittest.main()
