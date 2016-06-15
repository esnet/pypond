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

Round-tripping the data to an external script isn't necessarily the
most optimal method of doing these tests, but it seems to be the
more reliable way to check that both code bases interoperability
is synced up in a dynamic way.
"""

import json
import os
import unittest
from subprocess import Popen, PIPE, call

from pypond.event import Event
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

    def _call_interop_script(self, case, wire='', as_json=True):
        """call the external script."""

        args = ['node', self.interop_script, case, wire]

        proc = Popen(args, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        exitcode = proc.returncode

        # Out is bytes in python3 and a string in python2. The
        # str() cast is to keep the py2 string from becoming
        # specifically unicode.
        out = str(out.decode())

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

    def _validate_wire_points(self, orig, new):
        """
        Compare the data points from the original and reconstituted
        data structures. This check will be common to all of the
        round-trip tests any more specific tests can be done
        in the specific test methods.

        This is used on tests where the original arg to TimeSeries
        is a data structure that resembles the general wire format.
        Like so:

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
        """

        # build map between the columns and the points because after
        # the data round trips, the columns might be in a different
        # order - not an error because the points will still "line up."
        col_map = dict()

        for i in enumerate(orig.get('columns')):
            col_map[i[1]] = [i[0]]

        for i in enumerate(new.get('columns')):
            if i[1] not in col_map:
                msg = 'no corresponding column for incoming col {col}'.format(col=i[1])
                raise InteropException(msg)

            col_map[i[1]].append(i[0])

        # now validate the data since column index mapping has been built.

        for i in enumerate(orig.get('points')):
            idx = i[0]

            orig_data = i[1]
            new_data = new.get('points')[idx]

            for v in list(col_map.values()):
                orig_idx = v[0]
                new_idx = v[1]
                self.assertEqual(orig_data[orig_idx], new_data[new_idx])

    def test_event_series(self):
        """test a series that contains basic event objects."""
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

        self._validate_wire_points(event_series, new_json)
        self.assertTrue(new_json.get('utc'))

        # try something a bit fancier with different types
        interface_series = dict(
            name="star-cr5:to_anl_ip-a_v4",
            description="star-cr5->anl(as683):100ge:site-ex:show:intercloud",
            device="star-cr5",
            id=169,
            interface="to_anl_ip-a_v4",
            is_ipv6=False,
            is_oscars=False,
            oscars_id=None,
            resource_uri="",
            site="anl",
            site_device="noni",
            site_interface="et-1/0/0",
            stats_type="Standard",
            title=None,
            columns=["time", "in", "out"],
            points=[
                [1400425947000, 52, 34],
                [1400425948000, 18, 13],
                [1400425949000, 26, 67],
                [1400425950000, 93, 91]
            ]
        )

        series = TimeSeries(interface_series)

        wire = self._call_interop_script('event', series.to_string())

        new_series = TimeSeries(wire)
        new_json = new_series.to_json()

        self._validate_wire_points(interface_series, new_json)

        # Now with a list of events

        event_objects = [
            Event(1429673400000, {'in': 1, 'out': 2}),
            Event(1429673460000, {'in': 3, 'out': 4}),
            Event(1429673520000, {'in': 5, 'out': 6}),
        ]

        series = TimeSeries(dict(name='events', events=event_objects))

        wire = self._call_interop_script('event', series.to_string())

        new_series = TimeSeries(wire)

        for i in enumerate(event_objects):
            self.assertTrue(Event.same(i[1], new_series.at(i[0])))

    def test_indexed_event_series(self):
        """test a series of IndexedEvent objects."""
        indexed_event_series = dict(
            name="availability",
            columns=["index", "uptime"],
            points=[
                ["2015-06", "100%"],
                ["2015-05", "92%"],
                ["2015-04", "87%"],
                ["2015-03", "99%"],
                ["2015-02", "92%"],
                ["2015-01", "100%"],
                ["2014-12", "99%"],
                ["2014-11", "91%"],
                ["2014-10", "99%"],
                ["2014-09", "95%"],
                ["2014-08", "88%"],
                ["2014-07", "100%"]
            ]
        )

        series = TimeSeries(indexed_event_series)

        wire = self._call_interop_script('indexed_event', series.to_string())

        new_series = TimeSeries(wire)
        new_json = new_series.to_json()

        self._validate_wire_points(indexed_event_series, new_json)
        self.assertTrue(new_json.get('utc'))

        # again with more involved data

        availability_series = dict(
            name="availability",
            columns=["index", "uptime", "notes", "outages"],
            points=[
                ["2015-06", 100, "", 0],
                ["2015-05", 92, "Router failure June 12", 26],
                ["2015-04", 87, "Planned downtime in April", 82],
                ["2015-03", 99, "Minor outage March 2", 4],
                ["2015-02", 92, "", 12],
                ["2015-01", 100, "", 0],
                ["2014-12", 99, "", 3],
                ["2014-11", 91, "", 14],
                ["2014-10", 99, "", 3],
                ["2014-09", 95, "", 6],
                ["2014-08", 88, "", 17],
                ["2014-09", 100, "", 2]
            ]
        )

        series = TimeSeries(availability_series)

        wire = self._call_interop_script('indexed_event', series.to_string())

        new_series = TimeSeries(wire)
        new_json = new_series.to_json()

        self._validate_wire_points(availability_series, new_json)

    def test_timerange_event_series(self):
        """test a series with a TimerangeEvent objects."""
        timerange_event_series = dict(
            name="outages",
            columns=["timerange", "title", "esnet_ticket"],
            points=[
                [[1429673400000, 1429707600000], "BOOM", "ESNET-20080101-001"],
                [[1429673400000, 1429707600000], "BAM!", "ESNET-20080101-002"],
            ],
        )

        series = TimeSeries(timerange_event_series)

        wire = self._call_interop_script('time_range_event', series.to_string())

        new_series = TimeSeries(wire)
        new_json = new_series.to_json()

        self._validate_wire_points(timerange_event_series, new_json)
        self.assertTrue(new_json.get('utc'))
        self.assertEqual(timerange_event_series.get('name'), new_json.get('name'))

    def test_event_series_with_index(self):
        """test indexed data, not a series of IndexedEvent."""
        event_series_with_index = dict(
            index="1d-625",
            name="traffic",
            columns=["time", "value", "status"],
            points=[
                [1400425947000, 522, "ok"],
                [1400425948000, 183, "ok"],
                [1400425949000, 264, "fail"],
                [1400425950000, 935, "offline"]
            ]
        )

        series = TimeSeries(event_series_with_index)

        wire = self._call_interop_script('event', series.to_string())
        print(wire)

        new_series = TimeSeries(wire)
        new_json = new_series.to_json()
        print(new_json)

        self._validate_wire_points(event_series_with_index, new_json)
        self.assertTrue(new_json.get('utc'))
        self.assertEqual(event_series_with_index.get('index'), new_json.get('index'))

if __name__ == '__main__':
    unittest.main()
