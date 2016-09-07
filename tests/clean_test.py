"""
Tests for sanitizing, filling and renaming data.
"""

import copy
import datetime
import unittest
import warnings

from pypond.collection import Collection
from pypond.event import Event
from pypond.exceptions import ProcessorException, ProcessorWarning, TimeSeriesException
from pypond.indexed_event import IndexedEvent
from pypond.pipeline import Pipeline
from pypond.pipeline_in import Stream
from pypond.pipeline_out import CollectionOut
from pypond.processor import Filler
from pypond.series import TimeSeries
from pypond.timerange_event import TimeRangeEvent
from pypond.util import aware_utcnow

# global variables for the callbacks to write to.
# they are alwasy reset to None by setUp()

RESULT = None

EVENT_LIST = [
    Event(1429673400000, {'in': 1, 'out': 2}),
    Event(1429673460000, {'in': 3, 'out': 4}),
    Event(1429673520000, {'in': 5, 'out': 6}),
]

TICKET_RANGE = dict(
    name="outages",
    columns=["timerange", "title", "esnet_ticket"],
    points=[
        [[1429673400000, 1429707600000], "BOOM", "ESNET-20080101-001"],
        [[1429673400000, 1429707600000], "BAM!", "ESNET-20080101-002"],
    ],
)

AVAILABILITY_DATA = dict(
    name="availability",
    columns=["index", "uptime"],
    points=[
        ["2014-07", "100%"],
        ["2014-08", "88%"],
        ["2014-09", "95%"],
        ["2014-10", "99%"],
        ["2014-11", "91%"],
        ["2014-12", "99%"],
        ["2015-01", "100%"],
        ["2015-02", "92%"],
        ["2015-03", "99%"],
        ["2015-04", "87%"],
        ["2015-05", "92%"],
        ["2015-06", "100%"],
    ]
)


class CleanBase(unittest.TestCase):

    def setUp(self):
        """set up for all tests."""
        # canned collection
        self._canned_collection = Collection(EVENT_LIST)
        # canned series objects
        self._canned_event_series = TimeSeries(
            dict(name='collection', collection=self._canned_collection))

        global RESULTS
        RESULTS = None


class TestRenameFill(CleanBase):
    """
    A set of test for the second gen methods to manipulate timeseries
    and events.
    """

    def test_rename(self):
        """Test the renamer facility."""

        # rename an Event series

        ts = copy.deepcopy(self._canned_event_series)

        renamed = ts.rename_columns({'in': 'new_in', 'out': 'new_out'})

        self.assertEqual(
            renamed.at(0).get('new_in'),
            self._canned_event_series.at(0).get('in')
        )
        self.assertEqual(
            renamed.at(0).get('new_out'),
            self._canned_event_series.at(0).get('out')
        )

        self.assertEqual(
            renamed.at(1).get('new_in'),
            self._canned_event_series.at(1).get('in')
        )
        self.assertEqual(
            renamed.at(1).get('new_out'),
            self._canned_event_series.at(1).get('out')
        )

        self.assertEqual(
            renamed.at(2).get('new_in'),
            self._canned_event_series.at(2).get('in')
        )

        self.assertEqual(
            renamed.at(2).get('new_out'),
            self._canned_event_series.at(2).get('out')
        )

        # rename a TimeRangeEvent series

        ts = TimeSeries(TICKET_RANGE)

        renamed = ts.rename_columns({'title': 'event', 'esnet_ticket': 'ticket'})

        self.assertEqual(renamed.at(0).get('event'), ts.at(0).get('title'))
        self.assertEqual(renamed.at(0).get('ticket'), ts.at(0).get('esnet_ticket'))

        self.assertEqual(renamed.at(1).get('event'), ts.at(1).get('title'))
        self.assertEqual(renamed.at(1).get('ticket'), ts.at(1).get('esnet_ticket'))

        self.assertEqual(renamed.at(0).timestamp(), ts.at(0).timestamp())
        self.assertEqual(renamed.at(1).timestamp(), ts.at(1).timestamp())

        # rename and IndexedEvent series

        ts = TimeSeries(AVAILABILITY_DATA)

        renamed = ts.rename_columns(dict(uptime='available'))
        self.assertEqual(renamed.at(0).get('available'), ts.at(0).get('uptime'))
        self.assertEqual(renamed.at(2).get('available'), ts.at(2).get('uptime'))
        self.assertEqual(renamed.at(4).get('available'), ts.at(4).get('uptime'))
        self.assertEqual(renamed.at(6).get('available'), ts.at(6).get('uptime'))

        self.assertEqual(renamed.at(0).timestamp(), ts.at(0).timestamp())
        self.assertEqual(renamed.at(1).timestamp(), ts.at(1).timestamp())
        self.assertEqual(renamed.at(2).timestamp(), ts.at(2).timestamp())

    def test_bad_args(self):
        """Trigger error states for coverage."""

        simple_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000, {'in': 1, 'out': None, 'drop': None}],
                [1400425948000, {'in': None, 'out': 4, 'drop': None}],
                [1400425949000, {'in': None, 'out': None, 'drop': 13}],
                [1400425950000, {'in': None, 'out': None, 'drop': 14}],
                [1400425960000, {'in': 9, 'out': 8, 'drop': None}],
                [1400425970000, {'in': 11, 'out': 10, 'drop': 16}],
            ]
        )

        ts = TimeSeries(simple_missing_data)

        # bad ctor arg
        with self.assertRaises(ProcessorException):
            f = Filler(dict())

        # invalid method
        with self.assertRaises(TimeSeriesException):
            ts.fill(method='bogus')

        # limit not int
        with self.assertRaises(ProcessorException):
            ts.fill(fill_limit='z')

        # direct access to filler via pipeline needs to take a single path
        with self.assertRaises(ProcessorException):
            pip = Pipeline()
            pip.fill(method='linear', field_spec=['direction.in', 'direction.out'])

        # invalid method
        with self.assertRaises(ProcessorException):
            pip = Pipeline()
            pip.fill(method='bogus')

        # catch bad path at various points
        with warnings.catch_warnings(record=True) as wrn:
            ts.fill(field_spec='bad.path')
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

        with warnings.catch_warnings(record=True) as wrn:
            ts.fill(field_spec='bad.path', method='linear')
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

        with warnings.catch_warnings(record=True) as wrn:
            ts.fill(field_spec='direction.bogus')
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

        # trigger warnings about non-numeric values in linear.

        with warnings.catch_warnings(record=True) as wrn:
            simple_missing_data = dict(
                name="traffic",
                columns=["time", "direction"],
                points=[
                    [1400425947000, {'in': 1, 'out': None}],
                    [1400425948000, {'in': 'non_numeric', 'out': 4}],
                    [1400425949000, {'in': 5, 'out': None}],
                ]
            )

            ts = TimeSeries(simple_missing_data)

            ts.fill(field_spec='direction.in', method='linear')

            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

        # empty series for coverage caught a bug
        empty = TimeSeries(dict(
            name="Sensor values",
            columns=["time", "temperature"],
            points=[
            ]
        ))

        self.assertEqual(empty.fill(field_spec='temperature').size(), 0)

    def test_zero_fill(self):
        """test using the filler to fill missing values with zero."""

        simple_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000, {'in': 1, 'out': None}],
                [1400425948000, {'in': None, 'out': 4}],
                [1400425949000, {'in': 5, 'out': None}],
                [1400425950000, {'in': None, 'out': 8}],
                [1400425960000, {'in': 9, 'out': None}],
                [1400425970000, {'in': None, 'out': 12}],
            ]
        )

        ts = TimeSeries(simple_missing_data)

        # fill all invalid values

        new_ts = ts.fill(field_spec=['direction.in', 'direction.out'])

        self.assertEqual(new_ts.size(), 6)

        self.assertEqual(new_ts.at(0).get('direction.out'), 0)
        self.assertEqual(new_ts.at(2).get('direction.out'), 0)

        self.assertEqual(new_ts.at(1).get('direction.in'), 0)

        # fill one column

        new_ts = ts.fill(field_spec='direction.in')

        self.assertEqual(new_ts.size(), 6)

        self.assertEqual(new_ts.at(1).get('direction.in'), 0)
        self.assertEqual(new_ts.at(3).get('direction.in'), 0)

        self.assertIsNone(new_ts.at(0).get('direction.out'))
        self.assertIsNone(new_ts.at(2).get('direction.out'))

    def test_complex_zero_fill(self):
        """make sure more complex nested paths work OK"""

        complex_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000,
                    {'in': {'tcp': 1, 'udp': 3}, 'out': {'tcp': 2, 'udp': 3}}],
                [1400425948000,
                    {'in': {'tcp': 3, 'udp': None}, 'out': {'tcp': 4, 'udp': 3}}],
                [1400425949000,
                    {'in': {'tcp': 5, 'udp': None}, 'out': {'tcp': None, 'udp': 3}}],
                [1400425950000,
                    {'in': {'tcp': 7, 'udp': None}, 'out': {'tcp': None, 'udp': 3}}],
                [1400425960000,
                    {'in': {'tcp': 9, 'udp': 4}, 'out': {'tcp': 6, 'udp': 3}}],
                [1400425970000,
                    {'in': {'tcp': 11, 'udp': 5}, 'out': {'tcp': 8, 'udp': 3}}],
            ]
        )

        ts = TimeSeries(complex_missing_data)

        # zero fill everything

        new_ts = ts.fill(field_spec=['direction.out.tcp', 'direction.in.udp'])

        self.assertEqual(new_ts.at(0).get('direction.in.udp'), 3)
        self.assertEqual(new_ts.at(1).get('direction.in.udp'), 0)  # fill
        self.assertEqual(new_ts.at(2).get('direction.in.udp'), 0)  # fill
        self.assertEqual(new_ts.at(3).get('direction.in.udp'), 0)  # fill
        self.assertEqual(new_ts.at(4).get('direction.in.udp'), 4)
        self.assertEqual(new_ts.at(5).get('direction.in.udp'), 5)

        self.assertEqual(new_ts.at(0).get('direction.out.tcp'), 2)
        self.assertEqual(new_ts.at(1).get('direction.out.tcp'), 4)
        self.assertEqual(new_ts.at(2).get('direction.out.tcp'), 0)  # fill
        self.assertEqual(new_ts.at(3).get('direction.out.tcp'), 0)  # fill
        self.assertEqual(new_ts.at(4).get('direction.out.tcp'), 6)
        self.assertEqual(new_ts.at(5).get('direction.out.tcp'), 8)

        # do it again, but only fill the out.tcp

        new_ts = ts.fill(field_spec=['direction.out.tcp'])

        self.assertEqual(new_ts.at(0).get('direction.out.tcp'), 2)
        self.assertEqual(new_ts.at(1).get('direction.out.tcp'), 4)
        self.assertEqual(new_ts.at(2).get('direction.out.tcp'), 0)  # fill
        self.assertEqual(new_ts.at(3).get('direction.out.tcp'), 0)  # fill
        self.assertEqual(new_ts.at(4).get('direction.out.tcp'), 6)
        self.assertEqual(new_ts.at(5).get('direction.out.tcp'), 8)

        self.assertEqual(new_ts.at(0).get('direction.in.udp'), 3)
        self.assertEqual(new_ts.at(1).get('direction.in.udp'), None)  # no fill
        self.assertEqual(new_ts.at(2).get('direction.in.udp'), None)  # no fill
        self.assertEqual(new_ts.at(3).get('direction.in.udp'), None)  # no fill
        self.assertEqual(new_ts.at(4).get('direction.in.udp'), 4)
        self.assertEqual(new_ts.at(5).get('direction.in.udp'), 5)

    def test_linear(self):
        """Test linear interpolation filling returned by to_keyed_collections()."""

        simple_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000, {'in': 1, 'out': 2}],
                [1400425948000, {'in': None, 'out': None}],
                [1400425949000, {'in': None, 'out': None}],
                [1400425950000, {'in': 3, 'out': None}],
                [1400425960000, {'in': None, 'out': None}],
                [1400425970000, {'in': 5, 'out': 12}],
                [1400425980000, {'in': 6, 'out': 13}],
            ]
        )

        ts = TimeSeries(simple_missing_data)

        new_ts = ts.fill(field_spec=['direction.in', 'direction.out'],
                         method='linear')

        self.assertEqual(new_ts.size(), 7)

        self.assertEqual(new_ts.at(0).get('direction.in'), 1)
        self.assertEqual(new_ts.at(1).get('direction.in'), 1.6666666666666665)  # filled
        self.assertEqual(new_ts.at(2).get('direction.in'), 2.333333333333333)  # filled
        self.assertEqual(new_ts.at(3).get('direction.in'), 3)
        self.assertEqual(new_ts.at(4).get('direction.in'), 4.0)  # filled
        self.assertEqual(new_ts.at(5).get('direction.in'), 5)

        self.assertEqual(new_ts.at(0).get('direction.out'), 2)
        self.assertEqual(new_ts.at(1).get('direction.out'), 2.4347826086956523)  # filled
        self.assertEqual(new_ts.at(2).get('direction.out'), 2.8695652173913047)  # filled
        self.assertEqual(new_ts.at(3).get('direction.out'), 3.304347826086957)  # filled
        self.assertEqual(new_ts.at(4).get('direction.out'), 7.6521739130434785)  # filled
        self.assertEqual(new_ts.at(5).get('direction.out'), 12)

    def test_linear_list(self):
        """Test linear interpolation returned as an event list."""

        simple_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000, {'in': 1, 'out': None}],
                [1400425948000, {'in': None, 'out': None}],
                [1400425949000, {'in': None, 'out': None}],
                [1400425950000, {'in': 3, 'out': 8}],
                [1400425960000, {'in': None, 'out': None}],
                [1400425970000, {'in': 5, 'out': 12}],
                [1400425980000, {'in': 6, 'out': 13}],
            ]
        )

        ts = TimeSeries(simple_missing_data)

        # also test chaining multiple fillers together. in this series,
        # field_spec=['direction.in', 'direction.out'] would not start
        # filling until the 4th point so points 2 and 3 of direction.in
        # would not be filled. A chain like this will ensure both
        # columns will be fully filled.

        elist = (
            Pipeline()
            .from_source(ts)
            .fill(field_spec='direction.in', method='linear')
            .fill(field_spec='direction.out', method='linear')
            .to_event_list()
        )

        self.assertEqual(len(elist), len(simple_missing_data.get('points')))

        self.assertEqual(elist[0].get('direction.in'), 1)
        self.assertEqual(elist[1].get('direction.in'), 1.6666666666666665)  # filled
        self.assertEqual(elist[2].get('direction.in'), 2.333333333333333)  # filled
        self.assertEqual(elist[3].get('direction.in'), 3)
        self.assertEqual(elist[4].get('direction.in'), 4.0)  # filled
        self.assertEqual(elist[5].get('direction.in'), 5)

        self.assertEqual(elist[0].get('direction.out'), None)  # can't fill
        self.assertEqual(elist[1].get('direction.out'), None)  # can't fill
        self.assertEqual(elist[2].get('direction.out'), None)  # can't fill
        self.assertEqual(elist[3].get('direction.out'), 8)
        self.assertEqual(elist[4].get('direction.out'), 10.0)  # filled
        self.assertEqual(elist[5].get('direction.out'), 12)

    def test_assymetric_linear_fill(self):
        """Test new chained/assymetric linear default fill in TimeSeries."""

        simple_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000, {'in': 1, 'out': None}],
                [1400425948000, {'in': None, 'out': None}],
                [1400425949000, {'in': None, 'out': None}],
                [1400425950000, {'in': 3, 'out': 8}],
                [1400425960000, {'in': None, 'out': None}],
                [1400425970000, {'in': 5, 'out': 12}],
                [1400425980000, {'in': 6, 'out': 13}],
            ]
        )

        ts = TimeSeries(simple_missing_data)

        new_ts = ts.fill(method='linear', field_spec=['direction.in', 'direction.out'])

        self.assertEqual(new_ts.at(0).get('direction.in'), 1)
        self.assertEqual(new_ts.at(1).get('direction.in'), 1.6666666666666665)  # filled
        self.assertEqual(new_ts.at(2).get('direction.in'), 2.333333333333333)  # filled
        self.assertEqual(new_ts.at(3).get('direction.in'), 3)
        self.assertEqual(new_ts.at(4).get('direction.in'), 4.0)  # filled
        self.assertEqual(new_ts.at(5).get('direction.in'), 5)

        self.assertEqual(new_ts.at(0).get('direction.out'), None)  # can't fill
        self.assertEqual(new_ts.at(1).get('direction.out'), None)  # can't fill
        self.assertEqual(new_ts.at(2).get('direction.out'), None)  # can't fill
        self.assertEqual(new_ts.at(3).get('direction.out'), 8)
        self.assertEqual(new_ts.at(4).get('direction.out'), 10.0)  # filled
        self.assertEqual(new_ts.at(5).get('direction.out'), 12)

    def test_linear_stream(self):
        """Test streaming on linear fill"""

        def cback(collection, window_key, group_by):
            """the callback"""
            global RESULTS  # pylint: disable=global-statement
            RESULTS = collection

        events = [
            Event(1400425947000, 1),
            Event(1400425948000, 2),
            Event(1400425949000, dict(value=None)),
            Event(1400425950000, dict(value=None)),
            Event(1400425951000, dict(value=None)),
            Event(1400425952000, 5),
            Event(1400425953000, 6),
            Event(1400425954000, 7),
        ]

        stream = Stream()

        (
            Pipeline()
            .from_source(stream)
            .fill(method='linear', field_spec='value')
            .to(CollectionOut, cback)
        )

        for i in events:
            stream.add_event(i)

        self.assertEqual(RESULTS.size(), len(events))

        self.assertEqual(RESULTS.at(0).get(), 1)
        self.assertEqual(RESULTS.at(1).get(), 2)
        self.assertEqual(RESULTS.at(2).get(), 2.75)  # filled
        self.assertEqual(RESULTS.at(3).get(), 3.5)  # filled
        self.assertEqual(RESULTS.at(4).get(), 4.25)  # filled
        self.assertEqual(RESULTS.at(5).get(), 5)
        self.assertEqual(RESULTS.at(6).get(), 6)
        self.assertEqual(RESULTS.at(7).get(), 7)

    def test_linear_stream_limit(self):
        """Test streaming on linear fill with limiter"""

        # Sets up a state where we stop seeing a good data
        # on a linear fill. In this case the Taker is used to
        # not only limit the number of results, but also to
        # make sure any cached events get emitted.

        def cback(collection, window_key, group_by):
            """the callback"""
            global RESULTS  # pylint: disable=global-statement
            RESULTS = collection

        events = [
            Event(1400425947000, 1),
            Event(1400425948000, 2),
            Event(1400425949000, dict(value=None)),
            Event(1400425950000, 3),
            Event(1400425951000, dict(value=None)),
            Event(1400425952000, dict(value=None)),
            Event(1400425953000, dict(value=None)),
            Event(1400425954000, dict(value=None)),
        ]

        # error state first - the last 4 events won't be emitted.

        stream = Stream()

        (
            Pipeline()
            .from_source(stream)
            .fill(method='linear', field_spec='value')
            .to(CollectionOut, cback)
        )

        for i in events:
            stream.add_event(i)

        self.assertEqual(RESULTS.size(), 4)

        # shut it down and check again.
        stream.stop()

        # events "stuck" in the cache have been emitted
        self.assertEqual(RESULTS.size(), 8)

        # now use the Taker to make sure any cached events get
        # emitted as well - setting the fill_limit to 3 here
        # will make it so on the 7th event (after 3 have been
        # cached) those will be emitted, and then the 8th event
        # will be emitted because the state has been reset to
        # "have not seen a valid value yet" which means that
        # invalid events will be emitted and not cached.

        stream = Stream()

        (
            Pipeline()
            .from_source(stream)
            .fill(method='linear', fill_limit=3, field_spec='value')
            .to(CollectionOut, cback)
        )

        for i in events:
            stream.add_event(i)

        self.assertEqual(RESULTS.size(), 8)

    def test_pad_and_zero_limiting(self):
        """test the limiting on pad and zero options."""
        simple_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000, {'in': 1, 'out': None}],
                [1400425948000, {'in': None, 'out': None}],
                [1400425949000, {'in': None, 'out': None}],
                [1400425950000, {'in': 3, 'out': 8}],
                [1400425960000, {'in': None, 'out': None}],
                [1400425970000, {'in': None, 'out': 12}],
                [1400425980000, {'in': None, 'out': 13}],
                [1400425990000, {'in': 7, 'out': None}],
                [1400426000000, {'in': 8, 'out': None}],
                [1400426010000, {'in': 9, 'out': None}],
                [1400426020000, {'in': 10, 'out': None}],
            ]
        )

        ts = TimeSeries(simple_missing_data)

        # verify fill limit for zero fill
        zero_ts = ts.fill(method='zero', fill_limit=2,
                          field_spec=['direction.in', 'direction.out'])

        self.assertEqual(zero_ts.at(0).get('direction.in'), 1)
        self.assertEqual(zero_ts.at(1).get('direction.in'), 0)  # fill
        self.assertEqual(zero_ts.at(2).get('direction.in'), 0)  # fill
        self.assertEqual(zero_ts.at(3).get('direction.in'), 3)
        self.assertEqual(zero_ts.at(4).get('direction.in'), 0)  # fill
        self.assertEqual(zero_ts.at(5).get('direction.in'), 0)  # fill
        self.assertEqual(zero_ts.at(6).get('direction.in'), None)  # over limit skip
        self.assertEqual(zero_ts.at(7).get('direction.in'), 7)
        self.assertEqual(zero_ts.at(8).get('direction.in'), 8)
        self.assertEqual(zero_ts.at(9).get('direction.in'), 9)
        self.assertEqual(zero_ts.at(10).get('direction.in'), 10)

        self.assertEqual(zero_ts.at(0).get('direction.out'), 0)  # fill
        self.assertEqual(zero_ts.at(1).get('direction.out'), 0)  # fill
        self.assertEqual(zero_ts.at(2).get('direction.out'), None)  # over limit skip
        self.assertEqual(zero_ts.at(3).get('direction.out'), 8)
        self.assertEqual(zero_ts.at(4).get('direction.out'), 0)  # fill
        self.assertEqual(zero_ts.at(5).get('direction.out'), 12)
        self.assertEqual(zero_ts.at(6).get('direction.out'), 13)
        self.assertEqual(zero_ts.at(7).get('direction.out'), 0)  # fill
        self.assertEqual(zero_ts.at(8).get('direction.out'), 0)  # fill
        self.assertEqual(zero_ts.at(9).get('direction.out'), None)  # over limit skip
        self.assertEqual(zero_ts.at(10).get('direction.out'), None)  # over limit skip

        # verify fill limit for pad fill
        pad_ts = ts.fill(method='pad', fill_limit=2,
                         field_spec=['direction.in', 'direction.out'])

        self.assertEqual(pad_ts.at(0).get('direction.in'), 1)
        self.assertEqual(pad_ts.at(1).get('direction.in'), 1)  # fill
        self.assertEqual(pad_ts.at(2).get('direction.in'), 1)  # fill
        self.assertEqual(pad_ts.at(3).get('direction.in'), 3)
        self.assertEqual(pad_ts.at(4).get('direction.in'), 3)  # fill
        self.assertEqual(pad_ts.at(5).get('direction.in'), 3)  # fill
        self.assertEqual(pad_ts.at(6).get('direction.in'), None)  # over limit skip
        self.assertEqual(pad_ts.at(7).get('direction.in'), 7)
        self.assertEqual(pad_ts.at(8).get('direction.in'), 8)
        self.assertEqual(pad_ts.at(9).get('direction.in'), 9)
        self.assertEqual(pad_ts.at(10).get('direction.in'), 10)

        self.assertEqual(pad_ts.at(0).get('direction.out'), None)  # no fill start
        self.assertEqual(pad_ts.at(1).get('direction.out'), None)  # no fill start
        self.assertEqual(pad_ts.at(2).get('direction.out'), None)  # no fill start
        self.assertEqual(pad_ts.at(3).get('direction.out'), 8)
        self.assertEqual(pad_ts.at(4).get('direction.out'), 8)  # fill
        self.assertEqual(pad_ts.at(5).get('direction.out'), 12)
        self.assertEqual(pad_ts.at(6).get('direction.out'), 13)
        self.assertEqual(pad_ts.at(7).get('direction.out'), 13)  # fill
        self.assertEqual(pad_ts.at(8).get('direction.out'), 13)  # fill
        self.assertEqual(pad_ts.at(9).get('direction.out'), None)  # over limit skip
        self.assertEqual(pad_ts.at(10).get('direction.out'), None)  # over limit skip

    def test_fill_event_variants(self):
        """fill time range and indexed events."""

        range_list = [
            TimeRangeEvent(
                (aware_utcnow(), aware_utcnow() + datetime.timedelta(minutes=1)),
                {'in': 100}
            ),
            TimeRangeEvent(
                (aware_utcnow(), aware_utcnow() + datetime.timedelta(minutes=2)),
                {'in': None}
            ),
            TimeRangeEvent(
                (aware_utcnow(), aware_utcnow() + datetime.timedelta(minutes=3)),
                {'in': None}
            ),
            TimeRangeEvent(
                (aware_utcnow(), aware_utcnow() + datetime.timedelta(minutes=4)),
                {'in': 90}
            ),
            TimeRangeEvent(
                (aware_utcnow(), aware_utcnow() + datetime.timedelta(minutes=5)),
                {'in': 80}
            ),
            TimeRangeEvent(
                (aware_utcnow(), aware_utcnow() + datetime.timedelta(minutes=6)),
                {'in': 70}
            ),
        ]

        coll = Collection(range_list)
        # canned series objects
        rts = TimeSeries(
            dict(name='collection', collection=coll))

        new_rts = rts.fill(field_spec='in')

        self.assertEqual(new_rts.at(1).get('in'), 0)
        self.assertEqual(new_rts.at(2).get('in'), 0)

        # indexed events

        index_list = [
            IndexedEvent('1d-12355', {'value': 42}),
            IndexedEvent('1d-12356', {'value': None}),
            IndexedEvent('1d-12357', {'value': None}),
            IndexedEvent('1d-12358', {'value': 52}),
            IndexedEvent('1d-12359', {'value': 55}),
            IndexedEvent('1d-12360', {'value': 58}),
        ]

        coll = Collection(index_list)

        its = TimeSeries(
            dict(name='collection', collection=coll))

        new_its = its.fill()

        self.assertEqual(new_its.at(1).get(), 0)
        self.assertEqual(new_its.at(2).get(), 0)

    def test_scan_stop(self):
        """stop seeking good values if there are none - for coverage."""

        simple_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000, {'in': 1, 'out': None}],
                [1400425948000, {'in': 3, 'out': None}],
                [1400425949000, {'in': None, 'out': None}],
                [1400425950000, {'in': None, 'out': 8}],
                [1400425960000, {'in': None, 'out': None}],
                [1400425970000, {'in': None, 'out': 12}],
                [1400425980000, {'in': None, 'out': 13}],
            ]
        )

        ts = TimeSeries(simple_missing_data)

        new_ts = ts.fill(field_spec='direction.out', method='linear')

        self.assertEqual(new_ts.at(2).get('direction.in'), None)
        self.assertEqual(new_ts.at(3).get('direction.in'), None)
        self.assertEqual(new_ts.at(4).get('direction.in'), None)
        self.assertEqual(new_ts.at(5).get('direction.in'), None)
        self.assertEqual(new_ts.at(6).get('direction.in'), None)

    def test_pad(self):
        """Test the pad style fill."""

        simple_missing_data = dict(
            name="traffic",
            columns=["time", "direction"],
            points=[
                [1400425947000, {'in': 1, 'out': None, 'drop': None}],
                [1400425948000, {'in': None, 'out': 4, 'drop': None}],
                [1400425949000, {'in': None, 'out': None, 'drop': 13}],
                [1400425950000, {'in': None, 'out': None, 'drop': 14}],
                [1400425960000, {'in': 9, 'out': 8, 'drop': None}],
                [1400425970000, {'in': 11, 'out': 10, 'drop': 16}],
            ]
        )

        ts = TimeSeries(simple_missing_data)

        new_ts = ts.fill(method='pad',
                         field_spec=['direction.in', 'direction.out', 'direction.drop'])

        self.assertEqual(new_ts.at(0).get('direction.in'), 1)
        self.assertEqual(new_ts.at(1).get('direction.in'), 1)  # padded
        self.assertEqual(new_ts.at(2).get('direction.in'), 1)  # padded
        self.assertEqual(new_ts.at(3).get('direction.in'), 1)  # padded
        self.assertEqual(new_ts.at(4).get('direction.in'), 9)
        self.assertEqual(new_ts.at(5).get('direction.in'), 11)

        self.assertEqual(new_ts.at(0).get('direction.out'), None)  # 1st can't pad
        self.assertEqual(new_ts.at(1).get('direction.out'), 4)
        self.assertEqual(new_ts.at(2).get('direction.out'), 4)  # padded
        self.assertEqual(new_ts.at(3).get('direction.out'), 4)  # padded
        self.assertEqual(new_ts.at(4).get('direction.out'), 8)
        self.assertEqual(new_ts.at(5).get('direction.out'), 10)

        self.assertEqual(new_ts.at(0).get('direction.drop'), None)  # 1st can't pad
        self.assertEqual(new_ts.at(1).get('direction.drop'), None)  # bad prev can't pad
        self.assertEqual(new_ts.at(2).get('direction.drop'), 13)
        self.assertEqual(new_ts.at(3).get('direction.drop'), 14)
        self.assertEqual(new_ts.at(4).get('direction.drop'), 14)  # padded
        self.assertEqual(new_ts.at(5).get('direction.drop'), 16)


if __name__ == '__main__':
    unittest.main()
