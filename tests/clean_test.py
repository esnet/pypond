"""
Tests for sanitizing, filling and renaming data.
"""

import copy
import datetime
import unittest
import warnings

from pypond.collection import Collection
from pypond.event import Event
from pypond.exceptions import ProcessorException, ProcessorWarning
from pypond.indexed_event import IndexedEvent
from pypond.pipeline import Pipeline
from pypond.pipeline_in import UnboundedIn
from pypond.processors import Filler
from pypond.series import TimeSeries
from pypond.timerange_event import TimeRangeEvent
from pypond.util import aware_utcnow

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


class TestRenameFillAndAlign(CleanBase):
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

        with self.assertRaises(ProcessorException):
            f = Filler(dict())

        with self.assertRaises(ProcessorException):
            ts.fill(method='bogus')

        with self.assertRaises(ProcessorException):

            uin = UnboundedIn()

            elist = (
                Pipeline()
                .from_source(uin)
                .emit_on('flush')
                .fill(field_spec='direction.in', method='linear')
                .to_event_list()
            )

        with self.assertRaises(ProcessorException):

            elist = (
                Pipeline()
                .from_source(ts)
                .emit_on('bad_emit')
                .fill(field_spec='direction.in', method='linear')
                .to_event_list()
            )

        with warnings.catch_warnings(record=True) as wrn:
            ts.fill(field_spec='bad.path')
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

        with warnings.catch_warnings(record=True) as wrn:
            ts.fill(field_spec='direction.bogus')
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

        # trigger warnings about non-numeric values in linear.

        with warnings.catch_warnings(record=True) as wrn:
            simple_list_data = dict(
                name="traffic",
                columns=["time", "series"],
                points=[
                    [1400425947000, ['non numeric', None, 3, 4, 5, 6, 7]],
                ]
            )

            ts = TimeSeries(simple_list_data)

            new_ts = ts.fill(field_spec='series', method='linear')

            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

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

            new_ts = ts.fill(field_spec='direction.in', method='linear')

            # self.assertEqual(len(wrn), 1)
            # self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

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

        # fill all invalid values, limit to 3 in result set

        new_ts = ts.fill(limit=3)

        self.assertEqual(new_ts.size(), 3)

        self.assertEqual(new_ts.at(0).get('direction.out'), 0)
        self.assertEqual(new_ts.at(2).get('direction.out'), 0)

        self.assertEqual(new_ts.at(1).get('direction.in'), 0)

        # fill one column, limit to 4 in result set

        new_ts = ts.fill(field_spec='direction.in', limit=4)

        self.assertEqual(new_ts.size(), 4)

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

        new_ts = ts.fill()

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

        new_ts = ts.fill(field_spec=['direction.in', 'direction.out'],
                         method='linear', limit=6)

        self.assertEqual(new_ts.size(), 6)

        self.assertEqual(new_ts.at(0).get('direction.in'), 1)
        self.assertEqual(new_ts.at(1).get('direction.in'), 2.0)  # filled
        self.assertEqual(new_ts.at(2).get('direction.in'), 2.5)  # filled
        self.assertEqual(new_ts.at(3).get('direction.in'), 3)
        self.assertEqual(new_ts.at(4).get('direction.in'), 4.0)  # filled
        self.assertEqual(new_ts.at(5).get('direction.in'), 5)

        self.assertEqual(new_ts.at(0).get('direction.out'), None)  # 1st can't fill
        self.assertEqual(new_ts.at(1).get('direction.out'), None)  # no prev good val
        self.assertEqual(new_ts.at(2).get('direction.out'), None)  # no prev good val
        self.assertEqual(new_ts.at(3).get('direction.out'), 8)
        self.assertEqual(new_ts.at(4).get('direction.out'), 10.0)  # filled
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

        elist = (
            Pipeline()
            .from_source(ts)
            .emit_on('flush')  # it's linear
            .fill(field_spec='direction.in', method='linear')
            .to_event_list()
        )

        self.assertEqual(elist[0].get('direction.in'), 1)
        self.assertEqual(elist[1].get('direction.in'), 2.0)  # filled
        self.assertEqual(elist[2].get('direction.in'), 2.5)  # filled
        self.assertEqual(elist[3].get('direction.in'), 3)
        self.assertEqual(elist[4].get('direction.in'), 4.0)  # filled
        self.assertEqual(elist[5].get('direction.in'), 5)

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

        new_rts = rts.fill()

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

        new_ts = ts.fill(field_spec='direction.in', method='linear')
        self.assertEqual(new_ts.at(2).get('direction.in'), None)
        self.assertEqual(new_ts.at(3).get('direction.in'), None)
        self.assertEqual(new_ts.at(4).get('direction.in'), None)
        self.assertEqual(new_ts.at(5).get('direction.in'), None)
        self.assertEqual(new_ts.at(6).get('direction.in'), None)

    def test_list_fill(self):
        """Test basic filling of list values."""

        simple_list_data = dict(
            name="traffic",
            columns=["time", "series"],
            points=[
                [1400425947000, [None, None, 3, 4, 5, 6, 7]],
                [1400425948000, [1, None, None, 4, 5, 6, 7]],
                [1400425949000, [1, 2, 3, 4, None, None, None]],
                [1400425950000, [1, 2, 3, 4, None, None, 7]],
            ]
        )

        ts = TimeSeries(simple_list_data)

        new_ts = ts.fill(field_spec='series', method='zero')

        self.assertEqual(new_ts.at(0).get('series')[0], 0)
        self.assertEqual(new_ts.at(0).get('series')[1], 0)

        self.assertEqual(new_ts.at(1).get('series')[1], 0)
        self.assertEqual(new_ts.at(1).get('series')[2], 0)

        self.assertEqual(new_ts.at(2).get('series')[4], 0)
        self.assertEqual(new_ts.at(2).get('series')[5], 0)
        self.assertEqual(new_ts.at(2).get('series')[6], 0)

        new_ts = ts.fill(field_spec='series', method='pad')

        self.assertEqual(new_ts.at(0).get('series')[0], None)
        self.assertEqual(new_ts.at(0).get('series')[1], None)

        self.assertEqual(new_ts.at(1).get('series')[1], 1)
        self.assertEqual(new_ts.at(1).get('series')[2], 1)

        self.assertEqual(new_ts.at(2).get('series')[4], 4)
        self.assertEqual(new_ts.at(2).get('series')[5], 4)
        self.assertEqual(new_ts.at(2).get('series')[6], 4)

        new_ts = ts.fill(field_spec='series', method='linear')

        self.assertEqual(new_ts.at(0).get('series')[0], None)
        self.assertEqual(new_ts.at(0).get('series')[1], None)

        self.assertEqual(new_ts.at(1).get('series')[1], 2.5)
        self.assertEqual(new_ts.at(1).get('series')[2], 3.25)

        self.assertEqual(new_ts.at(2).get('series')[4], None)
        self.assertEqual(new_ts.at(2).get('series')[5], None)
        self.assertEqual(new_ts.at(2).get('series')[6], None)

        self.assertEqual(new_ts.at(3).get('series')[4], 5.5)
        self.assertEqual(new_ts.at(3).get('series')[5], 6.25)

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

        new_ts = ts.fill(method='pad')

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
