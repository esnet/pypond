#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Tests for the TimeSeries class

Also including tests for Collection class since they are tightly bound.
"""

import copy
import datetime
import unittest
import warnings

from pypond.collection import Collection
from pypond.event import Event
from pypond.exceptions import (
    CollectionException,
    CollectionWarning,
    PipelineException,
    TimeSeriesException,
)
from pypond.functions import Functions
from pypond.index import Index
from pypond.indexed_event import IndexedEvent
from pypond.series import TimeSeries
from pypond.timerange_event import TimeRangeEvent
from pypond.util import is_pvector, ms_from_dt, aware_utcnow, dt_from_ms

# taken from the pipeline tests
EVENT_LIST = [
    Event(1429673400000, {'in': 1, 'out': 2}),
    Event(1429673460000, {'in': 3, 'out': 4}),
    Event(1429673520000, {'in': 5, 'out': 6}),
]

# taken from the series tests
DATA = dict(
    name="traffic",
    columns=["time", "value", "status"],
    points=[
        [1400425947000, 52, "ok"],
        [1400425948000, 18, "ok"],
        [1400425949000, 26, "fail"],
        [1400425950000, 93, "offline"]
    ]
)

INDEXED_DATA = dict(
    index="1d-625",
    name="traffic",
    columns=["time", "value", "status"],
    points=[
        [1400425947000, 52, "ok"],
        [1400425948000, 18, "ok"],
        [1400425949000, 26, "fail"],
        [1400425950000, 93, "offline"]
    ]
)

TRAFFIC_DATA_IN = dict(
    name="star-cr5:to_anl_ip-a_v4",
    columns=["time", "in"],
    points=[
        [1400425947000, 52],
        [1400425948000, 18],
        [1400425949000, 26],
        [1400425950000, 93]
    ]
)

TRAFFIC_DATA_OUT = dict(
    name="star-cr5:to_anl_ip-a_v4",
    columns=["time", "out"],
    points=[
        [1400425947000, 34],
        [1400425948000, 13],
        [1400425949000, 67],
        [1400425950000, 91]
    ]
)

AVAILABILITY_DATA = dict(
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

TICKET_RANGE = dict(
    name="outages",
    columns=["timerange", "title", "esnet_ticket"],
    points=[
        [[1429673400000, 1429707600000], "BOOM", "ESNET-20080101-001"],
        [[1429673400000, 1429707600000], "BAM!", "ESNET-20080101-002"],
    ],
)


class SeriesBase(unittest.TestCase):
    """
    base for the tests.
    """

    def setUp(self):
        """setup."""
        # canned collection
        self._canned_collection = Collection(EVENT_LIST)
        # canned series objects
        self._canned_event_series = TimeSeries(
            dict(name='collection', collection=self._canned_collection))
        self._canned_wire_series = TimeSeries(DATA)
        # canned index
        self._canned_index_series = TimeSeries(INDEXED_DATA)


class TestTimeSeries(SeriesBase):
    """
    Test variations of TimeSeries object creation.
    """

    def test_series_creation(self):
        """test timeseries creation.

        Calls to to_json() are to trigger coverage for different variants.
        """

        # from a wire format event list
        ts1 = TimeSeries(DATA)
        self.assertEqual(ts1.size(), len(DATA.get('points')))

        # from a wire format index
        ts2 = TimeSeries(AVAILABILITY_DATA)
        self.assertEqual(ts2.size(), len(AVAILABILITY_DATA.get('points')))
        self.assertEqual(ts2.to_json().get('name'), 'availability')

        # from a list of events
        ts3 = TimeSeries(dict(name='events', events=EVENT_LIST))
        self.assertEqual(ts3.size(), len(EVENT_LIST))

        # from a collection
        ts4 = TimeSeries(dict(name='collection', collection=self._canned_collection))
        self.assertEqual(ts4.size(), self._canned_collection.size())

        # copy constructor
        ts5 = TimeSeries(ts4)
        self.assertEqual(ts4.size(), ts5.size())

        # from a wire format time range
        ts6 = TimeSeries(TICKET_RANGE)
        self.assertEqual(ts6.size(), len(TICKET_RANGE.get('points')))
        self.assertEqual(ts6.to_json().get('name'), 'outages')

        # non-utc indexed data variant mostly for coverage
        idxd = copy.deepcopy(INDEXED_DATA)
        idxd['utc'] = False
        ts7 = TimeSeries(idxd)
        self.assertFalse(ts7.is_utc())
        self.assertFalse(ts7.to_json().get('utc'))

        # indexed data variant using Index object - for coverage as well
        idxd2 = copy.deepcopy(INDEXED_DATA)
        idxd2['index'] = Index(idxd2.get('index'))
        ts8 = TimeSeries(idxd2)
        self.assertEqual(ts8.to_json().get('index'), '1d-625')

    def test_bad_ctor_args(self):
        """bogus conctructor args."""

        # bad wire format/etc
        with self.assertRaises(TimeSeriesException):
            TimeSeries(dict())

        # neither dict nor TimeSeries instance
        with self.assertRaises(TimeSeriesException):
            TimeSeries(list())

        # bad wire format
        bad_wire = copy.deepcopy(TICKET_RANGE)
        bad_wire.get('columns')[0] = 'bogus_type'

        with self.assertRaises(TimeSeriesException):
            TimeSeries(bad_wire)

    def test_range_accessors(self):
        """accessors to get at the underlying timerange."""
        self.assertEqual(
            self._canned_event_series.begin(),
            EVENT_LIST[0].timestamp())

        self.assertEqual(
            self._canned_event_series.end(),
            EVENT_LIST[-1].timestamp())

        self.assertEqual(
            self._canned_event_series.at(1).to_string(),
            EVENT_LIST[1].to_string())

    def test_slices_and_permutations(self):
        """methods that slice/etc the underlying series."""

        # bisect
        search = dt_from_ms(1400425949000 + 30)
        bsect_idx = self._canned_wire_series.bisect(search)

        # bisect with bad arg
        bad_search = datetime.datetime.now()
        with self.assertRaises(CollectionException):
            self._canned_wire_series.bisect(bad_search)

        bsection = self._canned_wire_series.at(bsect_idx)
        self.assertEqual(bsection.data().get('status'), 'fail')

        # clean
        self.assertEqual(self._canned_event_series.clean('in').size(), 3)
        self.assertEqual(self._canned_event_series.clean('bogus_value').size(), 0)

        # slice
        sliced = self._canned_event_series.slice(1, 3)
        self.assertEqual(sliced.size(), 2)
        self.assertTrue(Event.same(sliced.at(0), EVENT_LIST[1]))

    def test_data_accessors(self):
        """methods to get metadata and such."""
        self.assertEqual(self._canned_wire_series.name(), 'traffic')
        self.assertTrue(self._canned_wire_series.is_utc())

        # index stuff
        self.assertEqual(
            self._canned_index_series.index_as_string(),
            INDEXED_DATA.get('index'))
        self.assertTrue(isinstance(self._canned_index_series.index(), Index))
        self.assertEqual(
            self._canned_index_series.index_as_range().to_json(),
            [54000000000, 54086400000])

        self.assertEqual(
            self._canned_wire_series.meta(),
            {'utc': True, 'name': 'traffic'})

        self.assertEqual(self._canned_wire_series.meta('name'), 'traffic')

        self.assertEqual(len(list(self._canned_wire_series.events())), 4)
        self.assertEqual(
            self._canned_event_series.collection(),
            self._canned_event_series._collection)  # pylint: disable=protected-access

        self.assertEqual(self._canned_event_series.size_valid('in'), 3)

        # differently ordered in python3 and is a bad test anyways
        # self.assertEqual(
        #     str(self._canned_event_series),
        #     '{"utc": true, "points": [[1429673400000, 1, 2], [1429673460000, 3, 4], [1429673520000, 5, 6]], "name": "collection", "columns": ["time", "in", "out"]}'  # pylint: disable=line-too-long
        # )

    def test_underlying_methods(self):
        """basically aliases for underlying collection methods."""

        self.assertEqual(self._canned_event_series.count(), len(EVENT_LIST))

        tser = self._canned_event_series
        self.assertEqual(tser.sum('in').get('in'), 9)
        self.assertEqual(tser.avg('out').get('out'), 4)
        self.assertEqual(tser.mean('out').get('out'), 4)
        self.assertEqual(tser.min('in').get('in'), 1)
        self.assertEqual(tser.max('in').get('in'), 5)
        self.assertEqual(tser.median('out').get('out'), 4)
        self.assertEqual(tser.stdev('out').get('out'), 1.632993161855452)
        # redundant, but for coverage
        self.assertEqual(tser.aggregate(Functions.sum, 'in').get('in'), 9)

    def test_equality_methods(self):
        """test equal/same static methods."""

        ser1 = TimeSeries(DATA)
        ser2 = TimeSeries(DATA)

        self.assertTrue(TimeSeries.equal(ser1, ser1))
        self.assertTrue(TimeSeries.same(ser1, ser1))

        self.assertFalse(TimeSeries.equal(ser1, ser2))
        self.assertTrue(TimeSeries.same(ser1, ser2))

        copy_ctor = TimeSeries(ser1)
        self.assertTrue(TimeSeries.equal(copy_ctor, ser1))
        self.assertFalse(copy_ctor is ser1)

    def test_merge_sum_and_map(self):
        """test the time series merging/map static methods."""
        t_in = TimeSeries(TRAFFIC_DATA_IN)
        t_out = TimeSeries(TRAFFIC_DATA_OUT)

        t_merged = TimeSeries.merge(dict(name='traffic'), [t_in, t_out])

        self.assertEqual(t_merged.at(2).get('in'), 26)
        self.assertEqual(t_merged.at(2).get('out'), 67)

        t_summed = TimeSeries.sum_list(dict(name='traffic'), [t_in, t_in], 'in')

        self.assertEqual(t_summed.at(0).get('in'), 104)
        self.assertEqual(t_summed.at(1).get('in'), 36)

        # more variations for coverage

        test_idx_data = dict(
            name="availability",
            columns=["index", "uptime"],
            points=[
                ["2015-06", 100],
                ["2015-05", 92],
                ["2015-04", 87],
                ["2015-03", 99],
                ["2015-02", 92],
                ["2015-01", 100],
                ["2014-12", 99],
                ["2014-11", 91],
                ["2014-10", 99],
                ["2014-09", 95],
                ["2014-08", 88],
                ["2014-07", 100]
            ]
        )

        t_idx = TimeSeries(test_idx_data)
        idx_sum = TimeSeries.sum_list(dict(name='available'), [t_idx, t_idx], 'uptime')
        self.assertEqual(idx_sum.at(0).get('uptime'), 200)
        self.assertEqual(idx_sum.at(1).get('uptime'), 184)
        self.assertEqual(idx_sum.at(2).get('uptime'), 174)

        test_outage = dict(
            name="outages",
            columns=["timerange", "length", "esnet_ticket"],
            points=[
                [[1429673400000, 1429707600000], 23, "ESNET-20080101-001"],
                [[1429673500000, 1429707700000], 54, "ESNET-20080101-002"],
            ],
        )

        t_tr = TimeSeries(test_outage)
        tr_sum = TimeSeries.sum_list(dict(name='outage length'), [t_tr, t_tr], 'length')
        self.assertEqual(tr_sum.at(0).get('length'), 46)
        self.assertEqual(tr_sum.at(1).get('length'), 108)

    def test_ts_collapse(self):
        """
        Test TimeSeries.collapse()
        """
        ces = self._canned_event_series

        collapsed_ces = ces.collapse(['in', 'out'], 'in_out_sum', Functions.sum)
        self.assertEqual(len(collapsed_ces.columns()), 3)

        for i in collapsed_ces.events():
            self.assertEqual(i.get('in') + i.get('out'), i.get('in_out_sum'))


class TestCollection(SeriesBase):
    """
    Tests for the collection class.
    """

    def test_create_collection(self):
        """test collection creation and methods related to internal payload."""

        # event list
        col_1 = Collection(EVENT_LIST)
        self.assertEqual(col_1.size(), 3)
        self.assertEqual(col_1.type(), Event)
        self.assertEqual(col_1.size_valid('in'), 3)

        # copy ctor
        col_2 = Collection(col_1)
        self.assertEqual(col_2.size(), 3)
        self.assertEqual(col_2.type(), Event)
        self.assertEqual(col_2.size_valid('in'), 3)

        # copy ctor - no event copy
        col_3 = Collection(col_2, copy_events=False)
        self.assertEqual(col_3.size(), 0)
        self.assertEqual(col_3.size_valid('in'), 0)

        # pass in an immutable - use a pre- _check()'ed one
        col_4 = Collection(col_1._event_list)  # pylint: disable=protected-access
        self.assertEqual(col_4.size(), 3)
        self.assertEqual(col_4.size_valid('in'), 3)

        # other event types for coverage
        ie1 = IndexedEvent('1d-12355', {'value': 42})
        ie2 = IndexedEvent('1d-12356', {'value': 4242})
        col_5 = Collection([ie1, ie2])
        self.assertEqual(col_5.size(), 2)

        tre = TimeRangeEvent(
            (aware_utcnow(), aware_utcnow() + datetime.timedelta(hours=24)),
            {'in': 100})
        col_6 = Collection([tre])
        self.assertEqual(col_6.size(), 1)

    def test_accessor_methods(self):
        """test various access methods. Mostly for coverage."""

        col = self._canned_collection

        # basic accessors
        self.assertEqual(col.to_json(), EVENT_LIST)
        # These are ordered differently in python3 and is a bad test to begin with.
        # self.assertEqual(
        #     col.to_string(),
        #     '[{"data": {"out": 2, "in": 1}, "time": 1429673400000}, {"data": {"out": 4, "in": 3}, "time": 1429673460000}, {"data": {"out": 6, "in": 5}, "time": 1429673520000}]')  # pylint: disable=line-too-long
        # self.assertEqual(
        #     str(col),
        #     '[{"data": {"out": 2, "in": 1}, "time": 1429673400000}, {"data": {"out": 4, "in": 3}, "time": 1429673460000}, {"data": {"out": 6, "in": 5}, "time": 1429673520000}]')  # pylint: disable=line-too-long

        # test at() - corollary to array index
        self.assertTrue(Event.same(col.at(2), EVENT_LIST[2]))

        # overshoot
        with self.assertRaises(CollectionException):
            col.at(5)

        # test at_time()
        # get timestamp of second event and add some time to it
        ref_dtime = EVENT_LIST[1].timestamp() + datetime.timedelta(seconds=3)
        self.assertTrue(Event.same(col.at_time(ref_dtime), EVENT_LIST[1]))

        # overshoot the end of the list for coverage
        ref_dtime = EVENT_LIST[2].timestamp() + datetime.timedelta(seconds=3)
        self.assertTrue(Event.same(col.at_time(ref_dtime), EVENT_LIST[2]))

        # hit dead on for coverage
        self.assertEqual(col.at_time(EVENT_LIST[1].timestamp()).get('in'), 3)

        # empty collection for coverage
        empty_coll = Collection(col, copy_events=False)
        self.assertIsNone(empty_coll.at_time(ref_dtime))

        # at_first() and at_last()
        self.assertTrue(Event.same(col.at_first(), EVENT_LIST[0]))
        self.assertTrue(Event.same(col.at_last(), EVENT_LIST[2]))

        # get the raw event list
        self.assertTrue(is_pvector(col.event_list()))
        self.assertTrue(isinstance(col.event_list_as_list(), list))

    def test_maps_and_etc(self):
        """
        Test the accessors that return transformations, timeranges, etc.
        """
        col = self._canned_collection

        # range() = a TimeRange object representing the extents of the
        # events in the event list.
        new_range = col.range()
        self.assertTrue(ms_from_dt(new_range.begin()), 1429673400000)
        self.assertTrue(ms_from_dt(new_range.end()), 1429673520000)

        # filter
        def out_is_four(event):
            """test function"""
            return bool(event.get('out') == 4)
        filtered = col.filter(out_is_four)
        self.assertEqual(filtered.size(), 1)

        # map
        def in_only(event):
            """make new events wtin only data in."""
            return Event(event.timestamp(), {'in': event.get('in')})
        mapped = col.map(in_only)
        self.assertEqual(mapped.count(), 3)
        for i in mapped.events():
            self.assertIsNone(i.get('out'))

        # clean
        cleaned_good = col.clean('in')
        self.assertEqual(cleaned_good.size(), 3)

        cleaned_bad = col.clean(['bogus_data_key'])
        self.assertEqual(cleaned_bad.size(), 0)

    def test_collection_collapse(self):
        """test Collection.collaps()"""
        col = self._canned_collection

        collapsed_col = col.collapse(['in', 'out'], 'in_out_sum', Functions.sum)
        self.assertEqual(collapsed_col.size(), 3)

        for i in collapsed_col.events():
            self.assertEqual(len(list(i.data().keys())), 3)
            self.assertEqual(i.get('in') + i.get('out'), i.get('in_out_sum'))

    def test_aggregations(self):
        """sum, min, max, etc.
        """

        col = self._canned_collection
        self.assertEqual(col.sum('in').get('in'), 9)
        self.assertEqual(col.avg('out').get('out'), 4)
        self.assertEqual(col.mean('out').get('out'), 4)
        self.assertEqual(col.min('in').get('in'), 1)
        self.assertEqual(col.max('in').get('in'), 5)
        self.assertEqual(col.first('out').get('out'), 2)
        self.assertEqual(col.last('out').get('out'), 6)
        self.assertEqual(col.median('out').get('out'), 4)
        self.assertEqual(col.stdev('out').get('out'), 1.632993161855452)

    def test_mutators(self):
        """test collection mutation."""

        extra_event = Event(1429673580000, {'in': 7, 'out': 8})

        new_coll = self._canned_collection.add_event(extra_event)
        self.assertEqual(new_coll.size(), 4)

        # test slice() here since this collection is longer.
        sliced = new_coll.slice(1, 3)
        self.assertEqual(sliced.size(), 2)
        self.assertTrue(Event.same(sliced.at(0), EVENT_LIST[1]))

        # work stddev as well
        self.assertEqual(new_coll.stdev('in').get('in'), 2.23606797749979)
        self.assertEqual(new_coll.median('in').get('in'), 4)

    def test_bad_args(self):
        """pass in bad values"""
        with warnings.catch_warnings(record=True) as wrn:
            bad_col = Collection(dict())
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, CollectionWarning))
            self.assertEqual(bad_col.size(), 0)

    def test_other_exceptions(self):
        """trigger other exceptions"""
        with self.assertRaises(PipelineException):
            self._canned_collection.start()

        with self.assertRaises(PipelineException):
            self._canned_collection.stop()

        with self.assertRaises(PipelineException):
            self._canned_collection.on_emit()

        with self.assertRaises(PipelineException):
            ie1 = IndexedEvent('1d-12355', {'value': 42})
            self._canned_collection.add_event(ie1)

    def test_equality_methods(self):
        """test equal/same static methods."""
        self.assertTrue(
            Collection.equal(self._canned_collection, self._canned_collection))
        self.assertTrue(
            Collection.same(self._canned_collection, self._canned_collection))

        self.assertFalse(
            Collection.equal(self._canned_collection, Collection(EVENT_LIST)))
        self.assertTrue(
            Collection.same(self._canned_collection, Collection(EVENT_LIST)))

        copy_ctor = Collection(self._canned_collection)
        self.assertTrue(Collection.equal(self._canned_collection, copy_ctor))
        self.assertFalse(copy_ctor is self._canned_collection)

if __name__ == '__main__':
    unittest.main()
