"""
Tests for the TimeSeries class

Also including tests for Collection class since they are tightly bound.
"""

import copy
import datetime
import unittest
import warnings

from pypond.collection import Collection
from pypond.event import Event, IndexedEvent, TimeRangeEvent
from pypond.exceptions import (
    CollectionException,
    CollectionWarning,
    PipelineException,
    TimeSeriesException,
)
from pypond.index import Index
from pypond.series import TimeSeries
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
        self.assertEquals(ts1.size(), len(DATA.get('points')))

        # from a wire format index
        ts2 = TimeSeries(AVAILABILITY_DATA)
        self.assertEquals(ts2.size(), len(AVAILABILITY_DATA.get('points')))
        self.assertEquals(ts2.to_json().get('name'), 'availability')

        # from a list of events
        ts3 = TimeSeries(dict(name='events', events=EVENT_LIST))
        self.assertEquals(ts3.size(), len(EVENT_LIST))

        # from a collection
        ts4 = TimeSeries(dict(name='collection', collection=self._canned_collection))
        self.assertEquals(ts4.size(), self._canned_collection.size())

        # copy constructor
        ts5 = TimeSeries(ts4)
        self.assertEquals(ts4.size(), ts5.size())

        # from a wire format time range
        ts6 = TimeSeries(TICKET_RANGE)
        self.assertEquals(ts6.size(), len(TICKET_RANGE.get('points')))
        self.assertEquals(ts6.to_json().get('name'), 'outages')

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
        self.assertEquals(ts8.to_json().get('index').to_string(), '1d-625')

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
        self.assertEquals(
            self._canned_event_series.begin(),
            EVENT_LIST[0].timestamp())

        self.assertEquals(
            self._canned_event_series.end(),
            EVENT_LIST[-1].timestamp())

        self.assertEquals(
            self._canned_event_series.at(1).to_string(),
            EVENT_LIST[1].to_string())

    def test_slices_and_permutations(self):
        """methods that slice/etc the underlying series."""

        # bisect
        search = dt_from_ms(1400425949000 + 30)
        bsect_idx = self._canned_wire_series.bisect(search)

        bsection = self._canned_wire_series.at(bsect_idx)
        self.assertEquals(bsection.data().get('status'), 'fail')

        # clean
        self.assertEquals(self._canned_event_series.clean('in').size(), 3)
        self.assertEquals(self._canned_event_series.clean('bogus_value').size(), 0)

        # slice
        sliced = self._canned_event_series.slice(1, 3)
        self.assertEquals(sliced.size(), 2)
        self.assertTrue(Event.same(sliced.at(0), EVENT_LIST[1]))

    def test_data_accessors(self):
        """methods to get metadata and such."""
        self.assertEquals(self._canned_wire_series.name(), 'traffic')
        self.assertTrue(self._canned_wire_series.is_utc())

        # index stuff
        self.assertEquals(
            self._canned_index_series.index_as_string(),
            INDEXED_DATA.get('index'))
        self.assertTrue(isinstance(self._canned_index_series.index(), Index))
        self.assertEquals(
            self._canned_index_series.index_as_range().to_json(),
            [54000000000, 54086400000])

        self.assertEquals(
            self._canned_wire_series.meta(),
            {'utc': True, 'name': 'traffic'})

        self.assertEquals(self._canned_wire_series.meta('name'), 'traffic')

        self.assertEquals(len(list(self._canned_wire_series.events())), 4)
        self.assertEquals(
            self._canned_event_series.collection(),
            self._canned_event_series._collection)  # pylint: disable=protected-access

        self.assertEquals(self._canned_event_series.size_valid('in'), 3)

    def test_underlying_methods(self):
        """basically aliases for underlying collection methods."""

        self.assertEquals(self._canned_event_series.count(), len(EVENT_LIST))

        tser = self._canned_event_series
        self.assertEquals(tser.sum('in').get('in'), 9)
        self.assertEquals(tser.avg('out').get('out'), 4)
        self.assertEquals(tser.mean('out').get('out'), 4)
        self.assertEquals(tser.min('in').get('in'), 1)
        self.assertEquals(tser.max('in').get('in'), 5)
        # self.assertEquals(tser.first('out').get('out'), 2)
        # self.assertEquals(tser.last('out').get('out'), 6)
        self.assertEquals(tser.median('out').get('out'), 4)
        self.assertEquals(tser.stdev('out').get('out'), 1.632993161855452)

    def test_equality_methods(self):
        """test equal/same static methods."""

        ser1 = TimeSeries(DATA)
        ser2 = TimeSeries(DATA)

        self.assertTrue(TimeSeries.equal(ser1, ser1))
        # self.assertTrue(TimeSeries.same(ser1, ser1))

        self.assertFalse(TimeSeries.equal(ser1, ser2))

        # XXX _collection._event_list not evaluating as the same
        print TimeSeries.same(ser1, ser2)


class TestCollection(SeriesBase):
    """
    Tests for the collection class.
    """
    def test_create_collection(self):
        """test collection creation and methods related to internal payload."""

        # event list
        col_1 = Collection(EVENT_LIST)
        self.assertEquals(col_1.size(), 3)
        self.assertEquals(col_1.type(), Event)
        self.assertEquals(col_1.size_valid('in'), 3)

        # copy ctor
        col_2 = Collection(col_1)
        self.assertEquals(col_2.size(), 3)
        self.assertEquals(col_2.type(), Event)
        self.assertEquals(col_2.size_valid('in'), 3)

        # copy ctor - no event copy
        col_3 = Collection(col_2, copy_events=False)
        self.assertEquals(col_3.size(), 0)
        self.assertEquals(col_3.size_valid('in'), 0)

        # pass in an immutable - use a pre- _check()'ed one
        col_4 = Collection(col_1._event_list)  # pylint: disable=protected-access
        self.assertEquals(col_4.size(), 3)
        self.assertEquals(col_4.size_valid('in'), 3)

        # other event types for coverage
        ie1 = IndexedEvent('1d-12355', {'value': 42})
        ie2 = IndexedEvent('1d-12356', {'value': 4242})
        col_5 = Collection([ie1, ie2])
        self.assertEquals(col_5.size(), 2)

        tre = TimeRangeEvent(
            (aware_utcnow(), aware_utcnow() + datetime.timedelta(hours=24)),
            {'in': 100})
        col_6 = Collection([tre])
        self.assertEquals(col_6.size(), 1)

    def test_accessor_methods(self):
        """test various access methods. Mostly for coverage."""

        col = self._canned_collection

        # basic accessors
        self.assertEquals(col.to_json(), EVENT_LIST)
        self.assertEquals(
            col.to_string(),
            '[{"data": {"out": 2, "in": 1}, "time": 1429673400000}, {"data": {"out": 4, "in": 3}, "time": 1429673460000}, {"data": {"out": 6, "in": 5}, "time": 1429673520000}]')  # pylint: disable=line-too-long
        self.assertEquals(
            str(col),
            '[{"data": {"out": 2, "in": 1}, "time": 1429673400000}, {"data": {"out": 4, "in": 3}, "time": 1429673460000}, {"data": {"out": 6, "in": 5}, "time": 1429673520000}]')  # pylint: disable=line-too-long

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
        self.assertEquals(filtered.size(), 1)

        # map
        def in_only(event):
            """make new events wtin only data in."""
            return Event(event.timestamp(), {'in': event.get('in')})
        mapped = col.map(in_only)
        self.assertEquals(mapped.count(), 3)
        for i in mapped.events():
            self.assertIsNone(i.get('out'))

        # clean
        cleaned_good = col.clean('in')
        self.assertEquals(cleaned_good.size(), 3)

        cleaned_bad = col.clean(['bogus_data_key'])
        self.assertEquals(cleaned_bad.size(), 0)

    def test_aggregations(self):
        """sum, min, max, etc.
        """

        col = self._canned_collection
        self.assertEquals(col.sum('in').get('in'), 9)
        self.assertEquals(col.avg('out').get('out'), 4)
        self.assertEquals(col.mean('out').get('out'), 4)
        self.assertEquals(col.min('in').get('in'), 1)
        self.assertEquals(col.max('in').get('in'), 5)
        self.assertEquals(col.first('out').get('out'), 2)
        self.assertEquals(col.last('out').get('out'), 6)
        self.assertEquals(col.median('out').get('out'), 4)
        self.assertEquals(col.stdev('out').get('out'), 1.632993161855452)

    def test_mutators(self):
        """test collection mutation."""

        extra_event = Event(1429673580000, {'in': 7, 'out': 8})

        new_coll = self._canned_collection.add_event(extra_event)
        self.assertEquals(new_coll.size(), 4)

        # test slice() here since this collection is longer.
        sliced = new_coll.slice(1, 3)
        self.assertEquals(sliced.size(), 2)
        self.assertTrue(Event.same(sliced.at(0), EVENT_LIST[1]))

        # work stddev as well
        self.assertEquals(new_coll.stdev('in').get('in'), 2.23606797749979)
        self.assertEquals(new_coll.median('in').get('in'), 4)

    def test_bad_args(self):
        """pass in bad values"""
        with warnings.catch_warnings(record=True) as wrn:
            bad_col = Collection(dict())
            self.assertEquals(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, CollectionWarning))
            self.assertEquals(bad_col.size(), 0)

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
        self.assertFalse(
            Collection.equal(self._canned_collection, Collection(EVENT_LIST)))

        self.assertTrue(
            Collection.same(self._canned_collection, Collection(EVENT_LIST)))

if __name__ == '__main__':
    unittest.main()
