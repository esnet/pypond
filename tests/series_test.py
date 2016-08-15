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
import json
import unittest
import warnings

from pypond.collection import Collection
from pypond.event import Event
from pypond.exceptions import (
    CollectionException,
    CollectionWarning,
    FilterException,
    PipelineIOException,
    TimeSeriesException,
)
from pypond.functions import Functions, Filters
from pypond.index import Index
from pypond.indexed_event import IndexedEvent
from pypond.range import TimeRange
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

TICKET_RANGE = dict(
    name="outages",
    columns=["timerange", "title", "esnet_ticket"],
    points=[
        [[1429673400000, 1429707600000], "BOOM", "ESNET-20080101-001"],
        [[1429673400000, 1429707600000], "BAM!", "ESNET-20080101-002"],
    ],
)

# cooked up to make sure "complex" wire formats to "in" correctly.
DATA_FLOW = dict(
    name="traffic",
    columns=["time", "direction"],
    points=[
        [1400425947000, {'in': 1, 'out': 2}],
        [1400425948000, {'in': 3, 'out': 4}],
        [1400425949000, {'in': 5, 'out': 6}],
        [1400425950000, {'in': 7, 'out': 8}]
    ]
)

SEPT_2014_DATA = dict(
    utc=False,
    name="traffic",
    columns=["time", "value"],
    points=[
        [1409529600000, 80],
        [1409533200000, 88],
        [1409536800000, 52],
        [1409540400000, 80],
        [1409544000000, 26],
        [1409547600000, 37],
        [1409551200000, 6],  # last point of of 8-31 pacific time
        [1409554800000, 32],
        [1409558400000, 69],
        [1409562000000, 21],
        [1409565600000, 6],
        [1409569200000, 54],
        [1409572800000, 88],
        [1409576400000, 41],
        [1409580000000, 35],
        [1409583600000, 43],
        [1409587200000, 84],
        [1409590800000, 32],
        [1409594400000, 41],
        [1409598000000, 57],
        [1409601600000, 27],
        [1409605200000, 50],
        [1409608800000, 13],
        [1409612400000, 63],
        [1409616000000, 58],
        [1409619600000, 80],
        [1409623200000, 59],
        [1409626800000, 96],
        [1409630400000, 2],
        [1409634000000, 20],
        [1409637600000, 64],
        [1409641200000, 7],
        [1409644800000, 50],
        [1409648400000, 88],
        [1409652000000, 34],
        [1409655600000, 31],
        [1409659200000, 16],
        [1409662800000, 38],
        [1409666400000, 94],
        [1409670000000, 78],
        [1409673600000, 86],
        [1409677200000, 13],
        [1409680800000, 34],
        [1409684400000, 29],
        [1409688000000, 48],
        [1409691600000, 80],
        [1409695200000, 30],
        [1409698800000, 15],
        [1409702400000, 62],
        [1409706000000, 66],
        [1409709600000, 44],
        [1409713200000, 94],
        [1409716800000, 78],
        [1409720400000, 29],
        [1409724000000, 21],
        [1409727600000, 4],
        [1409731200000, 83],
        [1409734800000, 15],
        [1409738400000, 89],
        [1409742000000, 53],
        [1409745600000, 70],
        [1409749200000, 41],
        [1409752800000, 47],
        [1409756400000, 30],
        [1409760000000, 68],
        [1409763600000, 89],
        [1409767200000, 29],
        [1409770800000, 17],
        [1409774400000, 38],
        [1409778000000, 67],
        [1409781600000, 75],
        [1409785200000, 89],
        [1409788800000, 47],
        [1409792400000, 82],
        [1409796000000, 33],
        [1409799600000, 67],
        [1409803200000, 93],
        [1409806800000, 86],
        [1409810400000, 97],
        [1409814000000, 19],
        [1409817600000, 19],
        [1409821200000, 31],
        [1409824800000, 56],
        [1409828400000, 19],
        [1409832000000, 43],
        [1409835600000, 29],
        [1409839200000, 72],
        [1409842800000, 27],
        [1409846400000, 21],
        [1409850000000, 88],
        [1409853600000, 18],
        [1409857200000, 30],
        [1409860800000, 46],
        [1409864400000, 34],
        [1409868000000, 31],
        [1409871600000, 20],
        [1409875200000, 45],
        [1409878800000, 17],
        [1409882400000, 24],
        [1409886000000, 84],
        [1409889600000, 6],
        [1409893200000, 91],
        [1409896800000, 82],
        [1409900400000, 71],
        [1409904000000, 97],
        [1409907600000, 43],
        [1409911200000, 38],
        [1409914800000, 1],
        [1409918400000, 71],
        [1409922000000, 50],
        [1409925600000, 19],
        [1409929200000, 19],
        [1409932800000, 86],
        [1409936400000, 65],
        [1409940000000, 93],
        [1409943600000, 35]
    ]
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

        # make sure complex/deep/nested wire format is being handled correctly.
        ts7 = TimeSeries(DATA_FLOW)
        self.assertEqual(ts7.at(0).value('direction').get('in'), 1)
        self.assertEqual(ts7.at(0).value('direction').get('out'), 2)
        self.assertEqual(ts7.at(1).value('direction').get('in'), 3)
        self.assertEqual(ts7.at(1).value('direction').get('out'), 4)

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

        # events out of order
        bad_data = copy.deepcopy(DATA)
        bad_data['points'].reverse()

        with self.assertRaises(TimeSeriesException):
            TimeSeries(bad_data)

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

    def test_cropping(self):
        """test TimeSeries.crop()"""

        crop_data = dict(
            name="star-cr5:to_anl_ip-a_v4",
            columns=["time", "in"],
            points=[
                [1400425947000, 52],
                [1400425948000, 18],
                [1400425949000, 26],
                [1400425950000, 93],
                [1400425951000, 99],
                [1400425952000, 100],
            ]
        )

        series = TimeSeries(crop_data)
        new_range = TimeRange(1400425948000, 1400425951000)
        new_series = series.crop(new_range)

        self.assertEqual(new_series.size(), 3)
        self.assertEqual(new_series.at(0).data().get('in'), 18)
        self.assertEqual(new_series.at(2).data().get('in'), 93)

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

        # at_first() and at_last() and at_time()
        self.assertTrue(Event.same(self._canned_event_series.at_first(), EVENT_LIST[0]))
        self.assertTrue(Event.same(self._canned_event_series.at_last(), EVENT_LIST[2]))

        ref_dtime = EVENT_LIST[1].timestamp() + datetime.timedelta(seconds=3)
        self.assertTrue(Event.same(self._canned_event_series.at_time(ref_dtime),
                                   EVENT_LIST[1]))

    def test_underlying_methods(self):
        """basically aliases for underlying collection methods."""

        self.assertEqual(self._canned_event_series.count(), len(EVENT_LIST))

        tser = self._canned_event_series
        self.assertEqual(tser.sum('in'), 9)
        self.assertEqual(tser.avg('out'), 4)
        self.assertEqual(tser.mean('out'), 4)
        self.assertEqual(tser.min('in'), 1)
        self.assertEqual(tser.max('in'), 5)
        self.assertEqual(tser.median('out'), 4)
        self.assertEqual(tser.stdev('out'), 1.632993161855452)
        # redundant, but for coverage
        self.assertEqual(tser.aggregate(Functions.sum(), 'in'), 9)
        self.assertEqual(tser.aggregate(Functions.sum(), ('in',)), 9)

        ser1 = TimeSeries(DATA)
        self.assertEqual(ser1.aggregate(Functions.sum()), 189)

    def test_various_bad_args(self):
        """ensure proper exceptions are being raised."""

        ser1 = TimeSeries(DATA)

        with self.assertRaises(CollectionException):
            ser1.aggregate(dict())

        with self.assertRaises(CollectionException):
            ser1.aggregate(Functions.sum(), dict())

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

        t_merged = TimeSeries.timeseries_list_merge(dict(name='traffic'), [t_in, t_out])

        self.assertEqual(t_merged.at(2).get('in'), 26)
        self.assertEqual(t_merged.at(2).get('out'), 67)

        t_summed = TimeSeries.timeseries_list_sum(
            dict(name='traffic'), [t_in, t_in], 'in')

        self.assertEqual(t_summed.at(0).get('in'), 104)
        self.assertEqual(t_summed.at(1).get('in'), 36)

        # more variations for coverage

        test_idx_data = dict(
            name="availability",
            columns=["index", "uptime"],
            points=[
                ["2014-07", 100],
                ["2014-08", 88],
                ["2014-09", 95],
                ["2014-10", 99],
                ["2014-11", 91],
                ["2014-12", 99],
                ["2015-01", 100],
                ["2015-02", 92],
                ["2015-03", 99],
                ["2015-04", 87],
                ["2015-05", 92],
                ["2015-06", 100],
            ]
        )

        t_idx = TimeSeries(test_idx_data)
        idx_sum = TimeSeries.timeseries_list_sum(
            dict(name='available'), [t_idx, t_idx], 'uptime')
        self.assertEqual(idx_sum.at(0).get('uptime'), 200)
        self.assertEqual(idx_sum.at(1).get('uptime'), 176)
        self.assertEqual(idx_sum.at(2).get('uptime'), 190)

        test_outage = dict(
            name="outages",
            columns=["timerange", "length", "esnet_ticket"],
            points=[
                [[1429673400000, 1429707600000], 23, "ESNET-20080101-001"],
                [[1429673500000, 1429707700000], 54, "ESNET-20080101-002"],
            ],
        )

        t_tr = TimeSeries(test_outage)
        tr_sum = TimeSeries.timeseries_list_sum(
            dict(name='outage length'), [t_tr, t_tr], 'length')
        self.assertEqual(tr_sum.at(0).get('length'), 46)
        self.assertEqual(tr_sum.at(1).get('length'), 108)

    def test_map_and_collect(self):
        """test timeseries access methods for coverage."""

        # map
        def in_only(event):
            """make new events wtin only data in - same as .select() basically."""
            return Event(event.timestamp(), {'in': event.get('in')})
        mapped = self._canned_event_series.map(in_only)
        self.assertEqual(mapped.count(), 3)
        for i in mapped.events():
            self.assertIsNone(i.get('out'))

        # select
        selected = self._canned_event_series.select('out')
        self.assertEqual(selected.count(), 3)
        for i in selected.events():
            self.assertIsNone(i.get('in'))

    def test_ts_collapse(self):
        """
        Test TimeSeries.collapse()
        """
        ces = self._canned_event_series

        collapsed_ces = ces.collapse(['in', 'out'], 'in_out_sum', Functions.sum())

        for i in collapsed_ces.events():
            self.assertEqual(i.get('in') + i.get('out'), i.get('in_out_sum'))

    def test_aggregation_filtering(self):
        """test the filtering modifers to the agg functions."""

        event_objects = [
            Event(1429673400000, {'in': 1, 'out': 2}),
            Event(1429673460000, {'in': 3, 'out': None}),
            Event(1429673520000, {'in': 5, 'out': 6}),
        ]

        series = TimeSeries(dict(name='events', events=event_objects))

        self.assertEqual(series.sum('out', Filters.ignore_missing), 8)
        self.assertEqual(series.avg('out', Filters.ignore_missing), 4)
        self.assertEqual(series.min('out', Filters.zero_missing), 0)
        self.assertEqual(series.max('out', Filters.propogate_missing), None)
        self.assertEqual(series.mean('out', Filters.ignore_missing), 4)
        self.assertEqual(series.median('out', Filters.zero_missing), 2)
        self.assertEqual(series.stdev('out', Filters.zero_missing), 2.494438257849294)

        def bad_filtering_function():  # pylint: disable=missing-docstring
            pass

        with self.assertRaises(FilterException):
            series.sum('out', bad_filtering_function)


class TestRollups(SeriesBase):
    """
    Tests for the rollup methods
    """

    def test_fixed_window(self):
        """Test fixed window rollup"""

        timeseries = TimeSeries(SEPT_2014_DATA)

        daily_avg = timeseries.fixed_window_rollup('1d', dict(value=Functions.avg()))

        self.assertEqual(daily_avg.size(), 5)
        self.assertEqual(daily_avg.at(0).value(), 46.875)
        self.assertEqual(daily_avg.at(2).value(), 54.083333333333336)
        self.assertEqual(daily_avg.at(4).value(), 51.85)

        # not really a rollup, each data point will create one
        # aggregation index.

        timeseries = TimeSeries(SEPT_2014_DATA)

        hourly_avg = timeseries.hourly_rollup(dict(value=Functions.avg()))

        self.assertEqual(hourly_avg.size(), len(SEPT_2014_DATA.get('points')))
        self.assertEqual(hourly_avg.at(0).value(), 80.0)
        self.assertEqual(hourly_avg.at(2).value(), 52.0)
        self.assertEqual(hourly_avg.at(4).value(), 26.0)

    def test_fixed_window_collect(self):
        """Make collections for each day in the timeseries."""

        timeseries = TimeSeries(SEPT_2014_DATA)
        colls = timeseries.collect_by_fixed_window('1d')

        self.assertEqual(colls.get('1d-16314').size(), 24)
        self.assertEqual(colls.get('1d-16318').size(), 20)

    def test_non_fixed_rollups(self):
        """Work the calendar rollup logic / utc / etc."""

        timeseries = TimeSeries(SEPT_2014_DATA)

        # just silence the warnings, not do anything with them.
        with warnings.catch_warnings(record=True):

            daily_avg = timeseries.daily_rollup(dict(value=Functions.avg()))

            ts_1 = SEPT_2014_DATA.get('points')[0][0]

            self.assertEqual(
                Index.get_daily_index_string(dt_from_ms(ts_1), utc=False),
                daily_avg.at(0).index().to_string()
            )

            monthly_avg = timeseries.monthly_rollup(dict(value=Functions.avg()))

            self.assertEqual(
                Index.get_monthly_index_string(dt_from_ms(ts_1), utc=False),
                monthly_avg.at(0).index().to_string()
            )

            yearly_avg = timeseries.yearly_rollup(dict(value=Functions.avg()))

            self.assertEqual(
                Index.get_yearly_index_string(dt_from_ms(ts_1), utc=False),
                yearly_avg.at(0).index().to_string()
            )


class TestPercentileAndQuantile(SeriesBase):
    """
    Test the percentile and quantile operations.
    """

    def test_percentile(self):
        """Test percentile of a series."""

        series = TimeSeries(dict(
            name="Sensor values",
            columns=["time", "temperature"],
            points=[
                [1400425951000, 22.3],
                [1400425952000, 32.4],
                [1400425953000, 12.1],
                [1400425955000, 76.8],
                [1400425956000, 87.3],
                [1400425957000, 54.6],
                [1400425958000, 45.5],
                [1400425959000, 87.9]
            ]
        ))

        self.assertEqual(series.percentile(50, 'temperature'), 50.05)
        self.assertEqual(series.percentile(95, 'temperature'), 87.69)
        self.assertEqual(series.percentile(99, 'temperature'), 87.858)

        self.assertEqual(series.percentile(99, 'temperature', 'lower'), 87.3)
        self.assertEqual(series.percentile(99, 'temperature', 'higher'), 87.9)
        self.assertEqual(series.percentile(99, 'temperature', 'nearest'), 87.9)
        self.assertEqual(series.percentile(99, 'temperature', 'midpoint'), 87.6)

        self.assertEqual(series.percentile(0, 'temperature'), 12.1)
        self.assertEqual(series.percentile(100, 'temperature'), 87.9)

    def test_percentile_empty(self):
        """percentile of an empty timeseries."""

        series = TimeSeries(dict(
            name="Sensor values",
            columns=["time", "temperature"],
            points=[
            ]
        ))

        self.assertIsNone(series.percentile(0, 'temperature'))
        self.assertIsNone(series.percentile(100, 'temperature'))

    def test_percentile_single(self):
        """percentile of an timeseries with one point."""

        series = TimeSeries(dict(
            name="Sensor values",
            columns=["time", "temperature"],
            points=[
                [1400425951000, 22.3]
            ]
        ))

        self.assertEqual(series.percentile(0, 'temperature'), 22.3)
        self.assertEqual(series.percentile(50, 'temperature'), 22.3)
        self.assertEqual(series.percentile(100, 'temperature'), 22.3)

    def test_quantile(self):
        """test TimeSeries.quantile()"""

        series = TimeSeries(dict(
            name="Sensor values",
            columns=["time", "temperature"],
            points=[
                [1400425951000, 22.3],
                [1400425952000, 32.4],
                [1400425953000, 12.1],
                [1400425955000, 76.8],
                [1400425956000, 87.3],
                [1400425957000, 54.6],
                [1400425958000, 45.5],
                [1400425959000, 87.9]
            ]
        ))

        self.assertEqual(
            series.quantile(4, field_path='temperature'), [29.875, 50.05, 79.425])
        self.assertEqual(
            series.quantile(4, field_path='temperature', method='linear'),
            [29.875, 50.05, 79.425])
        self.assertEqual(
            series.quantile(4, field_path='temperature', method='lower'),
            [22.3, 45.5, 76.8])
        self.assertEqual(
            series.quantile(4, field_path='temperature', method='higher'),
            [32.4, 54.6, 87.3])
        self.assertEqual(
            series.quantile(4, field_path='temperature', method='nearest'),
            [32.4, 54.6, 76.8])
        self.assertEqual(
            series.quantile(4, field_path='temperature', method='midpoint'),
            [27.35, 50.05, 82.05])

        self.assertEqual(series.quantile(1, 'temperature', 'linear'), [])


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

        self.assertEqual(len(json.loads(col.to_string())), 3)

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

        collapsed_col = col.collapse(['in', 'out'], 'in_out_sum', Functions.sum())
        self.assertEqual(collapsed_col.size(), 3)

        for i in collapsed_col.events():
            self.assertEqual(len(list(i.data().keys())), 3)
            self.assertEqual(i.get('in') + i.get('out'), i.get('in_out_sum'))

    def test_aggregations(self):
        """sum, min, max, etc.
        """

        col = self._canned_collection
        self.assertEqual(col.sum('in'), 9)
        self.assertEqual(col.avg('out'), 4)
        self.assertEqual(col.mean('out'), 4)
        self.assertEqual(col.min('in'), 1)
        self.assertEqual(col.max('in'), 5)
        self.assertEqual(col.first('out'), 2)
        self.assertEqual(col.last('out'), 6)
        self.assertEqual(col.median('out'), 4)
        self.assertEqual(col.stdev('out'), 1.632993161855452)

    def test_aggregation_filtering(self):
        """Test the new filtering methods for cleaning stuff."""

        elist = [
            Event(1429673400000, {'in': 1, 'out': 1}),
            Event(1429673460000, {'in': 2, 'out': 5}),
            Event(1429673520000, {'in': 3, 'out': None}),
        ]

        coll = Collection(elist)

        self.assertEqual(coll.aggregate(Functions.sum(), 'in'), 6)

        self.assertEqual(coll.aggregate(Functions.sum(Filters.propogate_missing), 'in'), 6)
        self.assertEqual(coll.aggregate(Functions.sum(Filters.propogate_missing), 'out'), None)

        self.assertEqual(coll.aggregate(Functions.avg(Filters.ignore_missing), 'in'), 2)
        self.assertEqual(coll.aggregate(Functions.avg(Filters.ignore_missing), 'out'), 3)

        self.assertEqual(coll.aggregate(Functions.avg(Filters.zero_missing), 'in'), 2)
        self.assertEqual(coll.aggregate(Functions.avg(Filters.zero_missing), 'out'), 2)

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
        self.assertEqual(new_coll.stdev('in'), 2.23606797749979)
        self.assertEqual(new_coll.median('in'), 4)

    def test_bad_args(self):
        """pass in bad values"""
        with warnings.catch_warnings(record=True) as wrn:
            bad_col = Collection(dict())
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, CollectionWarning))
            self.assertEqual(bad_col.size(), 0)

        with self.assertRaises(CollectionException):
            self._canned_collection.set_events(dict())

    def test_sort_by_time(self):
        """test Collection.sort_by_time()"""

        reversed_events = [EVENT_LIST[2], EVENT_LIST[1], EVENT_LIST[0]]

        bad_order = Collection(reversed_events)
        self.assertNotEqual(bad_order.event_list_as_list(), EVENT_LIST)
        self.assertFalse(bad_order.is_chronological())

        good_order = bad_order.sort_by_time()

        self.assertEqual(good_order.event_list_as_list(), EVENT_LIST)
        self.assertTrue(good_order.is_chronological())

    def test_other_exceptions(self):
        """trigger other exceptions"""
        with self.assertRaises(PipelineIOException):
            self._canned_collection.start()

        with self.assertRaises(PipelineIOException):
            self._canned_collection.stop()

        with self.assertRaises(PipelineIOException):
            self._canned_collection.on_emit()

        with self.assertRaises(PipelineIOException):
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
