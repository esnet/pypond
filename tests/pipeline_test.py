"""
Tests for the pipeline.
"""

# sorry pylint, unit tests get long
# pylint: disable=too-many-lines

import datetime
import unittest

import pytz

from pypond.event import Event
from pypond.functions import Functions
from pypond.indexed_event import IndexedEvent
from pypond.pipeline import Pipeline
from pypond.pipeline_in import UnboundedIn
from pypond.pipeline_out import CollectionOut, EventOut
from pypond.range import TimeRange
from pypond.series import TimeSeries
from pypond.timerange_event import TimeRangeEvent
from pypond.util import aware_dt_from_args, dt_from_ms, ms_from_dt

# global variables for the callbacks to write to.
# they are alwasy reset to None by setUp()

RESULTS = None
RESULTS2 = None

# sample data

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


def _strp(dstr):
    """decode some existing test ts strings from js tests."""
    fmt = '%Y-%m-%dT%H:%M:%SZ'
    return datetime.datetime.strptime(dstr, fmt).replace(tzinfo=pytz.UTC)

EVENTLIST1 = [
    Event(_strp("2015-04-22T03:30:00Z"), {'in': 1, 'out': 2}),
    Event(_strp("2015-04-22T03:31:00Z"), {'in': 3, 'out': 4}),
    Event(_strp("2015-04-22T03:32:00Z"), {'in': 5, 'out': 6}),
]

SEPT_2014_DATA = dict(
    name="traffic",
    columns=["time", "value"],
    points=[
        [1409529600000, 80],
        [1409533200000, 88],
        [1409536800000, 52],
        [1409540400000, 80],
        [1409544000000, 26],
        [1409547600000, 37],
        [1409551200000, 6],
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

IN_OUT_DATA = dict(
    name="traffic",
    columns=["time", "in", "out", "perpendicular"],
    points=[
        [1409529600000, 80, 37, 1000],
        [1409533200000, 88, 22, 1001],
        [1409536800000, 52, 56, 1002]
    ]
)

DEEP_EVENT_LIST = [
    Event(1429673400000, {'direction': {'status': 'OK', 'in': 1, 'out': 2}}),
    Event(1429673460000, {'direction': {'status': 'OK', 'in': 3, 'out': 4}}),
    Event(1429673520000, {'direction': {'status': 'FAIL', 'in': 0, 'out': 0}}),
    Event(1429673580000, {'direction': {'status': 'OK', 'in': 8, 'out': 0}})
]


class BaseTestPipeline(unittest.TestCase):
    """
    Base class for the pipeline tests.
    """

    def setUp(self):
        """
        Common setup stuff.
        """
        # self._void_pipeline = Pipeline()
        global RESULTS, RESULTS2  # pylint: disable=global-statement
        RESULTS = RESULTS2 = None


class TestMapCollapseSelect(BaseTestPipeline):
    """
    Test the map(), collapse() and select() methods that have now been
    turned into pipeline operations.
    """

    def test_map(self):
        """test .map()"""

        def mapper(event):
            """swap in and out."""
            return event.set_data({'in': event.get('out'), 'out': event.get('in')})

        timeseries = TimeSeries(IN_OUT_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries.collection())
            .map(mapper)
            .emit_on('flush')
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('all').at(0).get('in'), 37)
        self.assertEqual(kcol.get('all').at(0).get('out'), 80)

    def test_simple_collapse(self):
        """collapse a subset of columns."""
        timeseries = TimeSeries(IN_OUT_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries)
            .collapse(['in', 'out'], 'in_out_sum', Functions.sum)
            .emit_on('flush')
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('all').at(0).get('in_out_sum'), 117)
        self.assertEqual(kcol.get('all').at(1).get('in_out_sum'), 110)
        self.assertEqual(kcol.get('all').at(2).get('in_out_sum'), 108)

    def test_multiple_collapse_chains(self):
        """multiple collapsers."""
        timeseries = TimeSeries(IN_OUT_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries)
            .collapse(['in', 'out'], 'in_out_sum', Functions.sum)
            .collapse(['in', 'out'], 'in_out_max', Functions.max)
            .emit_on('flush')
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('all').at(0).get('in_out_sum'), 117)
        self.assertEqual(kcol.get('all').at(1).get('in_out_sum'), 110)
        self.assertEqual(kcol.get('all').at(2).get('in_out_sum'), 108)

        self.assertEqual(kcol.get('all').at(0).get('in_out_max'), 80)
        self.assertEqual(kcol.get('all').at(1).get('in_out_max'), 88)
        self.assertEqual(kcol.get('all').at(2).get('in_out_max'), 56)

    def test_single_select(self):
        """select a single column."""
        timeseries = TimeSeries(IN_OUT_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries)
            .select('in')
            .to_keyed_collections()
        )

        new_ts = TimeSeries(dict(name='new_timeseries', collection=kcol.get('all')))

        self.assertEqual(new_ts.columns(), ['in'])

    def test_subset_select(self):
        """select multiple columns."""
        timeseries = TimeSeries(IN_OUT_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries)
            .select(['out', 'perpendicular'])
            .to_keyed_collections()
        )

        new_ts = TimeSeries(dict(name='new_timeseries', collection=kcol.get('all')))

        self.assertEqual(set(new_ts.columns()), set(['out', 'perpendicular']))


class TestFilterAndTake(BaseTestPipeline):
    """
    Tests for filter and take operations to sort events.
    """

    def test_simple_filter(self):
        """filter events in a batch."""

        def filter_cb(event):
            """filter callback"""
            return event.value() > 65

        timeseries = TimeSeries(SEPT_2014_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries)
            .filter(filter_cb)
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('all').size(), 39)

    def test_simple_take(self):
        """take 10 events in batch."""

        timeseries = TimeSeries(SEPT_2014_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries)
            .take(10)
            .to_keyed_collections()
        )

        new_ts = TimeSeries(dict(name='result', collection=kcol.get('all')))
        self.assertEqual(new_ts.size(), 10)

    def test_filter_and_take_chain(self):
        """filter events, then apply take"""

        def filter_cb(event):
            """filter callback"""
            return event.value() > 65

        timeseries = TimeSeries(SEPT_2014_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries)
            .filter(filter_cb)
            .take(10)
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('all').size(), 10)
        self.assertEqual(kcol.get('all').at(0).value(), 80)
        self.assertEqual(kcol.get('all').at(1).value(), 88)
        self.assertEqual(kcol.get('all').at(8).value(), 88)
        self.assertEqual(kcol.get('all').at(9).value(), 94)

    def test_take_and_group_by(self):
        """take events with different group by keys."""

        def gb_callback(event):
            """group into two groups."""
            return 'high' if event.value() > 65 else 'low'

        timeseries = TimeSeries(SEPT_2014_DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries)
            .emit_on('flush')
            .group_by(gb_callback)
            .take(10)
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('low').size(), 10)

        self.assertEqual(kcol.get('low').at(0).value(), 52)
        self.assertEqual(kcol.get('low').at(1).value(), 26)

        self.assertEqual(kcol.get('high').size(), 10)

        self.assertEqual(kcol.get('high').at(0).value(), 80)
        self.assertEqual(kcol.get('high').at(1).value(), 88)
        self.assertEqual(kcol.get('high').at(8).value(), 88)
        self.assertEqual(kcol.get('high').at(9).value(), 94)

    def test_group_by_variants(self):
        """test group by with strings and arrays."""

        data = dict(
            name="traffic",
            columns=["time", "value", "status"],
            points=[
                [1400425947000, 52, "ok"],
                [1400425948000, 18, "ok"],
                [1400425949000, 26, "fail"],
                [1400425950000, 93, "offline"]
            ]
        )

        # group on a single column with string input to group_by

        kcol = (
            Pipeline()
            .from_source(TimeSeries(data))
            .emit_on('flush')
            .group_by('status')
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('ok').size(), 2)
        self.assertEqual(kcol.get('fail').size(), 1)
        self.assertEqual(kcol.get('offline').size(), 1)

        # group on a deep/nested column with an array arg

        kcol = (
            Pipeline()
            .from_source(TimeSeries(dict(name='events', events=DEEP_EVENT_LIST)))
            .emit_on('flush')
            .group_by(['direction', 'status'])
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('OK').size(), 3)
        self.assertEqual(kcol.get('FAIL').size(), 1)
        self.assertEqual(kcol.get('OK').at(0).value('direction').get('out'), 2)
        self.assertEqual(kcol.get('OK').at(1).value('direction').get('in'), 3)
        self.assertEqual(kcol.get('FAIL').at(0).value('direction').get('out'), 0)

        # same thing but with the old.school.style

        kcol = (
            Pipeline()
            .from_source(TimeSeries(dict(name='events', events=DEEP_EVENT_LIST)))
            .emit_on('flush')
            .group_by('direction.status')
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('OK').size(), 3)
        self.assertEqual(kcol.get('FAIL').size(), 1)
        self.assertEqual(kcol.get('OK').at(0).value('direction').get('out'), 2)
        self.assertEqual(kcol.get('OK').at(1).value('direction').get('in'), 3)
        self.assertEqual(kcol.get('FAIL').at(0).value('direction').get('out'), 0)

        # and with a tuple

        kcol = (
            Pipeline()
            .from_source(TimeSeries(dict(name='events', events=DEEP_EVENT_LIST)))
            .emit_on('flush')
            .group_by(('direction', 'status',))
            .to_keyed_collections()
        )

        self.assertEqual(kcol.get('OK').size(), 3)
        self.assertEqual(kcol.get('FAIL').size(), 1)
        self.assertEqual(kcol.get('OK').at(0).value('direction').get('out'), 2)
        self.assertEqual(kcol.get('OK').at(1).value('direction').get('in'), 3)
        self.assertEqual(kcol.get('FAIL').at(0).value('direction').get('out'), 0)

    def test_group_by_and_count(self):
        """group by and also count."""

        timeseries = TimeSeries(SEPT_2014_DATA)

        # pylint: disable=missing-docstring

        def gb_callback(event):
            """group into two groups."""
            return 'high' if event.value() > 65 else 'low'

        def cback(count, window_key, group_by):  # pylint: disable=unused-argument
            """callback to pass in."""
            global RESULTS  # pylint: disable=global-statement
            if RESULTS is None:
                RESULTS = dict()
            RESULTS[group_by] = count

        (
            Pipeline()
            .from_source(timeseries)
            .take(10)
            .group_by(gb_callback)
            .emit_on('flush')
            .count(cback)
        )

        self.assertEqual(RESULTS.get('high'), 4)
        self.assertEqual(RESULTS.get('low'), 6)


class TestAggregator(BaseTestPipeline):
    """
    Tests for the aggregator.
    """

    def test_sum_and_find_max(self):
        """sum elements, find max get result out."""

        def cback(event):
            """catch the return"""
            self.assertEqual(event.get('total'), 117)

        timeseries = TimeSeries(IN_OUT_DATA)

        (
            Pipeline()
            .from_source(timeseries)
            .emit_on('flush')
            .collapse(['in', 'out'], 'total', Functions.sum)
            .aggregate(dict(total=Functions.max))
            .to(EventOut, cback)
        )

        # Same test but as an event list

        elist = (
            Pipeline()
            .from_source(timeseries)
            .emit_on('flush')
            .collapse(['in', 'out'], 'total', Functions.sum)
            .aggregate(dict(total=Functions.max))
            .to_event_list()
        )

        self.assertEqual(len(elist), 1)
        self.assertEqual(elist[0].get('total'), 117)

    def test_aggregate_deep_path(self):
        """Make sure that the aggregator will work on a deep path."""

        elist = (
            Pipeline()
            .from_source(TimeSeries(dict(name='events', events=DEEP_EVENT_LIST)))
            .emit_on('flush')
            .aggregate({'direction.out': Functions.max})
            .to_event_list()
        )

        self.assertEqual(elist[0].get('out'), 4)

        # Make sure it works with the the non-string version to aggregate
        # multiple columns

        elist = (
            Pipeline()
            .from_source(TimeSeries(dict(name='events', events=DEEP_EVENT_LIST)))
            .emit_on('flush')
            .aggregate({('direction.out', 'direction.in'): Functions.max})
            .to_event_list()
        )

        self.assertEqual(elist[0].get('out'), 4)
        self.assertEqual(elist[0].get('in'), 8)

    def test_windowed_average(self):
        """aggregate events into by windowed avg."""
        events_in = [
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=7, minute=57)),
                {'in': 3, 'out': 1}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=7, minute=58)),
                {'in': 9, 'out': 2}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=7, minute=59)),
                {'in': 6, 'out': 6}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=8, minute=0)),
                {'in': 4, 'out': 7}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=8, minute=1)),
                {'in': 5, 'out': 9}
            ),
        ]

        def cback(event):
            """callback to pass in."""
            global RESULTS  # pylint: disable=global-statement
            if RESULTS is None:
                RESULTS = dict()
            RESULTS['{0}'.format(event.index())] = event

        uin = UnboundedIn()

        (
            Pipeline()
            .from_source(uin)
            .window_by('1h')
            .emit_on('eachEvent')
            .aggregate({'in': Functions.avg, 'out': Functions.avg})
            .to(EventOut, cback)
        )

        for i in events_in:
            uin.add_event(i)

        self.assertEqual(RESULTS.get('1h-396199').get('in'), 6)
        self.assertEqual(RESULTS.get('1h-396199').get('out'), 3)
        self.assertEqual(RESULTS.get('1h-396200').get('in'), 4.5)
        self.assertEqual(RESULTS.get('1h-396200').get('out'), 8)

    def test_collect_and_aggregate(self):
        """collect events together and aggregate."""
        events_in = [
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=7, minute=57)),
                {'type': 'a', 'in': 3, 'out': 1}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=7, minute=58)),
                {'type': 'a', 'in': 9, 'out': 2}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=7, minute=59)),
                {'type': 'b', 'in': 6, 'out': 6}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=8, minute=0)),
                {'type': 'a', 'in': 4, 'out': 7}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=8, minute=1)),
                {'type': 'b', 'in': 5, 'out': 9}
            ),
        ]

        def cback(event):
            """callback to pass in."""
            global RESULTS  # pylint: disable=global-statement
            if RESULTS is None:
                RESULTS = dict()
            RESULTS['{0}:{1}'.format(event.index(), event.get('type'))] = event

        uin = UnboundedIn()

        (
            Pipeline()
            .from_source(uin)
            .group_by('type')
            .window_by('1h')
            .emit_on('eachEvent')
            .aggregate({'type': Functions.keep, 'in': Functions.avg, 'out': Functions.avg})
            .to(EventOut, cback)
        )

        for i in events_in:
            uin.add_event(i)

        self.assertEqual(RESULTS.get('1h-396199:a').get('in'), 6)
        self.assertEqual(RESULTS.get('1h-396199:a').get('out'), 1.5)
        self.assertEqual(RESULTS.get('1h-396199:b').get('in'), 6)
        self.assertEqual(RESULTS.get('1h-396199:b').get('out'), 6)
        self.assertEqual(RESULTS.get('1h-396200:a').get('in'), 4)
        self.assertEqual(RESULTS.get('1h-396200:a').get('out'), 7)
        self.assertEqual(RESULTS.get('1h-396200:b').get('in'), 5)
        self.assertEqual(RESULTS.get('1h-396200:b').get('out'), 9)

    def test_aggregate_and_conversion(self):
        """Aggregate/average and convert to TimeRangeEvent."""

        events_in = [
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=1, minute=57)),
                {'in': 3, 'out': 1}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=1, minute=58)),
                {'in': 9, 'out': 2}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=1, minute=59)),
                {'in': 6, 'out': 6}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=2, minute=0)),
                {'in': 4, 'out': 7}
            ),
            Event(
                aware_dt_from_args(dict(year=2015, month=3, day=14, hour=2, minute=1)),
                {'in': 5, 'out': 9}
            ),
        ]

        def cback(event):
            """callback to pass in."""
            global RESULTS  # pylint: disable=global-statement
            if RESULTS is None:
                RESULTS = dict()
            RESULTS['{0}'.format(ms_from_dt(event.timestamp()))] = event

        uin = UnboundedIn()

        (
            Pipeline()
            .from_source(uin)
            .window_by('1h')
            .emit_on('eachEvent')
            .aggregate({'in': Functions.avg, 'out': Functions.avg})
            .as_time_range_events(dict(alignment='lag'))
            .to(EventOut, cback)
        )

        for i in events_in:
            uin.add_event(i)

        self.assertEqual(RESULTS.get('1426294800000').get('in'), 6)
        self.assertEqual(RESULTS.get('1426294800000').get('out'), 3)

        self.assertEqual(RESULTS.get('1426298400000').get('in'), 4.5)
        self.assertEqual(RESULTS.get('1426298400000').get('out'), 8)


class TestConverter(BaseTestPipeline):
    """
    Tests for the Converter processor
    """

    def setUp(self):
        super(TestConverter, self).setUp()

        self._event = Event(dt_from_ms(1426316400000), 3)
        self._tre = TimeRangeEvent(TimeRange([1426316400000, 1426320000000]), 3)
        self._idxe = IndexedEvent("1h-396199", 3)

    def test_event_to_tre_conversion(self):
        """test converting Event objects to TimeRangeEvent."""

        # pylint: disable=missing-docstring

        stream1 = UnboundedIn()

        def cback1(event):
            self.assertEqual(ms_from_dt(event.begin()), 1426316400000)
            self.assertEqual(ms_from_dt(event.end()), 1426320000000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream1)
            .as_time_range_events(dict(alignment='front', duration='1h'))
            .to(EventOut, cback1)
        )

        stream1.add_event(self._event)

        stream2 = UnboundedIn()

        def cback2(event):
            self.assertEqual(ms_from_dt(event.begin()), 1426314600000)
            self.assertEqual(ms_from_dt(event.end()), 1426318200000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream2)
            .as_time_range_events(dict(alignment='center', duration='1h'))
            .to(EventOut, cback2)
        )

        stream2.add_event(self._event)

        stream3 = UnboundedIn()

        def cback3(event):
            self.assertEqual(ms_from_dt(event.begin()), 1426312800000)
            self.assertEqual(ms_from_dt(event.end()), 1426316400000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream3)
            .as_time_range_events(dict(alignment='behind', duration='1h'))
            .to(EventOut, cback3)
        )

        stream3.add_event(self._event)

    def test_event_to_idxe_conversion(self):
        """Test converting Event object to IndexedEvent."""

        # pylint: disable=missing-docstring

        stream1 = UnboundedIn()

        def cback1(event):
            self.assertEqual(event.index_as_string(), '1h-396199')
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream1)
            .as_indexed_events(dict(duration='1h'))
            .to(EventOut, cback1)
        )

        stream1.add_event(self._event)

    def test_event_to_event_noop(self):
        """Event to Event as a noop."""

        stream1 = UnboundedIn()

        def cback1(event):  # pylint: disable=missing-docstring
            self.assertEqual(event, self._event)

        (
            Pipeline()
            .from_source(stream1)
            .as_events()
            .to(EventOut, cback1)
        )

        stream1.add_event(self._event)

    def test_tre_to_event(self):
        """TimeRangeEvent to Event conversion."""

        stream1 = UnboundedIn()

        # pylint: disable=missing-docstring

        def cback1(event):
            self.assertEqual(ms_from_dt(event.timestamp()), 1426318200000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream1)
            .as_events(dict(alignment='center'))
            .to(EventOut, cback1)
        )

        stream1.add_event(self._tre)

        stream2 = UnboundedIn()

        def cback2(event):
            self.assertEqual(ms_from_dt(event.timestamp()), 1426316400000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream2)
            .as_events(dict(alignment='lag'))
            .to(EventOut, cback2)
        )

        stream2.add_event(self._tre)

        stream3 = UnboundedIn()

        def cback3(event):
            self.assertEqual(ms_from_dt(event.timestamp()), 1426320000000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream3)
            .as_events(dict(alignment='lead'))
            .to(EventOut, cback3)
        )

        stream3.add_event(self._tre)

    def test_tre_to_tre_noop(self):
        """TimeRangeEvent -> TimeRangeEvent noop."""

        stream1 = UnboundedIn()

        def cback1(event):  # pylint: disable=missing-docstring
            self.assertEqual(event, self._tre)

        (
            Pipeline()
            .from_source(stream1)
            .as_time_range_events()
            .to(EventOut, cback1)
        )

        stream1.add_event(self._tre)

    def test_idxe_to_event(self):
        """IndexedEvent -> Event conversion."""

        stream1 = UnboundedIn()

        # pylint: disable=missing-docstring

        def cback1(event):
            self.assertEqual(ms_from_dt(event.timestamp()), 1426318200000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream1)
            .as_events(dict(alignment='center'))
            .to(EventOut, cback1)
        )

        stream1.add_event(self._idxe)

        stream2 = UnboundedIn()

        def cback2(event):
            self.assertEqual(ms_from_dt(event.timestamp()), 1426316400000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream2)
            .as_events(dict(alignment='lag'))
            .to(EventOut, cback2)
        )

        stream2.add_event(self._idxe)

        stream3 = UnboundedIn()

        def cback3(event):
            self.assertEqual(ms_from_dt(event.timestamp()), 1426320000000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream3)
            .as_events(dict(alignment='lead'))
            .to(EventOut, cback3)
        )

        stream3.add_event(self._idxe)

    def test_idxe_to_tre(self):
        """IndexedEvent -> TimeRangeEvent conversion."""

        stream1 = UnboundedIn()

        def cback1(event):  # pylint: disable=missing-docstring
            self.assertEqual(ms_from_dt(event.begin()), 1426316400000)
            self.assertEqual(ms_from_dt(event.end()), 1426320000000)
            self.assertEqual(event.get(), 3)

        (
            Pipeline()
            .from_source(stream1)
            .as_time_range_events()
            .to(EventOut, cback1)
        )

        stream1.add_event(self._idxe)

    def test_idxe_to_idxe_noop(self):
        """IndexedEvent -> IndexedEvent noop."""

        stream1 = UnboundedIn()

        def cback1(event):  # pylint: disable=missing-docstring
            self.assertEqual(event, self._idxe)

        (
            Pipeline()
            .from_source(stream1)
            .as_indexed_events()
            .to(EventOut, cback1)
        )

        stream1.add_event(self._idxe)


class TestOffsetPipeline(BaseTestPipeline):
    """
    Tests for the offset pipeline operations. This is a simple processor
    mostly for pipeline testing.
    """

    def test_simple_offset_chain(self):
        """test a simple offset chain."""
        timeseries = TimeSeries(DATA)

        kcol = (
            Pipeline()
            .from_source(timeseries.collection())
            .offset_by(1, 'value')
            .offset_by(2)
            .to_keyed_collections()
        )

        self.assertEqual(kcol['all'].at(0).get(), 55)
        self.assertEqual(kcol['all'].at(1).get(), 21)
        self.assertEqual(kcol['all'].at(2).get(), 29)
        self.assertEqual(kcol['all'].at(3).get(), 96)

    def test_ts_offset_chain(self):
        """test running the offset chain directly from the TimeSeries."""
        timeseries = TimeSeries(DATA)

        kcol = (
            timeseries.pipeline()
            .offset_by(1, 'value')
            .offset_by(2)
            .to_keyed_collections()
        )
        self.assertEqual(kcol['all'].at(0).get(), 55)
        self.assertEqual(kcol['all'].at(1).get(), 21)
        self.assertEqual(kcol['all'].at(2).get(), 29)
        self.assertEqual(kcol['all'].at(3).get(), 96)

    def test_callback_offset_chain(self):
        """pass a callback in rather than retrieving a keyed collection."""

        def cback(collection, window_key, group_by):  # pylint: disable=unused-argument
            """callback to pass in."""
            global RESULTS  # pylint: disable=global-statement
            RESULTS = collection

        timeseries = TimeSeries(DATA)

        (
            Pipeline()
            .from_source(timeseries.collection())
            .offset_by(1, 'value')
            .offset_by(2)
            .to(CollectionOut, cback)
        )

        # Spurious lint error due to upstream tinkering
        # with the global variable
        # pylint: disable=no-member

        self.assertEqual(RESULTS.at(0).get(), 55)
        self.assertEqual(RESULTS.at(1).get(), 21)
        self.assertEqual(RESULTS.at(2).get(), 29)
        self.assertEqual(RESULTS.at(3).get(), 96)

    def test_streaming_multiple_chains(self):
        """streaming events with two pipelines."""

        def cback(collection, window_key, group_by):  # pylint: disable=unused-argument
            """callback to pass in."""
            global RESULTS  # pylint: disable=global-statement
            RESULTS = collection

        def cback2(collection, window_key, group_by):  # pylint: disable=unused-argument
            """callback to pass in."""
            global RESULTS2  # pylint: disable=global-statement
            RESULTS2 = collection

        source = UnboundedIn()

        pip1 = (
            Pipeline()
            .from_source(source)
            .offset_by(1, 'in')
            .offset_by(2, 'in')
            .to(CollectionOut, cback)
        )

        pip1.offset_by(3, 'in').to(CollectionOut, cback2)

        source.add_event(EVENTLIST1[0])

        # Spurious lint error due to upstream tinkering
        # with the global variable
        # pylint: disable=no-member

        self.assertEqual(RESULTS.size(), 1)
        self.assertEqual(RESULTS2.size(), 1)

        self.assertEqual(RESULTS.at(0).get('in'), 4)
        self.assertEqual(RESULTS2.at(0).get('in'), 7)

    def test_streaming_offset_chain(self):
        """stream events with an offset pipeline."""

        def cback(collection, window_key, group_by):  # pylint: disable=unused-argument
            """callback to pass in."""
            global RESULTS  # pylint: disable=global-statement
            RESULTS = collection

        source = UnboundedIn()

        (
            Pipeline()
            .from_source(source)
            .offset_by(3, 'in')
            .to(CollectionOut, cback)
        )
        source.add_event(EVENTLIST1[0])
        source.add_event(EVENTLIST1[1])

        # Spurious lint error due to upstream tinkering
        # with the global variable
        # pylint: disable=no-member

        self.assertEqual(RESULTS.size(), 2)
        self.assertEqual(RESULTS.at(0).get('in'), 4)
        self.assertEqual(RESULTS.at(1).get('in'), 6)

if __name__ == '__main__':
    unittest.main()
