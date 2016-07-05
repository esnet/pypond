"""
Tests for the pipeline.
"""

import datetime
import unittest

import pytz

from pypond.event import Event
from pypond.functions import Functions
from pypond.pipeline import Pipeline
from pypond.pipeline_io import CollectionOut
from pypond.series import TimeSeries
from pypond.sources import UnboundedIn

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

        self.assertEqual(RESULTS.size(), 2)
        self.assertEqual(RESULTS.at(0).get('in'), 4)
        self.assertEqual(RESULTS.at(1).get('in'), 6)

if __name__ == '__main__':
    unittest.main()
