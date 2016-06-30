"""
Tests for the pipeline.
"""

import datetime
import unittest

import pytz

from pypond.event import Event
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


def _strp(dstr):  # pylint: disable=no-self-use
    fmt = '%Y-%m-%dT%H:%M:%SZ'
    return datetime.datetime.strptime(dstr, fmt).replace(tzinfo=pytz.UTC)

EVENTLIST1 = [
    Event(_strp("2015-04-22T03:30:00Z"), {'in': 1, 'out': 2}),
    Event(_strp("2015-04-22T03:31:00Z"), {'in': 3, 'out': 4}),
    Event(_strp("2015-04-22T03:32:00Z"), {'in': 5, 'out': 6}),
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


class TestOffsetPipeline(BaseTestPipeline):
    """
    Tests for the offset pipeline operations. This is a simple processor
    mostly for pipeline testing.
    """

    def test_simple_offset_chain(self):
        """test a simple offset chain."""
        timeseries = TimeSeries(DATA)

        kcol = Pipeline().from_source(
            timeseries.collection()).offset_by(1, 'value').offset_by(2).to_keyed_collections()

        self.assertEqual(kcol['all'].at(0).get(), 55)
        self.assertEqual(kcol['all'].at(1).get(), 21)
        self.assertEqual(kcol['all'].at(2).get(), 29)
        self.assertEqual(kcol['all'].at(3).get(), 96)

    def test_ts_offset_chain(self):
        """test running the offset chain directly from the TimeSeries."""
        timeseries = TimeSeries(DATA)

        kcol = timeseries.pipeline().offset_by(1, 'value').offset_by(2).to_keyed_collections()

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

        Pipeline().from_source(
            timeseries.collection()).offset_by(
                1, 'value').offset_by(2).to(CollectionOut, cback)

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

        pip1 = Pipeline().from_source(source).offset_by(1, 'in').offset_by(2, 'in').to(
            CollectionOut, cback)

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

        Pipeline().from_source(source).offset_by(3, 'in').to(CollectionOut, cback)

        source.add_event(EVENTLIST1[0])
        source.add_event(EVENTLIST1[1])

        self.assertEqual(RESULTS.size(), 2)
        self.assertEqual(RESULTS.at(0).get('in'), 4)
        self.assertEqual(RESULTS.at(1).get('in'), 6)

if __name__ == '__main__':
    unittest.main()
