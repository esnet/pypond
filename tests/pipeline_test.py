"""
Tests for the pipeline.

Attributes
----------
DATA : TYPE
    Description
"""

import unittest

from pypond.pipeline import Pipeline
from pypond.series import TimeSeries

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


class BaseTestPipeline(unittest.TestCase):
    """
    Base class for the pipeline tests.
    """

    def setUp(self):
        """
        Common setup stuff.
        """
        self._void_pipeline = Pipeline()


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

if __name__ == '__main__':
    unittest.main()
