"""
Tests for the pipeline.

Attributes
----------
DATA : TYPE
    Description
"""

import unittest

from pypond.pipeline import Pipeline
from pypond.pipeline_io import CollectionOut
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

        Returns
        -------
        TYPE
            Description
        """
        self._void_pipeline = Pipeline()


class TestOffsetPipeline(BaseTestPipeline):
    """
    Tests for the offset pipeline operations. This is a simple processor
    mostly for pipeline testing.
    """

    def test_simple_offset_chain(self):
        """test a simple offset chain.

        Returns
        -------
        TYPE
            Description
        """
        out = None
        timeseries = TimeSeries(DATA)

        pip1 = Pipeline().from_source(
            timeseries.collection()).offset_by(1, 'value').offset_by(2).to(
                CollectionOut, out)

        print(pip1._d)

if __name__ == '__main__':
    unittest.main()
