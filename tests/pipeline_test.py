"""
Tests for the pipeline.
"""

import unittest

from pypond.pipeline import Pipeline


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

    def test_simple_offset(self):
        """test a simple pipeline offset."""
        pass

if __name__ == '__main__':
    unittest.main()
