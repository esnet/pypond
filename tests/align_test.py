"""
Tests for the align processor
"""

import unittest

from pypond.series import TimeSeries

SIMPLE_GAP_DATA = dict(
    name="traffic",
    columns=["time", "value"],
    points=[
        [1471824030000, .75],  # Mon, 22 Aug 2016 00:00:30 GMT
        [1471824105000, 2],  # Mon, 22 Aug 2016 00:01:45 GMT
        [1471824210000, 1],  # Mon, 22 Aug 2016 00:03:30 GMT
        [1471824390000, 1],  # Mon, 22 Aug 2016 00:06:30 GMT
        [1471824510000, 3],  # Mon, 22 Aug 2016 00:08:30 GMT
    ]
)


class AlignTest(unittest.TestCase):
    """
    tests for the align processor
    """

    def setUp(self):
        """setup for all tests."""
        self._simple_ts = TimeSeries(SIMPLE_GAP_DATA)

    def test_basic_linear_align(self):
        """test basic align"""

        aligned = self._simple_ts.align(window='1m')

        self.assertEqual(aligned.size(), 6)
        self.assertEqual(aligned.at(0).get(), 1.25)
        self.assertEqual(aligned.at(1).get(), 1.8571428571428572)
        self.assertEqual(aligned.at(2).get(), 1.2857142857142858)
        self.assertEqual(aligned.at(3).get(), 1.0)
        self.assertEqual(aligned.at(4).get(), 1.0)
        self.assertEqual(aligned.at(5).get(), 1.0)

    def test_basic_hold_align(self):
        """test basic hold align."""

        aligned = self._simple_ts.align(window='1m', method='hold')

        self.assertEqual(aligned.size(), 6)
        self.assertEqual(aligned.at(0).get(), .75)
        self.assertEqual(aligned.at(1).get(), 2)
        self.assertEqual(aligned.at(2).get(), 2)
        self.assertEqual(aligned.at(3).get(), 1)
        self.assertEqual(aligned.at(4).get(), 1)
        self.assertEqual(aligned.at(5).get(), 1)

    def test_align_limit(self):
        """test basic hold align."""

        aligned = self._simple_ts.align(window='1m', method='hold', limit=2)

        self.assertEqual(aligned.size(), 8)
        self.assertEqual(aligned.at(0).get(), .75)
        self.assertEqual(aligned.at(1).get(), 2)
        self.assertEqual(aligned.at(2).get(), 2)
        self.assertEqual(aligned.at(3).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(4).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(5).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(6).get(), 1)
        self.assertEqual(aligned.at(6).get(), 1)


if __name__ == '__main__':
    unittest.main()
