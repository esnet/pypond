"""
Tests for the Index class
"""

import unittest

from pypond.index import Index


class BaseTestIndex(unittest.TestCase):
    """Base for index tests."""
    def setUp(self):
        """setup"""

        self._daily_index = '1d-12355'
        self._month_index = '2014-09'


class TestIndexCreation(BaseTestIndex):
    """
    Test variations of Event object creation.
    """
    def test_create(self):
        """"Test index constructor args/etc."""

        # a daily index
        daily_idx = Index(self._daily_index)
        print 'dat', daily_idx.as_timerange().to_utc_string()

        # month index
        month_idx = Index(self._month_index)
        print 'mat', month_idx.as_timerange()

if __name__ == '__main__':
    unittest.main()
