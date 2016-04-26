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
        self._hourly_index = '1h-123554'
        self._5_min_index = '5m-4135541'
        self._30_sec_index = '30s-41135541'
        self._month_index = '2014-09'


class TestIndexCreation(BaseTestIndex):
    """
    Test variations of Event object creation.
    """
    def test_create(self):
        """"Test index constructor args/etc."""

        # a daily index
        daily_idx = Index(self._daily_index)
        self.assertEquals(
            daily_idx.as_timerange().to_utc_string(),
            '[Thu, 30 Oct 2003 00:00:00 UTC, Fri, 31 Oct 2003 00:00:00 UTC]')

        # hourly index
        hourly_idx = Index(self._hourly_index)
        self.assertEqual(
            hourly_idx.as_timerange().to_utc_string(),
            '[Sun, 05 Feb 1984 02:00:00 UTC, Sun, 05 Feb 1984 03:00:00 UTC]')

        # 5 minute index
        five_min_index = Index(self._5_min_index)
        self.assertEquals(
            five_min_index.as_timerange().to_utc_string(),
            '[Sat, 25 Apr 2009 12:25:00 UTC, Sat, 25 Apr 2009 12:30:00 UTC]')

        # 30 sec index
        thirty_sec_index = Index('30s-41135541')
        self.assertEquals(
            thirty_sec_index.as_timerange().to_utc_string(),
            '[Sun, 08 Feb 2009 04:10:30 UTC, Sun, 08 Feb 2009 04:11:00 UTC]')

        # month index
        month_idx = Index(self._month_index)
        print 'mat', month_idx.as_timerange()

if __name__ == '__main__':
    unittest.main()
