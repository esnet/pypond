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
        self._year_index = '2014'
        self._month_index = '2014-09'
        self._day_index = '2014-09-17'


class TestIndexCreation(BaseTestIndex):
    """
    Test variations of Event object creation.
    """
    def test_create(self):
        """test index constructor args and underlying TimeRange."""

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
        five_min_idx = Index(self._5_min_index)
        self.assertEquals(
            five_min_idx.as_timerange().to_utc_string(),
            '[Sat, 25 Apr 2009 12:25:00 UTC, Sat, 25 Apr 2009 12:30:00 UTC]')

        # 30 sec index
        thirty_sec_idx = Index(self._30_sec_index)
        self.assertEquals(
            thirty_sec_idx.as_timerange().to_utc_string(),
            '[Sun, 08 Feb 2009 04:10:30 UTC, Sun, 08 Feb 2009 04:11:00 UTC]')

        # year index
        year_idx = Index(self._year_index)
        self.assertEquals(
            year_idx.as_timerange().to_utc_string(),
            '[Wed, 01 Jan 2014 00:00:00 UTC, Wed, 31 Dec 2014 23:59:59 UTC]')

        # month index
        month_idx = Index(self._month_index)
        self.assertEquals(
            month_idx.as_timerange().to_utc_string(),
            '[Mon, 01 Sep 2014 00:00:00 UTC, Tue, 30 Sep 2014 23:59:59 UTC]')

        # test month again over year threshold
        month_border_idx = Index('2015-12')
        self.assertEquals(
            month_border_idx.as_timerange().to_utc_string(),
            '[Tue, 01 Dec 2015 00:00:00 UTC, Thu, 31 Dec 2015 23:59:59 UTC]')

        # day index
        day_idx = Index(self._day_index)
        self.assertEquals(
            day_idx.as_timerange().to_utc_string(),
            '[Wed, 17 Sep 2014 00:00:00 UTC, Wed, 17 Sep 2014 23:59:59 UTC]')

    def test_nice_string(self):
        """test the nice string method."""

        # year index
        year_idx = Index(self._year_index)
        self.assertEquals(year_idx.to_nice_string(), '2014')

        # month index
        month_idx = Index(self._month_index)
        self.assertEquals(month_idx.to_nice_string(), 'September')

        # day index w/ and w/out formatting
        day_idx = Index(self._day_index)
        self.assertEquals(day_idx.to_nice_string(), 'September 17 2014')
        self.assertEquals(day_idx.to_nice_string('%-d %b %Y'), '17 Sep 2014')

        # index index
        idx_idx = Index(self._30_sec_index)
        self.assertEquals(idx_idx.to_nice_string(), self._30_sec_index)

if __name__ == '__main__':
    unittest.main()
