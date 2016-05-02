"""
Tests for the Index class
"""

import datetime
import unittest
import warnings

from pypond.index import Index
from pypond.exceptions import IndexException, UtilityWarning
from pypond.util import aware_dt_from_args


class BaseTestIndex(unittest.TestCase):  # pylint: disable=too-many-instance-attributes
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

        self._canned_index = Index(self._day_index)


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

    def test_local_times(self):
        """non-utc dates are evil, but we apparently support them."""

        # even though local times == satan, the sanitizer should
        # still coerce it into utc.
        hour_utc = Index(self._hourly_index, utc=True)

        # make sure those local time warnings triggered - one for begin and end.
        with warnings.catch_warnings(record=True) as wrn:
            hour_local = Index(self._hourly_index, utc=False)
            self.assertEquals(len(wrn), 2)
            self.assertTrue(issubclass(wrn[0].category, UtilityWarning))

        self.assertEquals(hour_utc.begin(), hour_local.begin())

    def test_bad_args(self):
        """pass bogus args."""

        with self.assertRaises(IndexException):
            Index('12-34-56-78')

        with self.assertRaises(IndexException):
            Index('12-34-5a')

        with self.assertRaises(IndexException):
            Index('1d-234a')

        with self.assertRaises(IndexException):
            Index('198o')

        with self.assertRaises(IndexException):
            Index('2015-9@')

    def test_index_accessors(self):
        """test the various accessor methods - mostly for coverage."""

        # various accessors
        self.assertEquals(self._canned_index.to_json(), self._day_index)
        self.assertEquals(self._canned_index.to_string(), self._day_index)
        self.assertEquals(str(self._canned_index), self._day_index)

        # make sure end is what it should be
        beg = aware_dt_from_args(dict(year=2014, month=9, day=17))
        end = beg + datetime.timedelta(hours=23, minutes=59, seconds=59)

        self.assertEquals(self._canned_index.end(), end)

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


class TestIndexStaticMethods(BaseTestIndex):
    """Test the static/window methods formerly in util.js and the
    defunct Generator class."""
    def setUp(self):
        """setup method."""
        super(TestIndexStaticMethods, self).setUp()

    def test_get_index_string(self):
        """
        Used to be:

        const generator = new Generator("5m");
        it("should have the correct index", done => {
            const b = generator.bucket(d);
            const expected = "5m-4754394";
            expect(b.index().asString()).to.equal(expected);
            done();
        });
        """
        dtime = aware_dt_from_args(dict(year=2015, month=2, day=14, hour=7, minute=32, second=22), localize=True)
        print dtime

        # idx_str = Index.get_index_string('5m', dtime)
        # print idx_str

    def test_window_duration(self):
        """test window duration utility method - index window to ms."""

        self.assertEquals(Index.window_duration(self._30_sec_index), 30000)

        self.assertEquals(Index.window_duration(self._5_min_index), 300000)

        self.assertEquals(Index.window_duration(self._year_index), None)

if __name__ == '__main__':
    unittest.main()
