"""
Tests for the TimeRange class
"""

import datetime
import unittest
import time

from pyrsistent import freeze
import pytz

from pypond.range import TimeRange
from pypond.exceptions import TimeRangeException
from pypond.util import aware_utcnow, ms_from_dt, to_milliseconds, EPOCH


class BaseTestTimeRange(unittest.TestCase):
    """
    Base for range tests.
    """
    def setUp(self):
        self.test_end_ts = aware_utcnow()
        self.test_begin_ts = self.test_end_ts - datetime.timedelta(hours=12)
        self.test_end_ms = ms_from_dt(self.test_end_ts)
        self.test_begin_ms = ms_from_dt(self.test_begin_ts)

        self.canned_range = self._create_time_range(self.test_begin_ts, self.test_end_ts)

    def _create_time_range(self, arg1, arg2=None):  # pylint: disable=no-self-use
        """create a time range object"""
        return TimeRange(arg1, arg2)


class TestTimeRangeCreation(BaseTestTimeRange):
    """
    Test variations of TimeRange object creation.
    """
    def test_ts_creation(self):
        """test creation with timestamps (both args and lists), validate said."""

        # two args
        trng = self._create_time_range(self.test_begin_ts, self.test_end_ts)
        self.assertEqual(trng.begin(), self.test_begin_ts)
        self.assertEqual(trng.end(), self.test_end_ts)
        pay = trng.to_json()
        self.assertEqual(pay[0], self.test_begin_ms)
        self.assertEqual(pay[1], self.test_end_ms)

        # list type (tuple in this case)
        trng = self._create_time_range((self.test_begin_ts, self.test_end_ts,))
        self.assertEqual(trng.begin(), self.test_begin_ts)
        self.assertEqual(trng.end(), self.test_end_ts)
        pay = trng.to_json()
        self.assertEqual(pay[0], self.test_begin_ms)
        self.assertEqual(pay[1], self.test_end_ms)

    def test_ms_creation(self):
        """Test creation with epoch ms (both args and lists), validate said."""

        # two args
        trng = self._create_time_range(self.test_begin_ms, self.test_end_ms)
        self.assertEqual(trng.begin(), self.test_begin_ts)
        self.assertEqual(trng.end(), self.test_end_ts)
        pay = trng.to_json()
        self.assertEqual(pay[0], self.test_begin_ms)
        self.assertEqual(pay[1], self.test_end_ms)

        # list type (list/pvector)
        trng = self._create_time_range(freeze([self.test_begin_ms, self.test_end_ms]))
        self.assertEqual(trng.begin(), self.test_begin_ts)
        self.assertEqual(trng.end(), self.test_end_ts)
        pay = trng.to_json()
        self.assertEqual(pay[0], self.test_begin_ms)
        self.assertEqual(pay[1], self.test_end_ms)

    def test_copy_ctor(self):
        """test using the copy constructor."""
        orig = self.canned_range

        new_range = TimeRange(orig)

        self.assertTrue(orig.equals(new_range))  # tests all timestamps

    def test_unrounded_dt_sanitize(self):
        """datetime w/ microsecond accuracy being rounded to milliseconds internally"""

        def get_unrounded_dt():
            """make sure the dt object is at microsecond accuracy."""
            dtime = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
            if dtime.microsecond % 1000 != 0:
                return dtime
            else:
                # this is unlikely
                return get_unrounded_dt()

        unrounded_start = get_unrounded_dt() - datetime.timedelta(hours=1)
        unrounded_end = get_unrounded_dt()

        internal_round = TimeRange(unrounded_start, unrounded_end)

        # will not be equal due to internal rounding to ms
        self.assertNotEqual(internal_round.begin(), unrounded_start)
        self.assertNotEqual(internal_round.end(), unrounded_end)

        # use utility rounding function
        self.assertEqual(internal_round.begin(), to_milliseconds(unrounded_start))
        self.assertEqual(internal_round.end(), to_milliseconds(unrounded_end))

    def test_json_and_stringoutput(self):
        """verify the json (vanilla data structure) and string output is right"""
        rang = self.canned_range

        self.assertEqual(rang.to_json(), [self.test_begin_ms, self.test_end_ms])
        self.assertEqual(rang.to_string(), '[{b}, {e}]'.format(b=self.test_begin_ms,
                                                               e=self.test_end_ms))

    def test_human_friendly_strings(self):
        """test human friendly outputs."""

        rang = TimeRange(EPOCH, EPOCH + datetime.timedelta(hours=24))

        # Hmmm, this is going to vary depending on where it is run.
        # self.assertEqual(rang.humanize(), 'Dec 31, 1969 04:00:00 PM to Jan 1, 1970 04:00:00 PM')

        rang = TimeRange.last_day()
        self.assertEqual(rang.relative_string(), 'a day ago to now')

        rang = TimeRange.last_seven_days()
        self.assertEqual(rang.relative_string(), '7 days ago to now')

        rang = TimeRange.last_thirty_days()
        self.assertEqual(rang.relative_string(), '30 days ago to now')

        rang = TimeRange.last_month()
        self.assertEqual(rang.relative_string(), 'a month ago to now')

        rang = TimeRange.last_ninety_days()
        self.assertEqual(rang.relative_string(), '2 months ago to now')

    def test_invalid_constructor_args(self):
        """test invalid constructor args"""

        # test both (two, args) and ([list, arg]) inputs to work different logic

        # unaware datetime input
        u_begin = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        u_end = datetime.datetime.utcnow()

        with self.assertRaises(TimeRangeException):
            self._create_time_range(u_begin, u_end)
        with self.assertRaises(TimeRangeException):
            self._create_time_range([u_begin, u_end])

        # invalid types - pass in floats
        end = time.time() * 1000
        begin = end - 10000

        with self.assertRaises(TimeRangeException):
            self._create_time_range(begin, end)
        with self.assertRaises(TimeRangeException):
            self._create_time_range([begin, end])

        # type mismatch
        with self.assertRaises(TimeRangeException):
            self._create_time_range((int(begin), aware_utcnow()))
        with self.assertRaises(TimeRangeException):
            self._create_time_range(int(begin), aware_utcnow())

        with self.assertRaises(TimeRangeException):
            self._create_time_range([aware_utcnow() - datetime.timedelta(hours=12), int(end)])
        with self.assertRaises(TimeRangeException):
            self._create_time_range(aware_utcnow() - datetime.timedelta(hours=12), int(end))

        # end time before begin time
        bad_begin = aware_utcnow()
        bad_end = aware_utcnow() - datetime.timedelta(hours=12)

        with self.assertRaises(TimeRangeException):
            self._create_time_range(bad_begin, bad_end)
        with self.assertRaises(TimeRangeException):
            self._create_time_range([bad_begin, bad_end])

        with self.assertRaises(TimeRangeException):
            self._create_time_range(ms_from_dt(bad_begin), ms_from_dt(bad_end))
        with self.assertRaises(TimeRangeException):
            self._create_time_range((ms_from_dt(bad_begin), ms_from_dt(bad_end)))

if __name__ == '__main__':
    unittest.main()
