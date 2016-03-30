"""
Tests for the TimeRange class
"""

import datetime
import unittest
import time

from pypond.range import TimeRange
from pypond.exceptions import TimeRangeException
from pypond.util import aware_utcnow, ms_from_dt


class BaseTestTimeRange(unittest.TestCase):
    """
    Base for range tests.
    """
    def setUp(self):
        self.test_end_ts = aware_utcnow()
        self.test_begin_ts = self.test_end_ts - datetime.timedelta(hours=12)
        self.test_end_ms = ms_from_dt(self.test_end_ts)
        self.test_begin_ms = ms_from_dt(self.test_begin_ts)

    def _create_time_range(self, arg1, arg2=None):  # pylint: disable=no-self-use
        """create a time range object"""
        return TimeRange(arg1, arg2)


class TestTimeRangeCreation(BaseTestTimeRange):
    """
    Test variations of TimeRange object creation.
    """
    def test_invalid_constructor_args(self):
        """test invalid constructor args"""

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
