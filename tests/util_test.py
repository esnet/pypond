"""
Tests for the util module.
"""
import datetime
import time
import unittest

import pytz

from pypond.util import (
    aware_dt_from_args,
    aware_utcnow,
    dt_from_ms,
    dt_is_aware,
    EPOCH,
    localtime_from_ms,
    monthdelta,
    ms_from_dt,
    sanitize_dt,
)
from pypond.exceptions import UtilityException


class TestTime(unittest.TestCase):
    """
    Tests to verify the time conversion and testing functions work.

    Primarily to enforce the use of aware UTC datetime objects.
    """

    def setUp(self):
        self.ms_reference = 1458768183949  # Wed, 23 Mar 2016 21:23:03.949 GMT
        self.naive = datetime.datetime.utcnow()

    def test_round_trip(self):
        """Test ms -> dt -> ms and dt -> ms -> dt"""

        # ms -> dt -> ms
        to_dt = dt_from_ms(self.ms_reference)
        from_dt = ms_from_dt(to_dt)
        self.assertEquals(from_dt, self.ms_reference)

        # dt -> ms -> dt to test rounding in aware_utcnow()
        now = aware_utcnow()
        to_ms = ms_from_dt(now)
        back_to_dt = dt_from_ms(to_ms)
        self.assertEquals(now, back_to_dt)

        # dt from unixtime -> ms -> dt
        utc = datetime.datetime.utcfromtimestamp(1459442035).replace(tzinfo=pytz.UTC)
        utcms = ms_from_dt(utc)
        back_to_utc = dt_from_ms(utcms)
        self.assertEquals(utc, back_to_utc)

    def test_aware(self):
        """Verify test_aware function."""
        self.assertFalse(dt_is_aware(self.naive))

        aware = aware_utcnow()
        self.assertTrue(dt_is_aware(aware))

    def test_epoch_constant(self):
        """Make sure util.EPOCH does not get changed to be naive."""
        self.assertTrue(dt_is_aware(EPOCH))

    def test_dt_from_ms(self):
        """Test function to make datetime from epoch ms/verify aware."""
        dtime = dt_from_ms(self.ms_reference)
        self.assertTrue(dt_is_aware(dtime))

    def test_ms_from_dt(self):
        """Run reference ms into datetime and extract the ms again."""
        dtime = dt_from_ms(self.ms_reference)
        new_ms = ms_from_dt(dtime)
        # after the round trip, value should be the same.
        self.assertEqual(new_ms, self.ms_reference)

        # test sanity check stopping naive datetime objects
        with self.assertRaises(UtilityException):
            ms_from_dt(self.naive)

    def test_sanitize_dt(self):
        """Test datetime timezone conversion to UTC."""

        # aware utc should just go in and out.
        utc = aware_utcnow()
        sanitized_utc = sanitize_dt(utc)
        self.assertTrue(dt_is_aware(sanitized_utc))
        self.assertEqual(utc, sanitized_utc)

        # Sanitize a time zone aware localtime to UTC. The
        # sanitized object

        # Use .localize and not datetime.replace to generate
        # the local date because that doesn't handle DST correctly.
        def get_unrounded_local():
            """get an unrounded local time deal."""
            pacific = pytz.timezone('US/Pacific')
            local = pacific.localize(datetime.datetime.now())
            if local.microsecond % 10000 != 0:
                return local
            else:
                # unlikely
                return get_unrounded_local()

        local = get_unrounded_local()

        # testing mode to suppress warnings
        local_utc = sanitize_dt(local, testing=True)

        # objects will not be equal since sanitize is rounding to milliseconds
        self.assertNotEqual(local_utc, local)

        # this is a terrible way to round to milliseconds, but in this
        # case, we are trying to leave the time zone difference intact
        # whereas pypond.util wants to force everything to UTC.
        # But in general DO NOT DO THIS. -MMG

        msec = '{ms}000'.format(ms=str(local.microsecond)[0:3])
        local = local.replace(microsecond=int(msec))

        self.assertEqual(local_utc, local)

        # double check that delta is zero
        local_utc_delta = local_utc - local
        self.assertEqual(int(local_utc_delta.total_seconds()), 0)

        # test sanity check stopping naive datetime objects
        with self.assertRaises(UtilityException):
            sanitize_dt(self.naive)

    def test_dt_generation_from_args(self):
        """check aware_dt_from_args()

        This generates a datetime object from the kwargs that are passed to
        datetime.datetime. So all of the attrs will be the same between the
        UTC and local version.
        """
        dtime = aware_dt_from_args(
            dict(year=2015, month=3, day=14, hour=7, minute=32, second=22))

        dtime_loc = aware_dt_from_args(
            dict(year=2015, month=3, day=14, hour=7, minute=32, second=22),
            localize=True)

        self.assertEquals(dtime.year, dtime_loc.year)
        self.assertEquals(dtime.month, dtime_loc.month)
        self.assertEquals(dtime.day, dtime_loc.day)
        self.assertEquals(dtime.hour, dtime_loc.hour)
        self.assertEquals(dtime.minute, dtime_loc.minute)
        self.assertEquals(dtime.second, dtime_loc.second)

        self.assertNotEqual(dtime.tzinfo, dtime_loc.tzinfo)

    def test_bad_args(self):
        """Trigger errors for coverage."""
        with self.assertRaises(UtilityException):
            aware_dt_from_args(('year', 2015))

        with self.assertRaises(UtilityException):
            ms_from_dt(localtime_from_ms(time.time() * 1000))

    def test_misc(self):
        """random things for coverage."""
        # work logic in monthdelta
        dtime = datetime.datetime(year=2015, month=12, day=1)
        self.assertEquals(monthdelta(dtime, 0), dtime)

if __name__ == '__main__':
    unittest.main()
