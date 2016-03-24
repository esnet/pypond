"""
Tests for the util module.
"""
import datetime
import unittest

import pytz

from pypond.util import (
    aware_utcnow,
    dt_from_dt,
    dt_from_ms,
    dt_is_aware,
    EPOCH,
    ms_from_dt,
)


class TestTime(unittest.TestCase):
    """
    Tests to verify the time conversion and testing functions work.

    Primarily to enforce the use of aware UTC datetime objects.
    """

    def setUp(self):
        self.ms_reference = 1458768183949  # Wed, 23 Mar 2016 21:23:03.949 GMT

    def test_aware(self):
        """verify test_aware function."""

        naive = datetime.datetime.utcnow()
        self.assertFalse(dt_is_aware(naive))

        aware = aware_utcnow()
        self.assertTrue(dt_is_aware(aware))

    def test_epoch_constant(self):
        """Make sure util.EPOCH does not get changed to be naive."""
        self.assertTrue(dt_is_aware(EPOCH))

    def test_td_from_ms(self):
        """test function to make datetime from epoch ms/verify aware."""
        dtime = dt_from_ms(self.ms_reference)
        self.assertTrue(dt_is_aware(dtime))

    def test_ms_from_dt(self):
        """Run reference ms into datetime and extract the ms again."""
        dtime = dt_from_ms(self.ms_reference)
        new_ms = ms_from_dt(dtime)
        # after the round trip, value should be the same.
        self.assertEqual(new_ms, self.ms_reference)

    def test_dt_from_dt(self):
        """Test dt -> dt helper function."""
        dtime = aware_utcnow()
        new_dtime = dt_from_dt(dtime)
        self.assertEqual(dtime, new_dtime)


if __name__ == '__main__':
    unittest.main()
