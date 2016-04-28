"""
Tests for the TimeRange class
"""

import datetime
import re
import time
import unittest

from pyrsistent import freeze
import pytz

from pypond.range import TimeRange
from pypond.exceptions import TimeRangeException
from pypond.util import (
    aware_utcnow,
    EPOCH,
    HUMAN_FORMAT,
    is_pvector,
    LOCAL_TZ,
    ms_from_dt,
    to_milliseconds,
)


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

        # wrong number of args
        with self.assertRaises(TimeRangeException):
            TimeRange((begin, end, end))

    def test_awareness_check(self):
        """work the duck typing failover in the awareness check."""
        dtime = datetime.datetime.now()
        with self.assertRaises(TimeRangeException):
            TimeRange.awareness_check(dtime)


class TestTimeRangeOutput(BaseTestTimeRange):
    """
    Tests to check output from the time range objects
    """
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

    def test_accessors(self):
        """test various data accessors, primarily for coverage."""

        # check the underlying immutable vector
        rng = self.canned_range.range()
        self.assertTrue(is_pvector(rng))
        self.assertEquals(rng[0], self.test_begin_ts)

        # check the local string accessor
        i = 0
        l_string = self.canned_range.to_local_string()
        date_1 = ''

        for match in re.finditer(',', l_string):
            i += 1
            if i >= 2:
                date_1 = l_string[1:match.start()]
                break

        start_dt_as_local = self.test_begin_ts.astimezone(LOCAL_TZ).strftime(HUMAN_FORMAT)

        self.assertEquals(date_1, start_dt_as_local)

        # humanize check
        human = self.canned_range.humanize()
        self.assertEquals(human.split(' to ')[0], start_dt_as_local)

        # duration checks
        self.assertEquals(self.canned_range.duration(), 43200000)

        self.assertEquals(self.canned_range.humanize_duration(), '12 hours')

        # __str__
        to_str = str(self.canned_range)
        ms_bounds = '[{b}, {e}]'.format(b=self.test_begin_ms, e=self.test_end_ms)
        self.assertEquals(to_str, ms_bounds)


class TestTimeRangeComparisons(BaseTestTimeRange):
    """
    Test mutating and comparing TimeRange objects. Crib the values from
    the original javascript tests to make sure we're comparing the same stuff.
    """

    def _strp(self, dstr):  # pylint: disable=no-self-use
        fmt = '%Y-%m-%d %H:%M'
        return datetime.datetime.strptime(dstr, fmt).replace(tzinfo=pytz.UTC)

    def test_equality(self):
        """compare time ranges to check equality."""
        taa = self._strp("2010-01-01 12:00")
        tbb = self._strp("2010-02-01 12:00")
        range1 = TimeRange(taa, tbb)

        tcc = self._strp("2010-01-01 12:00")
        tdd = self._strp("2010-02-01 12:00")
        range2 = TimeRange(tcc, tdd)

        tee = self._strp("2012-03-01 12:00")
        tff = self._strp("2012-04-02 12:00")
        range3 = TimeRange(tee, tff)

        self.assertTrue(range1.equals(range2))
        self.assertFalse(range1.equals(range3))

    def test_overlap_non_overlap(self):
        """compare overlap to a non-overlapping range"""
        taa = self._strp("2010-01-01 12:00")
        tbb = self._strp("2010-02-01 12:00")
        range1 = TimeRange(taa, tbb)

        tcc = self._strp("2010-03-01 12:00")
        tdd = self._strp("2010-04-01 12:00")
        range2 = TimeRange(tcc, tdd)

        self.assertFalse(range1.overlaps(range2))
        self.assertFalse(range2.overlaps(range1))

    def test_overlap_overlap(self):
        """compare overlap to an overlapping range"""
        taa = self._strp("2010-01-01 12:00")
        tbb = self._strp("2010-09-01 12:00")
        range1 = TimeRange(taa, tbb)

        tcc = self._strp("2010-08-01 12:00")
        tdd = self._strp("2010-11-01 12:00")
        range2 = TimeRange(tcc, tdd)

        self.assertTrue(range1.overlaps(range2))
        self.assertTrue(range2.overlaps(range1))

    def test_contain_complete_contain(self):
        """compare for containment to a completely contained range."""
        taa = self._strp("2010-01-01 12:00")
        tbb = self._strp("2010-09-01 12:00")
        range1 = TimeRange(taa, tbb)

        tcc = self._strp("2010-03-01 12:00")
        tdd = self._strp("2010-06-01 12:00")
        range2 = TimeRange(tcc, tdd)

        self.assertTrue(range1.contains(range2))

    def test_containment_to_overlap(self):
        """compare for containment to an overlapping range."""
        taa = self._strp("2010-01-01 12:00")
        tbb = self._strp("2010-09-01 12:00")
        range1 = TimeRange(taa, tbb)

        tcc = self._strp("2010-06-01 12:00")
        tdd = self._strp("2010-12-01 12:00")
        range2 = TimeRange(tcc, tdd)

        self.assertFalse(range1.contains(range2))

    def test_compare_before_time(self):
        """compare to a time before the range."""
        taa = self._strp("2010-06-01 12:00")
        tbb = self._strp("2010-08-01 12:00")
        range1 = TimeRange(taa, tbb)

        before = self._strp("2010-01-15 12:00")

        self.assertFalse(range1.contains(before))

    def test_compare_during_range(self):
        """compare to a time during the time."""
        taa = self._strp("2010-06-01 12:00")
        tbb = self._strp("2010-08-01 12:00")
        range1 = TimeRange(taa, tbb)

        during = self._strp("2010-07-15 12:00")

        self.assertTrue(range1.contains(during))

    def test_compare_after_range(self):
        """compare to a time after the range."""
        taa = self._strp("2010-06-01 12:00")
        tbb = self._strp("2010-08-01 12:00")
        range1 = TimeRange(taa, tbb)

        after = self._strp("2010-12-15 12:00")

        self.assertFalse(range1.contains(after))

    def test_contains_badarg(self):
        """pass ms to contains - this is for coverage."""
        self.assertFalse(self.canned_range.contains(self.test_begin_ms))

    def test_within(self):
        """test within() test."""
        taa = self._strp("2010-06-01 12:00")
        tbb = self._strp("2010-07-01 12:00")
        range1 = TimeRange(taa, tbb)

        tcc = self._strp("2010-05-01 12:00")
        tdd = self._strp("2010-08-01 12:00")
        range2 = TimeRange(tcc, tdd)

        self.assertTrue(range1.within(range2))
        self.assertFalse(range2.within(range1))

    def test_extents(self):
        """test extents() factory."""
        taa = self._strp("2010-05-01 12:00")
        tbb = self._strp("2010-07-01 12:00")
        range1 = TimeRange(taa, tbb)

        tcc = self._strp("2010-06-01 12:00")
        tdd = self._strp("2010-08-01 12:00")
        range2 = TimeRange(tcc, tdd)

        range_ext = range1.extents(range2)
        self.assertTrue(range_ext.begin(), taa)
        self.assertTrue(range_ext.end(), tdd)

    def test_non_intersection(self):
        """compare if the ranges don't intersect."""
        # Two non-overlapping ranges: intersect() returns undefined
        begin_time = self._strp("2010-01-01 12:00")
        end_time = self._strp("2010-06-01 12:00")
        range1 = TimeRange(begin_time, end_time)

        begin_time_outside = self._strp("2010-07-15 12:00")
        end_time_outside = self._strp("2010-08-15 12:00")
        range_outside = TimeRange(begin_time_outside, end_time_outside)

        self.assertIsNone(range1.intersection(range_outside))

    def test_new_from_intersection(self):
        """new range if the ranges intersect."""
        # Two overlapping ranges: intersect() returns
        #    01 -------06       range
        #           05-----07   rangeOverlap
        #           05-06       intersection
        begin_time = self._strp("2010-01-01 12:00")
        end_time = self._strp("2010-06-01 12:00")
        range1 = TimeRange(begin_time, end_time)

        begin_time_overlap = self._strp("2010-05-01 12:00")
        end_time_overlap = self._strp("2010-07-01 12:00")
        range_overlap = TimeRange(begin_time_overlap, end_time_overlap)
        expected = TimeRange(begin_time_overlap, end_time)

        self.assertTrue(range1.intersection(range_overlap).equals(expected))

    def test_new_from_surround(self):
        """new range if one range surrounds another."""
        # One range fully inside the other intersect() returns the smaller range
        #    01 -------06    range
        #       02--04       rangeInside
        #       02--04       intersection
        begin_time = self._strp("2010-01-01 12:00")
        end_time = self._strp("2010-06-01 12:00")
        range1 = TimeRange(begin_time, end_time)

        begin_time_inside = self._strp("2010-02-01 12:00")
        end_time_inside = self._strp("2010-04-01 12:00")
        range_inside = TimeRange(begin_time_inside, end_time_inside)

        self.assertTrue(range1.intersection(range_inside).equals(range_inside))
        self.assertTrue(range_inside.intersection(range1).equals(range_inside))


class TestTimeRangeMutation(BaseTestTimeRange):
    """
    Test mutating TimeRange objects.
    """
    def test_set_end_and_begin(self):
        """test the begin/end mutators"""

        can = self.canned_range
        new_begin = self.test_begin_ts - datetime.timedelta(hours=24)
        new_end = self.test_end_ts + datetime.timedelta(hours=24)

        rng1 = can.set_begin(new_begin)
        self.assertEquals(rng1.begin(), new_begin)

        rng2 = rng1.set_end(new_end)
        self.assertEquals(rng2.end(), new_end)
        self.assertEquals(rng2.begin(), new_begin)

    def test_mutation_new_range(self):
        """mutate to new range"""

        rang = self.canned_range

        new_end = self.test_end_ts - datetime.timedelta(hours=1)
        new_range = rang.set_end(new_end)

        self.assertEqual(rang.begin(), new_range.begin())
        self.assertNotEqual(rang.end(), new_range.end())
        self.assertEqual(new_range.end(), new_end)

if __name__ == '__main__':
    unittest.main()
