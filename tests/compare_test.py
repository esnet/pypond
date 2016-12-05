"""
Tests for duplicates and such with the event objects.

Put in its own module since event_test.py is out of control at
"""

import datetime
import unittest

from pypond.event import Event
from pypond.indexed_event import IndexedEvent
from pypond.timerange_event import TimeRangeEvent
from pypond.util import aware_utcnow, ms_from_dt


class BaseTestEvent(unittest.TestCase):
    """Base class for comparison tests."""

    def setUp(self):
        pass


class TestDuplicateUtil(BaseTestEvent):
    """Test methods checking for/handling duplicate events."""

    def test_is_duplicate(self):
        """Test Event.is_duplicate()"""

        # events

        # pylint: disable=invalid-name
        e_ts = aware_utcnow()

        e1 = Event(e_ts, 23)
        e2 = Event(e_ts, 23)

        self.assertTrue(Event.is_duplicate(e1, e2))
        self.assertTrue(Event.is_duplicate(e1, e2, ignore_values=False))

        e3 = Event(e_ts, 25)

        self.assertTrue(Event.is_duplicate(e1, e3))
        self.assertFalse(Event.is_duplicate(e1, e3, ignore_values=False))

        # indexed events
        ie1 = IndexedEvent('1d-12355', {'value': 42})
        ie2 = IndexedEvent('1d-12355', {'value': 42})

        self.assertTrue(Event.is_duplicate(ie1, ie2))
        self.assertTrue(Event.is_duplicate(ie1, ie2, ignore_values=False))

        ie3 = IndexedEvent('1d-12355', {'value': 44})

        self.assertTrue(Event.is_duplicate(ie1, ie3))
        self.assertFalse(Event.is_duplicate(ie1, ie3, ignore_values=False))

        # time range events
        test_end_ts = aware_utcnow()
        test_begin_ts = test_end_ts - datetime.timedelta(hours=12)
        test_end_ms = ms_from_dt(test_end_ts)
        test_begin_ms = ms_from_dt(test_begin_ts)

        tre1 = TimeRangeEvent((test_begin_ms, test_end_ms), 11)
        tre2 = TimeRangeEvent((test_begin_ms, test_end_ms), 11)

        self.assertTrue(Event.is_duplicate(tre1, tre2))
        self.assertTrue(Event.is_duplicate(tre1, tre2, ignore_values=False))

        tre3 = TimeRangeEvent((test_begin_ms, test_end_ms), 22)

        self.assertTrue(Event.is_duplicate(tre1, tre3))
        self.assertFalse(Event.is_duplicate(tre1, tre3, ignore_values=False))

if __name__ == '__main__':
    unittest.main()
