"""
Tests for the Event class
"""
import datetime
import json
import unittest

from pyrsistent import pmap

from pypond.event import Event
from pypond.exceptions import EventException
from pypond.util import aware_utcnow


class TestEventCreation(unittest.TestCase):
    """
    Test variations of Event object creation.
    """

    def setUp(self):
        # make a canned event
        self.msec = 1458768183949
        self.data = {'a': 3, 'b': 6, 'c': 9}
        self.aware_ts = aware_utcnow()

        self.canned_event = self._create_event(self.msec, self.data)

    # utility methods

    def _create_event(self, arg1, arg2=None):  # pylint: disable=no-self-use
        return Event(arg1, arg2)

    def _base_checks(self, event, data, dtime=None):
        """canned checks to repeat."""
        self.assertEqual(event.data(), pmap(data))

        if dtime:
            self.assertEqual(event.timestamp(), dtime)

    # test methods

    # creation tests

    def test_regular_with_dt_data_key(self):
        """create a regular Event from datetime, dict."""

        data = {'a': 3, 'b': 6}
        event = self._create_event(self.aware_ts, data)
        self._base_checks(event, data, dtime=self.aware_ts)

        # Now try to creat one with a naive datetime
        ts = datetime.datetime.utcnow()
        with self.assertRaises(EventException):
            self._create_event(ts, data)

    def test_regular_with_event_copy(self):
        """create a regular event with copy constructor/existing event."""
        data = {'a': 3, 'b': 6}

        event = self._create_event(self.aware_ts, data)

        event2 = Event(event)
        self._base_checks(event2, data, dtime=self.aware_ts)

    def test_regular_with_ms_arg(self):
        """create a regular event with ms arg"""
        msec = 1458768183949
        data = {'a': 3, 'b': 6}

        event = self._create_event(msec, data)
        self._base_checks(event, data)
        # check that msec value translation.
        self.assertEqual(msec, event.to_json().get('time'))

    # access tests

    def test_to_json_and_stringify(self):
        """test output from to_json() and stringify() methods"""

        event_json = self.canned_event.to_json()

        self.assertTrue(isinstance(event_json, dict))
        self.assertEqual(event_json.get('time'), self.msec)
        self.assertEqual(set(event_json.get('data')), set(self.data))

        stringify = self.canned_event.stringify()
        self.assertEqual(stringify, json.dumps(self.data))

    def test_to_point(self):
        """test output from to_point()"""
        point = self.canned_event.to_point()

        self.assertTrue(isinstance(point, list))
        self.assertEqual(point[0], self.msec)
        self.assertEqual(set(point[1:]), set(self.data.values()))


if __name__ == '__main__':
    unittest.main()
