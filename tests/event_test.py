"""
Tests for the Event class
"""
import datetime
import json
import unittest

from pyrsistent import pmap

from pypond.event import Event


class TestEventCreation(unittest.TestCase):
    """
    Test variations of Event object creation.
    """

    def setUp(self):
        # make a canned event
        self.msec = 1458768183949
        self.data = {'a': 3, 'b': 6, 'c': 9}
        self.key = 'canned'

        self.canned_event = self._create_event(self.msec, self.data, self.key)

    # utility methods

    def _create_event(self, arg1, arg2=None, arg3=None):  # pylint: disable=no-self-use
        return Event(arg1, arg2, arg3)

    def _check_key_and_data(self, event, key, data):
        """canned checks to repeat."""
        self.assertEqual(event.key(), key)
        self.assertEqual(event.data(), pmap(data))

    # test methods

    # creation tests

    def test_regular_with_dt_data_key(self):
        """create a regular Event from datetime, dict and key"""
        ts = datetime.datetime.utcnow()
        data = {'a': 3, 'b': 6}
        key = 'cpu_usage'
        event = self._create_event(ts, data, key)
        self._check_key_and_data(event, key, data)

    def test_regular_with_event_copy(self):
        """create a regular event with copy constructor/existing event."""
        ts = datetime.datetime.utcnow()
        data = {'a': 3, 'b': 6}
        key = 'event_copy'
        event = self._create_event(ts, data, key)

        event2 = Event(event)
        self._check_key_and_data(event2, key, data)

    def test_regular_with_ms_arg(self):
        """create a regular event with ms arg"""
        msec = 1458768183949
        data = {'a': 3, 'b': 6}
        key = 'ms'

        event = self._create_event(msec, data, key)
        self._check_key_and_data(event, key, data)
        # check that msec value translation.
        self.assertEqual(msec, event.to_json().get('time'))

    # access tests

    def test_to_json_and_stringify(self):
        """test output from to_json() and stringify() methods"""

        event_json = self.canned_event.to_json()

        self.assertTrue(isinstance(event_json, dict))
        self.assertEqual(event_json.get('time'), self.msec)
        self.assertEqual(set(event_json.get('data')), set(self.data))
        self.assertEqual(event_json.get('key'), self.key)

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
