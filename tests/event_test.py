"""
Tests for the Event class
"""
import datetime
import json
import unittest

# prefer freeze over the data type specific functions
from pyrsistent import freeze

from pypond.event import Event
from pypond.exceptions import EventException
from pypond.util import aware_utcnow

DEEP_EVENT_DATA = {
    'NorthRoute': {
        'in': 123,
        'out': 456
    },
    'SouthRoute': {
        'in': 654,
        'out': 223
    }
}


class BaseTestEvent(unittest.TestCase):
    """
    Base for Event class tests.
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
        self.assertEqual(event.data(), freeze(data))

        if dtime:
            self.assertEqual(event.timestamp(), dtime)


class TestRegularEventCreation(BaseTestEvent):
    """
    Test variations of Event object creation.
    """

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


class TestRegularEventAccess(BaseTestEvent):
    """
    Tests that work the access and mutator methods for the regular
    Event class
    """

    # access tests

    def test_regular_with_deep_data(self):
        """create a regular Event with deep data and test get/field_spec query."""
        event = self._create_event(self.aware_ts, DEEP_EVENT_DATA)

        # check using field.spec notation
        self.assertEqual(event.get('NorthRoute.out'), DEEP_EVENT_DATA.get('NorthRoute').get('out'))
        # test alias function as well
        self.assertEqual(event.value('SouthRoute.in'), DEEP_EVENT_DATA.get('SouthRoute').get('in'))

    def test_regular_deep_set_new_data(self):
        """create a regular Event with deep data and set new data/receive new object."""
        event = self._create_event(self.aware_ts, DEEP_EVENT_DATA)

        west_route = {'WestRoute': {'in': 567, 'out': 890}}

        new_event = event.set_data(west_route)
        # should just be one new key now
        self.assertEqual(len(new_event.data()), 1)
        self.assertEqual(set(['WestRoute']), set(new_event.data().keys()))
        # test values
        self.assertEqual(new_event.data().get('WestRoute').get('out'), 890)
        # original event must still the same
        self.assertEqual(event.data(), freeze(DEEP_EVENT_DATA))

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
