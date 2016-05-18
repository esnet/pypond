#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Tests for the Event class
"""
import copy
import datetime
import json
import re
import unittest

# prefer freeze over the data type specific functions
from pyrsistent import freeze, thaw

from pypond.event import Event, TimeRangeEvent, IndexedEvent
from pypond.exceptions import EventException
from pypond.functions import Functions
from pypond.index import Index
from pypond.range import TimeRange
from pypond.util import (
    aware_utcnow,
    dt_from_ms,
    HUMAN_FORMAT,
    localtime_from_ms,
    LOCAL_TZ,
    ms_from_dt,
)

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

    def _test_deep_get(self, event):
        """Check deep data get() operations."""
        # check using field.spec.notation
        self.assertEqual(event.get('NorthRoute.out'), DEEP_EVENT_DATA.get('NorthRoute').get('out'))
        # test alias function as well
        self.assertEqual(event.value('SouthRoute.in'), DEEP_EVENT_DATA.get('SouthRoute').get('in'))

        # same tests but using new array method.
        self.assertEqual(event.get(['NorthRoute', 'out']),
                         DEEP_EVENT_DATA.get('NorthRoute').get('out'))
        # test alias function as well
        self.assertEqual(event.value(['SouthRoute', 'in']),
                         DEEP_EVENT_DATA.get('SouthRoute').get('in'))


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

    def test_bad_args(self):
        """Test with bad timestamp."""
        with self.assertRaises(EventException):
            Event(str(self.msec), self.data)


class TestRegularEventAccess(BaseTestEvent):
    """
    Tests that work the access and mutator methods for the regular
    Event class
    """

    # access tests

    def test_regular_with_deep_data_get(self):
        """create a regular Event with deep data and test get/field_spec query."""
        event = self._create_event(self.aware_ts, DEEP_EVENT_DATA)

        self._test_deep_get(event)

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

    def test_other_accessors(self):
        """check other accessor methods() - primarily for coverage."""

        # date string formatting
        self.assertEquals(
            self.canned_event.timestamp_as_utc_string(),
            dt_from_ms(self.msec).strftime(HUMAN_FORMAT))

        self.assertEquals(
            self.canned_event.timestamp_as_local_string(),
            localtime_from_ms(self.msec).strftime(HUMAN_FORMAT))

        # underlying datetime objects
        self.assertEquals(
            self.canned_event.begin(),
            dt_from_ms(self.msec))

        self.assertEquals(
            self.canned_event.begin(),
            self.canned_event.end())


class TestEventStaticMethods(BaseTestEvent):
    """
    Check the Event class static methods (equality operators),
    merge/create new events, map/reduce, sum, etc etc.
    """
    def test_event_same(self):
        """test Event.same() static method."""
        ev1 = copy.copy(self.canned_event)
        ev2 = copy.copy(self.canned_event)
        self.assertTrue(Event.same(ev1, ev2))

        # make a new one with same data but new timestamp.
        ev3 = Event(freeze(dict(time=self.aware_ts, data=ev1.data())))
        self.assertFalse(Event.same(ev1, ev3))

    def test_event_valid(self):
        """test Event.is_valid_value()"""
        dct = dict(
            good='good',
            also_good=[],
            none=None,
            nan=float('NaN'),
            empty_string='',  # presume this is undefined
        )
        event = Event(self.aware_ts, dct)

        self.assertTrue(Event.is_valid_value(event, 'good'))
        self.assertTrue(Event.is_valid_value(event, 'also_good'))
        self.assertFalse(Event.is_valid_value(event, 'none'))
        self.assertFalse(Event.is_valid_value(event, 'nan'))
        self.assertFalse(Event.is_valid_value(event, 'empty_string'))

    def test_event_selector(self):
        """test Event.selector()"""

        new_deep = dict({'WestRoute': {'in': 567, 'out': 890}}, **DEEP_EVENT_DATA)

        event = self._create_event(self.aware_ts, new_deep)

        ev2 = Event.selector(event, 'NorthRoute')
        self.assertEqual(len(ev2.data().keys()), 1)
        self.assertIsNotNone(ev2.data().get('NorthRoute'))

        ev3 = Event.selector(event, ['WestRoute', 'SouthRoute'])
        self.assertEqual(len(ev3.data().keys()), 2)
        self.assertIsNotNone(ev3.data().get('SouthRoute'))
        self.assertIsNotNone(ev3.data().get('WestRoute'))

        # bad args for coverage
        ev4 = Event.selector(event, 2)
        self.assertEqual(event, ev4)

    def test_event_merge(self):
        """Test Event.merge()/merge_events()"""
        # same timestamp, different keys

        # good ones, same ts, different payloads
        pay1 = dict(foo='bar', baz='quux')
        ev1 = Event(self.aware_ts, pay1)

        pay2 = dict(foo2='bar', baz2='quux')
        ev2 = Event(self.aware_ts, pay2)

        merged = Event.merge([ev1, ev2])
        self.assertEqual(set(thaw(merged.data())), set(dict(pay1, **pay2)))

        # bad, different ts (error), different payloads
        ev3 = Event(self.aware_ts, pay1)
        ev4 = Event(self.aware_ts + datetime.timedelta(minutes=1), pay2)
        with self.assertRaises(EventException):
            merged = Event.merge([ev3, ev4])

        # bad, same ts, same payloads (error)
        ev5 = Event(self.aware_ts, pay1)
        ev6 = Event(self.aware_ts, pay1)
        with self.assertRaises(EventException):
            merged = Event.merge([ev5, ev6])

        # type mismach for coverage
        idxe = IndexedEvent('1999', pay1)
        with self.assertRaises(EventException):
            Event.merge([ev1, idxe])


class TestEventMapReduceCombine(BaseTestEvent):
    """Test the map, reduce, and combine transforms."""

    def _get_event_series(self):
        """Generate a series of events to play with"""
        events = [
            self._create_event(self.aware_ts,
                               {'name': "source1", 'in': 2, 'out': 11}),
            self._create_event(self.aware_ts + datetime.timedelta(seconds=30),
                               {'name': "source1", 'in': 4, 'out': 13}),
            self._create_event(self.aware_ts + datetime.timedelta(seconds=60),
                               {'name': "source1", 'in': 6, 'out': 15}),
            self._create_event(self.aware_ts + datetime.timedelta(seconds=90),
                               {'name': "source1", 'in': 8, 'out': 18})
        ]

        return events

    def test_event_map_single_key(self):
        """Test Event.map() with single field key"""

        result = Event.map(self._get_event_series(), 'in')
        self.assertEqual(set(result), set({'in': [2, 4, 6, 8]}))

    def test_event_map_multi_key(self):
        """Test Event.map() with multiple field keys."""
        result = Event.map(self._get_event_series(), ['in', 'out'])
        self.assertEqual(set(result), set({'out': [11, 13, 15, 18], 'in': [2, 4, 6, 8]}))

    def test_event_map_function_arg_and_reduce(self):  # pylint: disable=invalid-name
        """Test Event.map() with a custom function and Event.reduce()"""
        def map_sum(event):  # pylint: disable=missing-docstring
            # return 'sum', event.get('in') + event.get('out')
            return dict(sum=event.get('in') + event.get('out'))
        result = Event.map(self._get_event_series(), map_sum)
        self.assertEqual(set(result), set({'sum': [13, 17, 21, 26]}))

        res = Event.reduce(result, Functions.avg)
        self.assertEqual(set(res), set({'sum': 19.25}))

    def test_event_map_no_key_map_all(self):
        """Test Event.map() with no field key - it will map everything"""
        result = Event.map(self._get_event_series())
        self.assertEqual(set(result),
                         set({'in': [2, 4, 6, 8],
                              'name': ['source1', 'source1', 'source1', 'source1'],
                              'out': [11, 13, 15, 18]}))

    def test_simple_map_reduce(self):
        """test simple map/reduce."""
        result = Event.map_reduce(self._get_event_series(), ['in', 'out'], Functions.avg)
        self.assertEqual(set(result), set({'in': 5.0, 'out': 14.25}))

    def test_sum_events_with_combine(self):
        """test summing multiple events together via combine on the back end."""

        # combine them all
        events = [
            self._create_event(self.aware_ts, {'a': 5, 'b': 6, 'c': 7}),
            self._create_event(self.aware_ts, {'a': 2, 'b': 3, 'c': 4}),
            self._create_event(self.aware_ts, {'a': 1, 'b': 2, 'c': 3}),

        ]

        result = Event.sum(events)
        self.assertEqual(result.get('a'), 8)
        self.assertEqual(result.get('b'), 11)
        self.assertEqual(result.get('c'), 14)

        # combine single field
        result = Event.sum(events, 'a')
        self.assertEqual(result.get('a'), 8)
        self.assertIsNone(result.get('b'))
        self.assertIsNone(result.get('c'))

        # grab multiple fields
        result = Event.sum(events, ['a', 'c'])
        self.assertEqual(result.get('a'), 8)
        self.assertIsNone(result.get('b'))
        self.assertEqual(result.get('c'), 14)

        # raise an exception
        bad_events = events + [
            self._create_event(
                self.aware_ts + datetime.timedelta(seconds=1),
                {'a': 8, 'b': 9, 'c': 0})
        ]
        with self.assertRaises(EventException):
            Event.sum(bad_events, 'a')

        # average
        result = Event.avg(
            events + [self._create_event(self.aware_ts, {'a': 1, 'b': 1, 'c': 2})],
            'c')
        self.assertEquals(result.get('c'), 4)

        # bad arg
        self.assertIsNone(Event.sum([]))
        self.assertIsNone(Event.avg([]))

        # work the extra reducer functions in Functions module
        result = Event.combine(events, 'c', Functions.max)
        self.assertEqual(result[0].get('c'), 7)

        result = Event.combine(events, 'c', Functions.min)
        self.assertEqual(result[0].get('c'), 3)

        result = Event.combine(events, 'c', Functions.count)
        self.assertEqual(result[0].get('c'), 3)

        result = Event.combine(events, 'c', Functions.first)
        self.assertEqual(result[0].get('c'), 7)

        result = Event.combine(events, 'c', Functions.last)
        self.assertEqual(result[0].get('c'), 3)

        result = Event.combine(events, 'c', Functions.difference)
        self.assertEqual(result[0].get('c'), 4)

        self.assertIsNone(Functions.first([]))
        self.assertIsNone(Functions.last([]))

    def test_event_collapse(self):
        """test collapse()"""

        ev1 = self._create_event(self.aware_ts, {'a': 5, 'b': 6, 'c': 7})

        ev2 = ev1.collapse(['a', 'c'], 'a_to_c', Functions.sum, append=True)
        self.assertEquals(len(ev2.data().keys()), 4)
        self.assertEquals(ev2.get('a_to_c'), 12)

        ev3 = ev1.collapse(['a', 'c'], 'a_to_c', Functions.sum, append=False)
        self.assertEquals(len(ev3.data().keys()), 1)
        self.assertEquals(ev3.get('a_to_c'), 12)


class TestIndexedEvent(BaseTestEvent):
    """
    Tests for the IndexedEvent class
    """
    def test_indexed_event_create(self):
        """test indexed event creation."""

        # creation with args
        ie1 = IndexedEvent('1d-12355', {'value': 42})
        self.assertEquals(
            ie1.timerange_as_utc_string(),
            '[Thu, 30 Oct 2003 00:00:00 UTC, Fri, 31 Oct 2003 00:00:00 UTC]')
        self.assertEquals(ie1.get('value'), 42)

        # creation with Index
        idx = Index('1d-12355')
        ie2 = IndexedEvent(idx, dict(value=42))

        self.assertEqual(
            ie2.timerange_as_utc_string(),
            '[Thu, 30 Oct 2003 00:00:00 UTC, Fri, 31 Oct 2003 00:00:00 UTC]')
        self.assertEquals(ie1.get('value'), 42)

        # copy ctor
        ie3 = IndexedEvent(ie1)
        self.assertEquals(
            ie3.timerange_as_utc_string(),
            '[Thu, 30 Oct 2003 00:00:00 UTC, Fri, 31 Oct 2003 00:00:00 UTC]')
        self.assertEquals(ie3.get('value'), 42)

        # pass in the immutable guts
        ie4 = IndexedEvent(ie3._d)  # pylint: disable=protected-access
        self.assertEquals(
            ie4.timerange_as_utc_string(),
            '[Thu, 30 Oct 2003 00:00:00 UTC, Fri, 31 Oct 2003 00:00:00 UTC]')
        self.assertEquals(ie4.get('value'), 42)

    def test_accessor_methods(self):
        """test accessors for coverage."""
        idx = '1d-12355'
        val = 42

        ie1 = IndexedEvent(idx, {'value': val})

        self.assertEquals(
            ie1.to_string(),
            '{"index": "1d-12355", "data": {"value": 42}}')
        self.assertEquals(
            str(ie1),
            '{"index": "1d-12355", "data": {"value": 42}}')

        self.assertEquals(
            ie1.to_point(),
            ['1d-12355', 42])

        self.assertEquals(
            ie1.timerange_as_utc_string(),
            '[Thu, 30 Oct 2003 00:00:00 UTC, Fri, 31 Oct 2003 00:00:00 UTC]')

        # check the local string accessor
        i = 0
        l_string = ie1.timerange_as_local_string()
        date_1 = ''

        for match in re.finditer(',', l_string):
            i += 1
            if i >= 2:
                date_1 = l_string[1:match.start()]
                break

        start_dt_as_local = ie1.begin().astimezone(LOCAL_TZ).strftime(HUMAN_FORMAT)

        self.assertEquals(date_1, start_dt_as_local)

        # begin/end()
        self.assertEquals(
            ie1.end() - ie1.begin(),
            datetime.timedelta(days=1))
        self.assertEquals(
            ie1.end() - ie1.timestamp(),
            datetime.timedelta(days=1))

    def test_indexed_event_merge(self):
        """test merging indexed events."""

        index = '1h-396206'
        event1 = IndexedEvent(index, {'a': 5, 'b': 6})
        event2 = IndexedEvent(index, freeze({'c': 2}))  # pmap for coverage
        merged = Event.merge([event1, event2])

        self.assertEquals(merged.get('a'), 5)
        self.assertEquals(merged.get('b'), 6)
        self.assertEquals(merged.get('c'), 2)

        # bad merges
        # type mismatch
        with self.assertRaises(EventException):
            Event.merge([event1, self.canned_event])

        # different index
        event3 = IndexedEvent('1h-396207', dict(d=9))
        with self.assertRaises(EventException):
            Event.merge([event1, event3])

        # key collision
        event4 = IndexedEvent(index, dict(b=9))
        with self.assertRaises(EventException):
            Event.merge([event1, event4])

        # wrong length/etc
        self.assertIsNone(Event.merge({}))
        self.assertIsNone(Event.merge([]))
        self.assertEquals(Event.merge([event4]), event4)

    def test_i_event_deep_get(self):
        """test.deep.get"""

        idxe = IndexedEvent('1d-12355', DEEP_EVENT_DATA)

        self._test_deep_get(idxe)

    def test_bad_args(self):
        """test various bad args."""
        with self.assertRaises(EventException):
            IndexedEvent(234, DEEP_EVENT_DATA)

        with self.assertRaises(EventException):
            IndexedEvent('2015', [23])

    def test_mutator(self):
        """mutate the data."""

        ie1 = IndexedEvent('1d-12355', {'value': 42})

        ie2 = ie1.set_data(55)
        self.assertEqual(ie2.get(), 55)


class TestTimeRangeEvent(BaseTestEvent):
    """
    Tests for the TimeRangeEvent class.
    """
    def setUp(self):
        super(TestTimeRangeEvent, self).setUp()

        self.test_end_ts = aware_utcnow()
        self.test_begin_ts = self.test_end_ts - datetime.timedelta(hours=12)
        self.test_end_ms = ms_from_dt(self.test_end_ts)
        self.test_begin_ms = ms_from_dt(self.test_begin_ts)

        self.canned_time_range = TimeRangeEvent((self.test_begin_ms, self.test_end_ms), 11)

    def test_constructor(self):
        """test creating TimeRangeEvents and basic accessors."""

        # create with ms timestamps/tuple
        tr1 = TimeRangeEvent((self.test_begin_ms, self.test_end_ms), 23)
        self.assertEqual(tr1.data(), dict(value=23))
        self.assertEqual(tr1.to_point()[1], 23)

        # create with datetime/list
        tr2 = TimeRangeEvent([self.test_begin_ts, self.test_end_ts], 2323)
        self.assertEqual(tr2.to_json().get('data'), dict(value=2323))
        self.assertEqual(tr2.to_point()[1], 2323)

        # copy constructor
        tr3 = TimeRangeEvent(tr1)
        self.assertEqual(tr3.data(), dict(value=23))
        self.assertEqual(tr3.to_point()[1], 23)

        # borrow a pmap from another instance
        tr4 = TimeRangeEvent(tr2._d)  # pylint: disable=protected-access
        self.assertEqual(tr4.to_json().get('data'), dict(value=2323))
        self.assertEqual(tr4.to_point()[1], 2323)

        # bad copy arg
        with self.assertRaises(EventException):
            TimeRangeEvent(self.canned_event)

    def test_time_range_event_merge(self):
        """Test merging."""

        t_range = TimeRange(self.test_begin_ts, self.test_end_ts)
        tr1 = TimeRangeEvent(t_range, dict(a=5, b=6))
        tr2 = TimeRangeEvent(t_range, dict(c=2))

        merged = Event.merge([tr1, tr2])

        self.assertEqual(merged.get('a'), 5)
        self.assertEqual(merged.get('b'), 6)
        self.assertEqual(merged.get('c'), 2)

        # bad merges
        # type mismatch
        with self.assertRaises(EventException):
            Event.merge([tr1, self.canned_event])

        # timestamp mismatch
        bad_range = TimeRange(self.test_begin_ts + datetime.timedelta(seconds=1),
                              self.test_end_ts)
        tr3 = TimeRangeEvent(bad_range, dict(d=9))
        with self.assertRaises(EventException):
            Event.merge([tr1, tr3])

        # key collision
        tr4 = TimeRangeEvent(t_range, dict(c=4))
        with self.assertRaises(EventException):
            Event.merge([merged, tr4])

    def test_ts_getters(self):
        """Test the accessors for the underlying TimeRange."""
        ctr = self.canned_time_range
        self.assertEqual(ctr.begin(), self.test_begin_ts)
        self.assertEqual(ctr.end(), self.test_end_ts)
        self.assertEqual(ctr.begin(), ctr.timestamp())
        self.assertEqual(ctr.humanize_duration(), '12 hours')

    def test_accessors(self):
        """work various accessors for coverage."""
        jso = self.canned_time_range.to_json()
        self.assertEqual(jso.get('timerange')[0], self.test_begin_ms)
        self.assertEqual(jso.get('timerange')[1], self.test_end_ms)

        self.assertEquals(self.canned_time_range.to_string(), json.dumps(jso))
        self.assertEquals(str(self.canned_time_range), json.dumps(jso))

        self.assertEquals(self.canned_time_range.timerange_as_utc_string().find(
            self.test_begin_ts.strftime(HUMAN_FORMAT)), 1)

        self.assertEquals(self.canned_time_range.timerange_as_local_string().find(
            localtime_from_ms(self.test_begin_ms).strftime(HUMAN_FORMAT)), 1)

    def test_deep_get(self):
        """Test the deep field_spec gets."""

        tre = TimeRangeEvent((self.test_begin_ms, self.test_end_ms), DEEP_EVENT_DATA)

        self._test_deep_get(tre)

    def test_data_setters(self):
        """Test the mutators."""
        ctr = self.canned_time_range

        new_value = 22

        new_range = ctr.set_data(new_value)
        self.assertEqual(new_range.data(), dict(value=new_value))
        self.assertEqual(new_range.to_point()[1], new_value)

if __name__ == '__main__':
    unittest.main()
