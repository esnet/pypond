#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Implementation of Pond Collection class.
"""

import copy
import json

from pyrsistent import freeze, thaw

from .sources import BoundedIn
from .event import Event
from .exceptions import CollectionException, CollectionWarning, UtilityException
from .functions import Functions
from .range import TimeRange
from .util import unique_id, is_pvector, ObjectEncoder, _check_dt


class Collection(BoundedIn):  # pylint: disable=too-many-public-methods
    """
    A collection is a list of Events. You can construct one out of either
    another collection, or a list of Events. You can addEvent() to a collection
    and a new collection will be returned.

    Basic operations on the list of events are also possible. You
    can iterate over the collection with a for..of loop, get the size()
    of the collection and access a specific element with at().

    Initialize from copy, lists, etc.

    instance_or_list arg can be:

    * a Collection object (copy ctor)
    * a python list
    * a pyrsistent.pvector

    The list and pvector will contain Events.
    """
    def __init__(self, instance_or_list, copy_events=True):
        """
        Create a collection object.
        """
        super(Collection, self).__init__()

        self._id = unique_id('collection-')
        self._event_list = None
        self._type = None

        if isinstance(instance_or_list, Collection):
            other = instance_or_list
            if copy_events:
                # pylint: disable=protected-access
                self._event_list = other._event_list
                self._type = other._type
            else:
                self._event_list = freeze(list())

        elif isinstance(instance_or_list, list):
            events = list()
            for i in instance_or_list:
                self._check(i)
                events.append(i)
            self._event_list = freeze(events)

        elif is_pvector(instance_or_list):
            self._event_list = instance_or_list
            for i in self._event_list:
                self._check(i)

        else:
            msg = 'Arg was not a Collection, list or pvector - '
            msg += 'initializing event list to empty pvector'
            self._warn(msg, CollectionWarning)

            self._event_list = freeze(list())

    def to_json(self):
        """
        Returns the collection as json object.

        This is actually like json.loads(s) - produces the
        actual vanilla data structure.

        :returns: list of Event objects
        """
        return thaw(self._event_list)

    def to_string(self):
        """
        Retruns the collection as a string, useful for serialization.

        In JS land, this is synonymous with __str__ or __unicode__

        Use custom object encoder because this is a list of Event* objects.

        :returns: str -- String representation of this object.
        """
        return json.dumps(self.to_json(), cls=ObjectEncoder)

    def type(self):
        """
        Event object type.

        The class of the type of events in this collection.

        :returns: Event | IndexedEvent | TimeRangeEvent (class not instance)
        """
        return self._type

    def size(self):
        """Number of items in collection.

        :returns: int -- N items in collection.
        """
        return len(self._event_list)

    def size_valid(self, field_spec='value'):
        """
        Returns the number of valid items in this collection.

        Uses the fieldSpec to look up values in all events.
        It then counts the number that are considered valid,
        i.e. are not NaN, undefined or null.

        :param field_spec: Name of value to look up.
        :type field_spec: str
        :returns: int - Number of <field_spec> values in all the Events.
        """
        count = 0

        for i in self.events():
            if Event.is_valid_value(i, field_spec):
                count += 1

        return count

    def at(self, pos):  # pylint: disable=invalid-name
        """Returns an item in the collection by its position.

        Creates a new object via copy ctor.

        :param pos: The index of the event to be retrieved.
        :type pos: int
        :returns: new Event | IndexedEvent | TimeRangeEvent instance
        :raises: CollectionException
        """
        try:
            return self._type(self._event_list[pos])
        except IndexError:
            raise CollectionException('invalid index given to at()')

    def at_time(self, time):
        """Return an item by time. Primarily a utility method that sits in
        front of bisect() and fetches using at().

        If you have events at 12:00 and 12:02 and you make the query
        at 12:01, the one at 12:00 will be returned. Otherwise it will
        return the exact match.

        :param time: Datetime object >= to the event to be returned. Must
            be an aware UTC datetime object.
        :type time: datetime.datetime
        :returns: new Event | IndexedEvent | TimeRangeEvent instance
        """
        pos = self.bisect(time)

        if pos and pos < self.size():
            return self.at(pos)

    def at_first(self):
        """Retrieve the first item in this collection.

        :returns: new Event | IndexedEvent | TimeRangeEvent instance
        """
        if self.size():
            return self.at(0)

    def at_last(self):
        """Return the last event item in this collection.

        :returns: new Event | IndexedEvent | TimeRangeEvent instance
        """
        if self.size():
            return self.at(-1)

    def bisect(self, dtime, b=0):  # pylint: disable=invalid-name
        """
        Finds the index that is just less than the time t supplied.
        In other words every event at the returned index or less
        has a time before the supplied t, and every sample after the
        index has a time later than the supplied t.

        Optionally supply a begin index to start searching from.

            * dtime - python datetime object to bisect collection with
                will be made into an aware datetime in UTC.
            * b - position to start

        Returns index that is the greatest but still below t - see docstring
        for at_time()

        :param dtime: Datetime object >= to the event to be returned. Must
            be an aware UTC datetime object.
        :type dtime: datetime.datetime
        :param b: Array index position to start searching from.
        :type b: int
        :returns: int or None -- The index position of the desired event.
        :raises: EventException
        """

        i = copy.copy(b)  # paranoia
        size = self.size()

        if not size:
            return None

        try:
            _check_dt(dtime)
        except UtilityException:
            msg = 'at_time() and bisect() must be called with aware UTC datetime objects'
            raise CollectionException(msg)

        while i < size:
            ts_tmp = self.at(i).timestamp()
            if ts_tmp > dtime:
                return i - 1 if i - 1 >= 0 else 0
            elif ts_tmp == dtime:
                return i

            i += 1

        return i - 1

    def events(self):
        """
        Generator to allow for..of loops over series.events()

        ::

            for i in series.events():
                do_stuff(i)

        :returns: iterator -- Iterate over the internal events.
        """
        return iter(self._event_list)

    def event_list(self):
        """Returns the raw Immutable event list.

        :returns: pyrsistent.pvector - The raw immutable event list.
        """
        return self._event_list

    def event_list_as_list(self):
        """return a python list of the event list.

        :returns: list -- Thawed version of the internal pvector.
        """
        return thaw(self.event_list())

    # Series range

    def range(self):
        """
        From the range of times, or Indexes within the TimeSeries, return
        the extents of the Collection/TimeSeries as a TimeRange.

        :returns: TimeRange
        """
        min_val = None
        max_val = None

        for i in self.events():
            if min_val is None or i.begin() < min_val:
                min_val = i.begin()

            if max_val is None or i.end() > max_val:
                max_val = i.end()

        if min_val is not None and max_val is not None:
            return TimeRange(min_val, max_val)

    # event list mutation

    def add_event(self, event):
        """
        Add an event to the payload and return a new Collection object.

        :param event: Event to add to collection.
        :type event: Event | IndexedEvent | TimeRangeEvent
        :returns: Collection
        """
        self._check(event)
        return Collection(self._event_list.append(event))

    def slice(self, begin, end):
        """
        Perform a slice of events within the Collection, returns a new
        Collection representing a portion of this TimeSeries from begin up to
        but not including end. Uses typical python [slice:syntax].

        :param begin: Slice begin.
        :type begin: int
        :param end: Slice end.
        :type end: int
        :returns: Collection
        """
        sliced = Collection(self._event_list[begin:end])
        sliced._type = self._type  # pylint: disable=protected-access
        return sliced

    def filter(self, func):
        """Filter the collection's event list with the supplied function.
        The function will be passed each of the Event objects and return
        a boolean value. If True, then it will be included in the filter.

        ::

            def is_even(event):
                return bool(event.get('some_value') % 2 == 0)

        :param func: Funtion to filter with.
        :type func: func
        :returns: Collection
        """
        flt_events = list()

        for i in self.events():
            if func(i):
                flt_events.append(i)

        return Collection(flt_events)

    def map(self, func):
        """Map function. Apply function to the collection events
        and return a new Collection from the resulting events. Function
        must creat a new Event* instance.

        ::

            def in_only(event):
                # make new events wtin only data value "in".
                return Event(event.timestamp(), {'in': event.get('in')})

        :param func: Mapper function
        :type func: func
        :returns: Collection
        """
        mapped_events = list()

        for i in self.events():
            mapped_events.append(func(i))

        return Collection(mapped_events)

    def clean(self, field_spec):
        """
        Returns a new Collection by testing the fieldSpec
        values for being valid (not NaN, null or undefined).
        The resulting Collection will be clean for that fieldSpec.

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: Collection
        """
        fspec = self._field_spec_to_array(field_spec)
        flt_events = list()

        for i in self.events():
            if Event.is_valid_value(i, fspec):
                flt_events.append(i)

        return Collection(flt_events)

    def collapse(self, field_spec_list, name, reducer, append=True):
        """
        Takes a fieldSpecList (list of column names) and collapses
        them to a new column which is the reduction of the matched columns
        in the fieldSpecList.

        :param field_spec_list: List of columns to collapse.
        :type field_spec_list: list
        :param name: Name of new column containing collapsed data.
        :type name: str
        :param reducer: Function to pass to reducer.
        :type reducer: func
        :param append: Append collapsed column to existing data dict or make new (default: False).
        :type append: bool
        """
        fsl = self._field_spec_to_array(field_spec_list)

        collapsed_events = list()

        for evn in self.events():
            collapsed_events.append(evn.collapse(fsl, name, reducer, append))

        return Collection(collapsed_events)

    def _field_spec_to_array(self, fspec):  # pylint: disable=no-self-use
        """split the field spec if it is not already a list."""
        if isinstance(fspec, list):
            return fspec
        elif isinstance(fspec, str):
            return fspec.split('.')

    # sum/min/max etc

    def count(self):
        """Get count - calls size()

        :returns: int -- Number of events in the collection.
        """
        return self.size()

    def aggregate(self, func, field_spec=['value']):  # pylint: disable=dangerous-default-value
        """
        Aggregates the events down using a user defined function to
        do the reduction.

        :param func: Function to pass to map reduce to perform the aggregation.
        :type func: func
        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: dict -- dict of reduced/aggregated values.
        """
        fspec = self._field_spec_to_array(field_spec)
        result = Event.map_reduce(self.event_list_as_list(), fspec, func)
        return result

        # pylint: disable=dangerous-default-value

    def first(self, field_spec=['value']):
        """Get first value in the collection for the fspec

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: depends on value
        """
        return self.aggregate(Functions.first, field_spec)

    def last(self, field_spec=['value']):
        """Get last value in the collection for the fspec

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: depends on value
        """
        return self.aggregate(Functions.last, field_spec)

    def sum(self, field_spec=['value']):
        """Get sum

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: int or float
        """
        return self.aggregate(Functions.sum, field_spec)

    def avg(self, field_spec=['value']):
        """Get avg

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: int or float
        """
        return self.aggregate(Functions.avg, field_spec)

    def max(self, field_spec=['value']):
        """Get max

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: int or float
        """
        return self.aggregate(Functions.max, field_spec)

    def min(self, field_spec=['value']):
        """Get min

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: int or float
        """
        return self.aggregate(Functions.min, field_spec)

    def mean(self, field_spec=['value']):
        """Get mean

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: int or float
        """
        return self.avg(field_spec)

    def median(self, field_spec=['value']):
        """Get median

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: int or float
        """
        return self.aggregate(Functions.median, field_spec)

    def stdev(self, field_spec=['value']):
        """Get std dev

        :param field_spec: Field spec to values. "Deep" syntax either
            'deep.value' or ['deep', 'value']
        :type field_spec: list or str
        :returns: int or float
        """
        return self.aggregate(Functions.stddev, field_spec)

    def __str__(self):
        """call to_string()

        :returns: str - String representation of the object.
        """
        return self.to_string()

    @staticmethod
    def equal(coll1, coll2):
        """
        Test to see if instances are the *same instance*.

        :param coll1: A collection.
        :type coll1: Collection
        :param coll2: A collection.
        :type coll2: Collection
        :returns: bool
        """
        # pylint: disable=protected-access
        return bool(
            coll1._type is coll2._type and
            coll1._event_list is coll2._event_list
        )

    @staticmethod
    def same(coll1, coll2):
        """
        Test to see if the collections *have the same values*.

        :param coll1: A collection.
        :type coll1: Collection
        :param coll2: A collection.
        :type coll2: Collection
        :returns: bool
        """
        # pylint: disable=protected-access
        return bool(
            coll1._type == coll2._type and
            coll1._event_list == coll2._event_list
        )
