"""
Implementation of Pond Collection class.
"""

import copy
import json

from pyrsistent import freeze, thaw

from .sources import BoundedIn
from .event import Event
from .exceptions import CollectionException, CollectionWarning
from .range import TimeRange
from .util import unique_id, is_pvector, ObjectEncoder


class Collection(BoundedIn):  # pylint: disable=too-many-public-methods
    """
    A collection is a list of Events. You can construct one out of either
    another collection, or a list of Events. You can addEvent() to a collection
    and a new collection will be returned.

    Basic operations on the list of events are also possible. You
    can iterate over the collection with a for..of loop, get the size()
    of the collection and access a specific element with at().
    """
    def __init__(self, instance_or_list, copy_events=True):
        """
        Initialize from copy, lists, etc.

        instance_or_list can be:
            * a Collection object (copy ctor)
            * a python list
            * a pyrsistent.pvector
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
        actual vanilla data structure."""
        return thaw(self._event_list)

    def to_string(self):
        """
        Retruns the collection as a string, useful for serialization.

        In JS land, this is synonymous with __str__ or __unicode__

        Use custom object encoder because this is a list of Event* objects.
        """
        return json.dumps(self.to_json(), cls=ObjectEncoder)

    def type(self):
        """
        Event object type.

        returns Event | IndexedEvent | TimeRangeEvent

        The class of the type of events in this collection.
        """
        return self._type

    def size(self):
        """Number of items in collection."""
        return len(self._event_list)

    def size_valid(self, field_spec='value'):
        """
        Returns the number of valid items in this collection.

        Uses the fieldSpec to look up values in all events.
        It then counts the number that are considered valid,
        i.e. are not NaN, undefined or null.
        """
        count = 0

        for i in self.events():
            if Event.is_valid_value(i, field_spec):
                count += 1

        return count

    def at(self, pos):  # pylint: disable=invalid-name
        """Returns an item in the collection by its position.

        Creates a new object via copy ctor."""
        try:
            return self._type(self._event_list[pos])
        except ValueError:
            raise CollectionException('invalid index given to at()')

    def at_time(self, time):
        """Return an item by time."""
        pos = self.bisect(time)

        if pos and pos < self.size():
            return self.at(pos)

    def at_first(self):
        """First item."""
        if self.size():
            return self.at(0)

    def at_last(self):
        """Last item."""
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

        Returns index that is the greatest but still below t
        """

        i = copy.copy(b)  # paranoia
        size = self.size()

        if not size:
            return None

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
        """
        return iter(self._event_list)

    def event_list(self):
        """Returns the raw Immutable event list."""
        return self._event_list

    def event_list_as_list(self):
        """return a python list of the event list."""
        return thaw(self.event_list())

    # Series range

    def range(self):
        """
        From the range of times, or Indexes within the TimeSeries, return
        the extents of the TimeSeries as a TimeRange.
        returns the extents of the TimeSeries
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
        Add an event and return a new object.
        """
        self._check(event)
        return Collection(self._event_list.append(event))

    def slice(self, begin, end):
        """
        Perform a slice of events within the Collection, returns a new
        Collection representing a portion of this TimeSeries from begin up to
        but not including end.
        """
        sliced = Collection(self._event_list[begin:end])
        sliced._type = self._type  # pylint: disable=protected-access
        return sliced

    def filter(self, func):
        """Generate a filtered event list."""
        raise NotImplementedError

    def map(self, func):
        """Map function."""
        raise NotImplementedError

    def clean(self, field_spec):
        """
        Returns a new Collection by testing the fieldSpec
        values for being valid (not NaN, null or undefined).
        The resulting Collection will be clean for that fieldSpec.
        """
        raise NotImplementedError

    # sum/min/max etc

    def count(self, field_spec='value'):
        """Get count - calls self.size_valid(field_spec)"""
        raise NotImplementedError

    def sum(self, field_spec='value'):
        """Get sum"""
        raise NotImplementedError

    def max(self, field_spec='value'):
        """Get max"""
        raise NotImplementedError

    def min(self, field_spec='value'):
        """Get min"""
        raise NotImplementedError

    def avg(self, field_spec='value'):
        """Get avg"""
        raise NotImplementedError

    def mean(self, field_spec='value'):
        """Get mean"""
        raise NotImplementedError

    def median(self, field_spec='value'):
        """Get median"""
        raise NotImplementedError

    def stdev(self, field_spec='value'):
        """Get std dev"""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        return self.to_string()
