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
import math

from pyrsistent import thaw, pvector

from .event import Event
from .exceptions import CollectionException, CollectionWarning, UtilityException
from .functions import Functions, f_check
from .io.input import Bounded
from .range import TimeRange
from .util import (
    _check_dt,
    is_function,
    is_pvector,
    ObjectEncoder,
    unique_id,
)


class Collection(Bounded):  # pylint: disable=too-many-public-methods
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


    Parameters
    ----------
    instance_or_list : list, Collection, pyrsistent.pvector
        A collection object to copy or a list of Event objects
    copy_events : bool, optional
        Copy event list when using copy constructor, otherwise the
        new object has an emtpy event list.
    """

    def __init__(self, instance_or_list=None, copy_events=True):
        """
        Create a collection object.
        """
        super(Collection, self).__init__()

        self._id = unique_id('collection-')
        self._event_list = None
        self._type = None

        if instance_or_list is None:
            self._event_list = pvector(list())
        elif isinstance(instance_or_list, Collection):
            other = instance_or_list
            if copy_events:
                # pylint: disable=protected-access
                self._event_list = other._event_list
                self._type = other._type
            else:
                self._event_list = pvector(list())

        elif isinstance(instance_or_list, list):
            for i in instance_or_list:
                self._check(i)
            self._event_list = pvector(instance_or_list)

        elif is_pvector(instance_or_list):
            self._event_list = instance_or_list
            for i in self._event_list:
                self._check(i)

        else:
            msg = 'Arg was not a Collection, list or pvector - '
            msg += 'initializing event list to empty pvector'
            self._warn(msg, CollectionWarning)

            self._event_list = pvector(list())

    def to_json(self):
        """
        Returns the collection as json object.

        This is actually like json.loads(s) - produces the
        actual vanilla data structure.

        Returns
        -------
        list
            A thawed list of Event objects.
        """
        return thaw(self._event_list)

    def to_string(self):
        """
        Retruns the collection as a string, useful for serialization.

        In JS land, this is synonymous with __str__ or __unicode__

        Use custom object encoder because this is a list of Event* objects.

        Returns
        -------
        str
            String representation of this object.
        """
        return json.dumps(self.to_json(), cls=ObjectEncoder)

    def type(self):
        """
        Event object type.

        The class of the type of events in this collection.

        Returns
        -------
        Event
            The class (not instance) of the type of events.
        """
        return self._type

    def size(self):
        """Number of items in collection.

        Returns
        -------
        int
            Number of items in collection
        """
        return len(self._event_list)

    def size_valid(self, field_path=None):
        """
        Returns the number of valid items in this collection.

        Uses the fieldSpec to look up values in all events.
        It then counts the number that are considered valid,
        i.e. are not NaN, undefined or null.

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        int
            Number of valid <field_path> values in all of the Events.
        """
        count = 0

        fpath = self._field_path_to_array(field_path)

        for i in self.events():
            if Event.is_valid_value(i, fpath):
                count += 1

        return count

    def at(self, pos):  # pylint: disable=invalid-name
        """Returns an item in the collection by its index position.

        Creates a new object via copy ctor.

        Parameters
        ----------
        pos : int
            Index of the event to be retrieved.

        Returns
        -------
        Event
            A new Event object of the event at index pos

        Raises
        ------
        CollectionException
            Raised if there is an index error.
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

        Parameters
        ----------
        time : datetime.datetime
            Datetime object >= to the event to be returned. Must
            be an aware UTC datetime object.

        Returns
        -------
        Event
            Returns a new Event instance via at()
        """
        pos = self.bisect(time)

        if pos and pos < self.size():
            return self.at(pos)

    def at_first(self):
        """Retrieve the first item in this collection.

        Returns
        -------
        Event
            An event instance.
        """
        if self.size():
            return self.at(0)

    def at_last(self):
        """Return the last event item in this collection.

        Returns
        -------
        Event
            An event instance.
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

        Parameters
        ----------
        dtime : datetime.datetime
            Datetime object >= to the event to be returned. Must
            be an aware UTC datetime object.
        b : int, optional
            Array index to start searching from

        Returns
        -------
        int
            The index of the searched-for event

        Raises
        ------
        CollectionException
            Raised if given a naive or non-UTC dtime
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

        Returns
        -------
        iterator
            An iterator to loop over the events.
        """
        return iter(self._event_list)

    def set_events(self, events):
        """Create a new Collection from this one and set the internal
        list of events

        Parameters
        ----------
        events : list or pyrsistent.pvector
            A list of events

        Returns
        -------
        Collection
            Returns a new collection with the event list set to the
            everts arg

        Raises
        ------
        CollectionException
            Raised if wrong arg type.
        """
        if not isinstance(events, list) and not is_pvector(events):
            msg = 'arg must be a list or pvector'
            raise CollectionException(msg)

        ret = Collection(self)
        ret._event_list = events  # pylint: disable=protected-access
        return ret

    def event_list(self):
        """Returns the raw Immutable event list.

        Returns
        -------
        pyrsistent.pvector
            Raw immutable event list.
        """
        return self._event_list

    def event_list_as_list(self):
        """return a python list of the event list.

        Returns
        -------
        list
            Thawed version of internal immutable data structure.
        """
        return thaw(self.event_list())

    def sort_by_time(self):
        """Return a new instance of this collection after making sure
        that all of the events are sorted by timestamp.

        Returns
        -------
        Collection
            A copy of this collection with the events chronologically
            sorted.
        """
        ordered = sorted(self._event_list, key=lambda x: x.ts)
        return self.set_events(ordered)

    def sort(self, field_path):
        """Sorts the Collection using the value referenced by field_path.

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        Collection
            New collection of sorted values.
        """

        fpath = self._field_path_to_array(field_path)
        ordered = sorted(self._event_list, key=lambda x: x.get(fpath))
        return self.set_events(ordered)

    def is_chronological(self):
        """Checks that the events in this collection are in chronological
        order.

        Returns
        -------
        bool
            True if events are in chronologcal order.
        """
        ret = True
        current_ts = None

        for i in self._event_list:
            if current_ts is None:
                current_ts = i.timestamp()
            else:
                if i.timestamp() < current_ts:
                    ret = False
                current_ts = i.timestamp()

        return ret

    # Series range

    def range(self):
        """
        From the range of times, or Indexes within the TimeSeries, return
        the extents of the Collection/TimeSeries as a TimeRange.

        Returns
        -------
        TimeRange
            Extents as time range.
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

        Parameters
        ----------
        event : Event
            Event object to add to collection.

        Returns
        -------
        Collection
            New collection with the event added to it.
        """
        self._check(event)

        coll = Collection(self)
        coll._event_list = self._event_list.append(event)  # pylint: disable=protected-access

        return coll

    def slice(self, begin, end):
        """
        Perform a slice of events within the Collection, returns a new
        Collection representing a portion of this TimeSeries from begin up to
        but not including end. Uses typical python [slice:syntax].

        Parameters
        ----------
        begin : int
            Slice begin.
        end : int
            Slice end.

        Returns
        -------
        Collection
            New collection with sliced payload.
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

        Would produce a new collection where 'some_value' is only
        even numbers.

        Parameters
        ----------
        func : function
            Function to filter with.

        Returns
        -------
        Collection
            New collection containing filtered events.
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

        Parameters
        ----------
        func : function
            Mapper function

        Returns
        -------
        Collection
            New collection containing mapped events.
        """
        mapped_events = list()

        for i in self.events():
            mapped_events.append(func(i))

        return Collection(mapped_events)

    def clean(self, field_path=None):
        """
        Returns a new Collection by testing the fieldSpec
        values for being valid (not NaN, null or undefined).
        The resulting Collection will be clean for that fieldSpec.

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        Collection
            New collection containing only "clean" events.
        """
        flt_events = list()

        fpath = self._field_path_to_array(field_path)

        for i in self.events():
            if Event.is_valid_value(i, fpath):
                flt_events.append(i)

        return Collection(flt_events)

    def collapse(self, field_spec_list, name, reducer, append=True):
        """
        Takes a fieldSpecList (list of column names) and collapses
        them to a new column which is the reduction of the matched columns
        in the fieldSpecList.

        Parameters
        ----------
        field_spec_list : list
            List of columns to collapse. If you need to retrieve deep
            nested values that ['can.be', 'done.with', 'this.notation'].
        name : str
            Name of new column containing collapsed data.
        reducer : function
            Function to pass to reducer.
        append : bool, optional
            Append collapsed column to existing data or make new events with
            only that column.

        Returns
        -------
        Collection
            New collection containing the collapsed data.
        """
        collapsed_events = list()

        for evn in self.events():
            collapsed_events.append(evn.collapse(field_spec_list, name, reducer, append))

        return Collection(collapsed_events)

    # sum/min/max etc

    def count(self):
        """Get count - calls size()

        Returns
        -------
        int
            Num events in the collection.
        """
        return self.size()

    def aggregate(self, func, field_path=None):
        """
        Aggregates the events down using a user defined function to
        do the reduction. Only a single column can be aggregated on
        so this takes a field_path, NOT a field_spec.

        This is essentially a wrapper around map/reduce, constraining
        it to a single column and returning the value, not the dict
        from map().

        Parameters
        ----------
        func : function
            Function to pass to map reduce to aggregate.
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        various
            Returns the aggregated value, so it depends on what kind
            of data are being handled/aggregation being done.
        """

        if not is_function(func):
            msg = 'First arg to aggregate() must be a function'
            raise CollectionException(msg)

        fpath = None

        if isinstance(field_path, str):
            fpath = field_path
        elif isinstance(field_path, (list, tuple)):
            # if the ['array', 'style', 'field_path'] is being used,
            # we need to turn it back into a string since we are
            # using a subset of the the map() functionality on
            # a single column
            fpath = '.'.join(field_path)
        elif field_path is None:
            # map() needs a field name to use as a key. Normally
            # this case is normally handled by _field_path_to_array()
            # inside get(). Also, if map(func, field_spec=None) then
            # it will map all the columns.
            fpath = 'value'
        else:
            msg = 'Collection.aggregate() takes a string/list/tuple field_path'
            raise CollectionException(msg)

        result = Event.map_reduce(self.event_list(), fpath, func)

        return result.get(fpath)

    def first(self, field_path=None, filter_func=None):
        """Get first value in the collection for the fspec

        Parameters
        ----------
        field_spec : str, list, tuple, None
            Column or columns to look up. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this.  If None, all columns
            will be operated on.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        depends on data
            Type varies depending on underlying data
        """
        return self.aggregate(Functions.first(f_check(filter_func)), field_path)

    def last(self, field_path=None, filter_func=None):
        """Get last value in the collection for the fspec

        Parameters
        ----------
        field_spec : str, list, tuple, None
            Column or columns to look up. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this.  If None, all columns
            will be operated on.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        depends on data
            Type varies depending on underlying data
        """
        return self.aggregate(Functions.last(f_check(filter_func)), field_path)

    def sum(self, field_path=None, filter_func=None):
        """Get sum

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        int or float
            Summed value.
        """
        return self.aggregate(Functions.sum(f_check(filter_func)), field_path)

    def avg(self, field_path=None, filter_func=None):
        """Get avg

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        int or float
            Average value.
        """
        return self.aggregate(Functions.avg(f_check(filter_func)), field_path)

    def max(self, field_path=None, filter_func=None):
        """Get max

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        int or float
            Maximum value.
        """
        return self.aggregate(Functions.max(f_check(filter_func)), field_path)

    def min(self, field_path=None, filter_func=None):
        """Get min

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        int or float
            Minimum value.
        """
        return self.aggregate(Functions.min(f_check(filter_func)), field_path)

    def mean(self, field_path=None, filter_func=None):
        """Get mean

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        int or float
            Mean value (grrr!).
        """
        return self.avg(field_path, filter_func)

    def median(self, field_path=None, filter_func=None):
        """Get median

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        int or float
            Median value.
        """
        return self.aggregate(Functions.median(f_check(filter_func)), field_path)

    def stdev(self, field_path=None, filter_func=None):
        """Get std dev

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.
        filter_func : function, None
            A function (static method really) from the Filters class in module
            `pypond.functions.Filters`. It will control how bad or missing
            (None, NaN, empty string) values will be cleansed or filtered
            during aggregation. If no filter is specified, then the missing
            values will be retained which will potentially cause errors.

        Returns
        -------
        int or float
            Standard deviation.
        """
        return self.aggregate(Functions.stddev(f_check(filter_func)), field_path)

    def percentile(self, perc, field_path, method='linear', filter_func=None):
        """Gets percentile perc within the Collection. This works the same
        way as numpy.

        Parameters
        ----------
        perc : int
            The percentile (should be between 0 and 100)
        field_path : str, list, tuple, None, optional
            Name of a single value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.
        method : str, optional
            Specifies the interpolation method to use when the desired
            percentile lies between two data points. Options are:

            linear: i + (j - i) * fraction, where fraction is the fractional
            part of the index surrounded by i and j.

            lower: i

            higher: j

            nearest: i or j whichever is nearest

            midpoint: (i + j) / 2

        Returns
        -------
        int or float
            The percentile.
        """
        return self.aggregate(Functions.percentile(perc, method, f_check(filter_func)),
                              field_path)

    def quantile(self, num, field_path=None, method='linear'):
        """Gets num quantiles within the Collection

        Parameters
        ----------
        num : Number of quantiles to divide the Collection into.
            Description
        field_path : None, optional
            The field to return as the quantile. If not set, defaults
            to 'value.'
        method : str, optional
            Specifies the interpolation method to use when the desired
            percentile lies between two data points. Options are:

            linear: i + (j - i) * fraction, where fraction is the fractional
            part of the index surrounded by i and j.

            lower: i

            higher: j

            nearest: i or j whichever is nearest

            midpoint: (i + j) / 2

        Returns
        -------
        list
            An array of quantiles
        """
        results = list()
        sorted_coll = self.sort(field_path)
        subsets = 1.0 / num

        if num > self.size():
            msg = 'Subset num is greater than the Collection length'
            raise CollectionException(msg)

        i = copy.copy(subsets)

        while i < 1:

            index = int(math.floor(((sorted_coll.size() - 1) * i)))

            if index < sorted_coll.size() - 1:

                fraction = (sorted_coll.size() - 1) * i - index
                val0 = sorted_coll.at(index).get(field_path)
                val1 = sorted_coll.at(index + 1).get(field_path)

                val = None

                if method == 'lower' or fraction == 0:
                    val = val0
                elif method == 'linear':
                    val = val0 + (val1 - val0) * fraction
                elif method == 'higher':
                    val = val1
                elif method == 'nearest':
                    val = val0 if fraction < .5 else val1
                elif method == 'midpoint':
                    val = (val0 + val1) / 2

                results.append(val)

            i += subsets

        return results

    def __str__(self):
        """call to_string()

        to_string() is already being tested so skip coverage.

        Returns
        -------
        str
            String representation of the object.
        """
        return self.to_string()  # pragma: no cover

    @staticmethod
    def equal(coll1, coll2):
        """
        Test to see if instances are the *same instance*.

        Parameters
        ----------
        coll1 : Collection
            A collection.
        coll2 : Collection
            Another collection.

        Returns
        -------
        bool
            True if same instance.
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

        Parameters
        ----------
        coll1 : Collection
            A collection.
        coll2 : Collection
            Another collection.

        Returns
        -------
        bool
            True if same values.
        """
        # pylint: disable=protected-access
        return bool(
            coll1._type == coll2._type and
            coll1._event_list == coll2._event_list
        )
