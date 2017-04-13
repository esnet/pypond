#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Implementation of the Pond Event classes.

http://software.es.net/pond/#/events
"""

# Sorry pylint, I've abstracted out all I can and there are lots of docstrings.
# pylint: disable=too-many-lines

import collections
import copy
import datetime
import json

from functools import reduce  # pylint: disable=redefined-builtin

import six

from pyrsistent import thaw, freeze, PMap, pmap

from .bases import PypondBase
from .exceptions import EventException, NAIVE_MESSAGE
from .range import TimeRange
from .index import Index
from .functions import Functions, f_check
from .util import (
    dt_from_ms,
    dt_is_aware,
    format_dt,
    is_function,
    is_pmap,
    is_pvector,
    is_valid,
    ms_from_dt,
    sanitize_dt,
)


class EventBase(PypondBase):
    """
    Common code for the event classes.

    Parameters
    ----------
    underscore_d : pyrsistent.pmap
        Immutable dict-like object containing the payload for the
        events.
    """
    __slots__ = ('_d',)

    def __init__(self, underscore_d):
        """Constructor for base class.
        """
        # initialize common code
        super(EventBase, self).__init__()

        # pylint doesn't like self._d but be consistent w/original code.
        # pylint: disable=invalid-name

        # immutable pmap object, holds payload for all subclasses.
        self._d = underscore_d

    # common methods

    def data(self):
        """Direct access to the event data. The result will be an pyrsistent.pmap.

        Returns
        -------
        pyrsistent.pmap
            The immutable data payload.
        """
        return self._d.get('data')

    def get(self, field_path=None):
        """
        Get specific data out of the Event. The data will be converted
        to a js object. You can use a fieldSpec to address deep data.
        A fieldSpec could be "a.b" or it could be ['a', 'b']. Favor
        the list version please.

        The field spec can have an arbitrary number of "parts."

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        various
            Type depends on underyling data
        """

        fspec = self._field_path_to_array(field_path)

        try:
            return reduce(PMap.get, fspec, self.data())
        except TypeError:
            msg = 'Error retrieving deep field_path: {0}'.format(fspec)
            msg += ' -- all path segments other than terminal one must return a pmap'
            raise EventException(msg)

    def value(self, field_path=None):
        """
        Alias for get()

        Parameters
        ----------
        field_path : str, list, tuple, None
            Name of value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        various
            Type depends on underlying data.
        """
        return self.get(field_path)

    def to_json(self):
        """abstract, override in subclasses.

        Raises
        ------
        NotImplementedError
            Needs to be implemented in subclasses.
        """
        raise NotImplementedError  # pragma: nocover

    def to_string(self):
        """
        Retruns the Event as a string, useful for serialization.
        It's a JSON string of the whole object.

        In JS land, this is synonymous with __str__ or __unicode__

        Returns
        -------
        str
            String representation of this object.
        """
        return json.dumps(self.to_json())

    def stringify(self):
        """Produce a json string of the internal data.

        Returns
        -------
        str
            String representation of this object's internal data.
        """
        return json.dumps(thaw(self.data()))

    def __str__(self):
        """call to_string()"""
        return self.to_string()

    def __eq__(self, other):
        """equality operator. need this to be able to check if
        the event_list in a collection is the same as another.

        Parameters
        ----------
        other : Event
            Event object for == comparison.

        Returns
        -------
        bool
            True if other event has same payload.
        """
        return bool(self._d == other._d)  # pylint: disable=protected-access

    def timestamp(self):
        """abstract, override in subclass

        Raises
        ------
        NotImplementedError
            Needs to be implemented in subclasses.
        """
        raise NotImplementedError  # pragma: nocover

    def begin(self):
        """abstract, override in subclass

        Raises
        ------
        NotImplementedError
            Needs to be implemented in subclasses.
        """
        raise NotImplementedError  # pragma: nocover

    def end(self):
        """abstract, override in subclass

        Raises
        ------
        NotImplementedError
            Needs to be implemented in subclasses.
        """
        raise NotImplementedError  # pragma: nocover

    @property
    def ts(self):
        """A property to expose the datetime.datetime value returned
        by the timestamp() method.  This is so we can support sorting
        of a list of events via the following method:

            ordered = sorted(self._event_list, key=lambda x: x.ts)

        Returns
        -------
        datetime.datetime
            Returns the value returned by timestamp()
        """
        return self.timestamp()

    # static methods, primarily for arg processing.

    @staticmethod
    def timestamp_from_arg(arg):
        """extract timestamp from a constructor arg.

        Parameters
        ----------
        arg : int or datetime.datetime
            Time value as passed to one of the constructors

        Returns
        -------
        datetime.datetime
            Datetime object that has been sanitized

        Raises
        ------
        EventException
            Does not accept unaware datetime objects.
        """
        if isinstance(arg, six.integer_types):
            return dt_from_ms(arg)
        elif isinstance(arg, datetime.datetime):
            if not dt_is_aware(arg):
                raise EventException(NAIVE_MESSAGE)

            return sanitize_dt(arg)
        else:
            raise EventException('Unable to get datetime from {a} - should be a datetime object or an integer in epoch ms.'.format(a=arg))  # pylint: disable=line-too-long

    @staticmethod
    def timerange_from_arg(arg):
        """create TimeRange from a constructor arg.

        Parameters
        ----------
        arg : list, tuple, pvector or TimeRange
            Time value as passed to one of the constructors.

        Returns
        -------
        TimeRange
            New TimeRange instance from args

        Raises
        ------
        EventException
            Raised on invalid arg.
        """
        if isinstance(arg, TimeRange):
            return arg
        elif isinstance(arg, (list, tuple)) or is_pvector(arg):
            return TimeRange(arg)
        else:
            raise EventException('Unable to create TimeRange out of arg {arg}'.format(arg=arg))

    @staticmethod
    def index_from_args(instance_or_index, utc=True):
        """create Index from a constructor arg.

        Parameters
        ----------
        instance_or_index : Index or str
            Index value as passed to a constructor
        utc : bool, optional
            Use utc time internally, please don't not do this.

        Returns
        -------
        Index
            New Index object from args.

        Raises
        ------
        EventException
            Raised on invalid arg.
        """
        if isinstance(instance_or_index, six.string_types):
            return Index(instance_or_index, utc)
        elif isinstance(instance_or_index, Index):
            return instance_or_index
        else:
            msg = 'can not get index from {arg} - must be a string or Index'.format(
                arg=instance_or_index)
            raise EventException(msg)

    @staticmethod
    def data_from_arg(arg):
        """extract data from a constructor arg and make immutable.

        Parameters
        ----------
        arg : dict, pmap, int, float, str
            Data payloas as passed to one of the constructors. If dict or
            pmap, that is used as the data payload, if other value, then
            presumed to be a simple payload of {'value': arg}.

        Returns
        -------
        pyrsistent.pmap
            Immutable dict-like object

        Raises
        ------
        EventException
            Raised on bad arg input.
        """
        if isinstance(arg, dict):
            return freeze(arg)
        elif is_pmap(arg):
            return copy.copy(arg)
        elif isinstance(arg, int) or isinstance(arg, float) or isinstance(arg, str):
            return freeze({'value': arg})
        else:
            raise EventException('Could not interpret data from {a}'.format(a=arg))


class Event(EventBase):  # pylint: disable=too-many-public-methods
    """
    A generic event. This represents a data object at a single timestamp,
    supplied at initialization.

    The timestamp may be a python date object, datetime object, or
    ms since UNIX epoch. It is stored internally as a datetime object.

    The data may be any type.

    Asking the Event object for the timestamp returns an integer copy
    of the number of ms since the UNIX epoch. There's no method on
    the Event object to mutate the Event timestamp after it is created.

    The creation of an Event is done by combining two parts:
    the timestamp (or time range, or Index...) and the data.

    To construct you specify the timestamp as either:

    - a python date or datetime object
    - millisecond timestamp: the number of ms since the UNIX epoch

    To specify the data you can supply either:

    - a python dict
    - a pyrsistent.PMap created with pyrsistent.freeze(), or
    - a simple type such as an integer. In the case of the simple type
      this is a shorthand for supplying {"value": v}.

    If supplying a PMap for either of the args (rather than supplying
    a python dict and letting the Event class handle it which is
    preferred), create it with freeze() and not pmap(). This is because
    any nested dicts must similarly be made immutable and pmap() will
    only freeze the "outer" dict.

    Parameters
    ----------
    instance_or_time : Event, pyrsistent.PMap, int, datetime.datetime
        An event for copy constructor, a fully formed and formatted
        immutable data payload, or an int (epoch ms) or a
        datetime.datetime object to create a timestamp from.
    data : None, optional
        Could be dict/PMap/int/float/str to use for data payload.
    """
    __slots__ = ()  # inheriting relevant slots, stil need this

    def __init__(self, instance_or_time, data=None):
        """
        Create a basic event.
        """
        # pylint doesn't like self._d but be consistent w/original code.
        # pylint: disable=invalid-name

        if isinstance(instance_or_time, Event):
            super(Event, self).__init__(instance_or_time._d)  # pylint: disable=protected-access
            return

        if is_pmap(instance_or_time) and 'time' in instance_or_time \
                and 'data' in instance_or_time:
            super(Event, self).__init__(instance_or_time)
            return

        time = self.timestamp_from_arg(instance_or_time)
        data = self.data_from_arg(data)

        super(Event, self).__init__(pmap(dict(time=time, data=data)))

    # Query/accessor methods

    def _get_epoch_ms(self):
        return ms_from_dt(self.timestamp())

    def to_json(self):
        """
        Returns the Event as a JSON object, essentially

        ::

            {time: ms_since_epoch, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual data structure from the object internal data.

        Returns
        -------
        dict
            time/data keys
        """
        return dict(
            time=self._get_epoch_ms(),
            data=thaw(self.data()),
        )

    def to_point(self, cols=None):
        """
        Returns a flat array starting with the timestamp, followed by the values.
        Can be given an optional list of columns so the returned list will
        have the values in order. Primarily for the TimeSeries wire format.

        Parameters
        ----------
        cols : list, optional
            List of data columns to order the data points in so the
            TimeSeries wire format lines up correctly. If not specified,
            the points will be whatever order that dict.values() decides
            to return it in.

        Returns
        -------
        list
            Epoch ms followed by points.
        """
        points = [self._get_epoch_ms()]

        data = thaw(self.data())

        if isinstance(cols, list):
            points += [data.get(x, None) for x in cols]
        else:
            points += [x for x in list(data.values())]

        return points

    def timestamp_as_utc_string(self):
        """The timestamp of this data, in UTC time, as a formatted string.

        Returns
        -------
        str
            Formatted data string.
        """
        return format_dt(self.timestamp())

    def timestamp_as_local_string(self):
        """The timestamp of this data, in Local time, as a formatted string.

        Returns
        -------
        str
            Formatted data string.
        """
        return format_dt(self.timestamp(), localize=True)

    def timestamp(self):
        """The timestamp of this data

        Returns
        -------
        datetime.datetime
            Datetime object
        """
        return self._d.get('time')

    def begin(self):
        """The begin time of this Event, which will be just the timestamp.

        Returns
        -------
        datetime.datetime
            Datetime object
        """
        return self.timestamp()

    def end(self):
        """The end time of this Event, which will be just the timestamp.

        Returns
        -------
        datetime.datetime
            Datetime object
        """
        return self.timestamp()

    # data setters, returns new object

    def set_data(self, data):
        """Sets the data portion of the event and returns a new Event.

        Parameters
        ----------
        data : dict
            New data payload for this event object.

        Returns
        -------
        Event
            A new event object.
        """
        new_d = self._d.set('data', self.data_from_arg(data))
        return Event(new_d)

    def collapse(self, field_spec_list, name, reducer, append=False):
        """
        Collapses this event's columns, represented by the fieldSpecList
        into a single column. The collapsing itself is done with the reducer
        function. Optionally the collapsed column could be appended to the
        existing columns, or replace them (the default).

        Parameters
        ----------
        field_spec_list : list
            List of columns to collapse. If you need to retrieve deep
            nested values that ['can.be', 'done.with', 'this.notation'].
        name : str
            Name of new column with collapsed data.
        reducer : function
            Function to pass to reducer.
        append : bool, optional
            Set True to add new column to existing data dict, False to create
            a new Event with just the collapsed data.

        Returns
        -------
        Event
            New event object.
        """
        data = thaw(self.data()) if append else dict()
        vals = list()

        for i in field_spec_list:
            vals.append(self.get(i))

        data[name] = reducer(vals)

        return self.set_data(data)

    def key(self):
        """Return timestamp as ms since epoch

        Returns
        -------
        int
            ms since epoch.
        """
        return ms_from_dt(self.timestamp())

    def type(self):  # pylint: disable=no-self-use
        """Return type of the event object

        Returns
        -------
        class
            Return the class of thise event type.
        """
        return Event

    # Static class methods

    @staticmethod
    def is_duplicate(event1, event2, ignore_values=True):
        """Returns if the two supplied events are duplicates
        of each other. By default, duplicated means that the
        timestamps are the same. This is the case with incoming events
        where the second event is either known to be the same (but
        duplicate) of the first, or supersedes the first. You can
        also pass in false for ignoreValues and get a full
        compare.

        Parameters
        ----------
        event1 : Event, IndexedEvent or TimeSeriesEvent
            One of the event variants.
        event2 : Event, IndexedEvent or TimeSeriesEvent
            One of the event variants.
        ignore_values : bool, optional
            If set to True, the values of the events will be compared
            as well. The default means only the type and key will
            be compared.

        Returns
        -------
        TYPE
            Description
        """

        if ignore_values is True:
            return bool(event1.type() == event2.type() and
                        event1.key() == event2.key())
        else:
            return bool(event1.type() == event2.type() and
                        Event.same(event1, event2))

    @staticmethod
    def same(event1, event2):
        """
        Different name for is() which is an invalid method name.
        Different than __eq__ - see Object.is() JS documentation.

        Check if the two objects are the same.

        Parameters
        ----------
        event1 : Event
            An event.
        event2 : Event
            Another event.

        Returns
        -------
        bool
            Returns True if the event payloads is the same.
        """
        # pylint: disable=protected-access

        if event1.type() != event2.type():
            return False

        from .indexed_event import IndexedEvent
        from .timerange_event import TimeRangeEvent

        extended_map = {
            IndexedEvent: 'index',
            TimeRangeEvent: 'range',
        }

        if event1.type() in extended_map:
            key_attr = extended_map.get(event1.type())
            return bool(
                str(event1.get(key_attr)) == str(event2.get(key_attr)) and
                event1._d.get('data') == event2._d.get('data')
            )
        else:
            # regular events
            return bool(is_pmap(event1._d) and is_pmap(event2._d) and
                        event1._d == event2._d)

    @staticmethod
    def is_valid_value(event, field_path=None):
        """
        The same as Event.value() only it will return false if the
        value is either undefined, NaN or Null.

        Parameters
        ----------
        event : Event
            An event.
        field_path : str, list, tuple, None, optional
            Name of value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        bool
            Return false if undefined, NaN, or None.
        """
        val = event.value(field_path)

        return is_valid(val)

    @staticmethod
    def selector(event, field_spec=None):
        """
        Function to select specific fields of an event using
        a fieldSpec and return a new event with just those fields.

        The fieldSpec currently can be:

        * A single field name
        * An list of field names

        The function returns a new event.

        Parameters
        ----------
        event : Event
            Event to pull from.
        field_spec : str, list, tuple, None, optional
            Column or columns to look up. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this.  If None, the default
            column 'value' will be used.

        Returns
        -------
        Event
            A new event object.
        """
        new_dict = dict()

        if isinstance(field_spec, str):
            new_dict[field_spec] = event.get(field_spec)
        elif isinstance(field_spec, (list, tuple)):
            for i in field_spec:
                if isinstance(i, str):
                    new_dict[i] = event.get(i)
        elif field_spec is None:
            new_dict['value'] = event.get()
        else:
            return event

        return event.set_data(new_dict)

    # merge methods (deal in lists of events)

    @staticmethod
    def merge(events):
        """Merges multiple `events` together into a new array of events, one
        for each time/index/timerange of the source events. Merging is done on
        the data of each event. Values from later events in the list overwrite
        early values if fields conflict.

        Common use cases:

        * append events of different timestamps
        * merge in events with one field to events with another
        * merge in events that supersede the previous events

        Parameters
        ----------
        events : list
            A list of a homogenous kind of event.

        Returns
        -------
        list
            A list of the merged events.

        Raises
        ------
        EventException
            Raised if event list is not homogenous.
        """
        # need to defer import on these static methods to avoid
        # circular import errors.
        from .indexed_event import IndexedEvent
        from .timerange_event import TimeRangeEvent

        if isinstance(events, list) or is_pvector(events):
            if len(events) == 0:
                return list()

        # ordered to retain ordering of events as passed in
        event_map = collections.OrderedDict()
        type_map = dict()

        def group_by_time(event):
            """Group by the time (the key), as well as keeping track
            of the event types so we can check that for a given key
            they are homogeneous and also so we can build an output
            event for this key"""

            typ = event.type()
            key = event.key()

            if key not in event_map:
                event_map[key] = list()

            event_map[key].append(event)

            if key not in type_map:
                type_map[key] = typ
            else:
                if type_map[key] != typ:
                    msg = 'Events for time {0} are not homogenous'.format(key)
                    raise EventException(msg)

        for i in events:
            group_by_time(i)

        def dict_merge(dct, merge_dct):
            """Merge two dicts, dct will be updated with values in merge_dct."""
            for k, _ in list(merge_dct.items()):
                if (k in dct and isinstance(dct[k], dict) and
                        isinstance(merge_dct[k], collections.Mapping)):
                    dict_merge(dct[k], merge_dct[k])
                else:
                    dct[k] = merge_dct[k]

        out_events = list()

        for key, events in list(event_map.items()):
            data = dict()

            for i in events:
                dict_merge(data, thaw(i.data()))

            typ = type_map.get(key)

            if typ == Event:
                out_events.append(Event(key, data))
            elif typ == IndexedEvent:
                out_events.append(IndexedEvent(key, data))
            elif typ == TimeRangeEvent:
                args = key.split(',')
                trange = TimeRange(int(args[0]), int(args[1]))
                out_events.append(TimeRangeEvent(trange, data))

        return out_events

    @staticmethod
    def combine(events, field_spec, reducer):  # pylint: disable=too-many-locals, too-many-branches
        """Combines multiple `events` together into a new array of events, one
        for each time/index/timerange of the source events. The list of
        events may be specified as an array or `Immutable.List`. Combining acts
        on the fields specified in the `fieldSpec` and uses the reducer
        function to take the multiple values and reducer them down to one.

        The return result will be an of the same form as the input. If you
        pass in an array of events, you will get an array of events back. If
        you pass an `Immutable.List` of events then you will get an
        `Immutable.List` of events back.

        This is the general version of `Event.sum()` and `Event.avg()`. If those
        common use cases are what you want, just use those functions. If you
        want to specify your own reducer you can use this function.

        See also: `TimeSeries.timeSeriesListSum()`

        Parameters
        ----------
        events : list
            List of Event objects
        field_spec : string, list
            Column or columns to look up. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this.  If None, all columns
            will be operated on.
        reducer : function
            Reducer function to apply to column data

        Returns
        -------
        list
            List of new events

        Raises
        ------
        EventException
            Raised if illegal input is received.
        """

        # need to defer import on these static methods to avoid
        # circular import errors.
        from .indexed_event import IndexedEvent
        from .timerange_event import TimeRangeEvent

        if isinstance(events, list) or is_pvector(events):
            if len(events) == 0:
                return list()

        field_names = None

        if isinstance(field_spec, str):
            field_names = [field_spec]
        elif isinstance(field_spec, (list, tuple)):
            field_names = field_spec

        # ordered to retain ordering of events as passed in
        event_map = collections.OrderedDict()
        type_map = dict()

        def group_by_time(event):
            """Group by the time (the key), as well as keeping track
            of the event types so we can check that for a given key
            they are homogeneous and also so we can build an output
            event for this key"""

            typ = event.type()
            key = event.key()

            if key not in event_map:
                event_map[key] = list()

            event_map[key].append(event)

            if key not in type_map:
                type_map[key] = typ
            else:
                if type_map[key] != typ:
                    msg = 'Events for time {0} are not homogenous'.format(key)
                    raise EventException(msg)

        for i in events:
            group_by_time(i)

        out_events = list()

        for key, events in list(event_map.items()):
            map_event = dict()

            for event in events:

                if field_names is None:
                    field_names = list(thaw(event.data()).keys())

                for field in field_names:
                    if field not in map_event:
                        map_event[field] = list()
                    map_event[field].append(event.data().get(field))

            data = dict()
            for field_name, values in list(map_event.items()):
                data[field_name] = reducer(values)

            typ = type_map[key]

            if typ == Event:
                out_events.append(Event(key, data))
            elif typ == IndexedEvent:
                out_events.append(IndexedEvent(key, data))
            elif typ == TimeRangeEvent:
                args = key.split(',')
                trange = TimeRange(int(args[0]), int(args[1]))
                out_events.append(TimeRangeEvent(trange, data))

        return out_events

    # these call combine with appropriate reducer

    @staticmethod
    def sum(events, field_spec=None, filter_func=None):
        """combine() called with a summing function as a reducer. All
        of the events need to have the same timestamp.

        Parameters
        ----------
        events : list
            A list of Event objects
        field_spec : list, str, None, optional
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
        list
            A list containing the summed events.

        Raises
        ------
        EventException
            Raised on mismatching timestamps.
        """

        summ = Event.combine(events, field_spec, Functions.sum(f_check(filter_func)))

        return summ

    @staticmethod
    def avg(events, field_spec=None, filter_func=None):
        """combine() called with a averaging function as a reducer.

        Parameters
        ----------
        events : list
            A list of Event objects
        field_spec : list, str, None, optional
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
        list
            A list containing the averaged events.
        """

        avg = Event.combine(events, field_spec, Functions.avg(f_check(filter_func)))

        return avg

    # map, reduce, etc

    @staticmethod
    def map(events, field_spec=None):
        """
        Maps a list of events according to the selection
        specification in. The spec may be a single
        field name, a list of field names, or a function
        that takes an event and returns a key/value pair.

        ::

            Example 1

                    in   out
             3am    1    2
             4am    3    4

            result ->  {in: [1, 3], out: [2, 4]}

        Parameters
        ----------
        events : list
            A list of events
        field_spec : str, list, func or None, optional
            Column or columns to map. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this. If None, then
            all columns will be mapped.

            If field_spec is a function, the function should return a
            dict. The keys will be come the "column names" that will
            be used in the dict that is returned.

        Returns
        -------
        dict
            A dict of mapped columns/values.
        """
        result = dict()

        def key_check(k):
            """add needed keys to result"""
            if k not in result:
                result[k] = list()

        if isinstance(field_spec, str):
            for evt in events:
                key_check(field_spec)
                result[field_spec].append(evt.get(field_spec))
        elif isinstance(field_spec, (list, tuple)):
            for spec in field_spec:
                for evt in events:
                    key_check(spec)
                    result[spec].append(evt.get(spec))
        elif is_function(field_spec):
            for evt in events:
                pairs = field_spec(evt)
                for k, v in list(pairs.items()):
                    key_check(k)
                    result[k].append(v)
        else:
            # type not found or None or none - map everything
            for evt in events:
                for k, v in list(evt.data().items()):
                    key_check(k)
                    result[k].append(v)

        return result

    @staticmethod
    def reduce(mapped, reducer):
        """
        Takes a list of events and a reducer function and returns
        a new Event with the result, for each column. The reducer is
        of the form

        ::

            function sum(valueList) {
                return calcValue;
            }

        Parameters
        ----------
        mapped : dict
            Dict as produced by map()
        reducer : function
            The reducer function.

        Returns
        -------
        dict
            A dict of reduced values.
        """
        result = dict()

        for k, v in list(mapped.items()):
            result[k] = reducer(v)

        return result

    @staticmethod
    def map_reduce(events, field_spec, reducer):
        """map and reduce

        Parameters
        ----------
        events : list
            A list of events
        field_spec : str, list, func or None, optional
            Column or columns to map. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this. If None, then
            all columns will be mapped.
        reducer : function
            The reducer function.

        Returns
        -------
        dict
            A dict as returned by reduce()
        """
        return Event.reduce(Event.map(events, field_spec), reducer)
