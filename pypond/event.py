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
import copy
import datetime
import json

# using freeze/thaw more bulletproof than pmap/pvector since data is free-form
from pyrsistent import thaw, freeze

from .bases import PypondBase
from .exceptions import EventException, NAIVE_MESSAGE
from .range import TimeRange
from .index import Index
from .functions import Functions
from .util import (
    dt_from_ms,
    dt_is_aware,
    format_dt,
    is_function,
    is_nan,
    is_pmap,
    is_pvector,
    ms_from_dt,
    sanitize_dt,
)


class EventBase(PypondBase):
    """
    Common code for the event classes.
    """
    def __init__(self, underscore_d):
        "ctor"
        # initialize common code
        super(EventBase, self).__init__()

        # pylint doesn't like self._d but be consistent w/original code.
        # pylint: disable=invalid-name

        # immutable pmap object, holds payload for all subclasses.
        self._d = underscore_d

    # common methods

    def data(self):
        """Direct access to the event data. The result will be an pyrsistent.pmap.

        :returns: pyrsistent.pmap
        """
        return self._d.get('data')

    def get(self, field_spec=['value']):  # pylint: disable=dangerous-default-value
        """
        Get specific data out of the Event. The data will be converted
        to a js object. You can use a fieldSpec to address deep data.
        A fieldSpec could be "a.b" or it could be ['a', 'b'].

        The field spec can have an arbitrary number of "parts."

        :param field_spec: Field spec of data value to get.
        :type field_spect: str/list
        :returns: various - Depends on underlying data.
        """
        if isinstance(field_spec, str):
            path = field_spec.split('.')  # pylint: disable=no-member
        elif isinstance(field_spec, list):
            path = field_spec

        return reduce(dict.get, path, thaw(self.data()))

    def value(self, field_spec=['value']):  # pylint: disable=dangerous-default-value
        """
        Alias for get()

        :param field_spec: Field spec of data value to get.
        :type field_spect: str/list
        :returns: various - Depends on underlying data.
        """
        return self.get(field_spec)

    def to_json(self):
        """abstract, override in subclasses."""
        raise NotImplementedError  # pragma: nocover

    def to_string(self):
        """
        Retruns the Event as a string, useful for serialization.
        It's a JSON string of the whole object.

        In JS land, this is synonymous with __str__ or __unicode__

        :returns: str -- String representation of this object.
        """
        return json.dumps(self.to_json())

    def stringify(self):
        """Produce a json string of the internal data.

        :returns: str -- String representation of this object's data.
        """
        return json.dumps(thaw(self.data()))

    def __str__(self):
        """call to_string()"""
        return self.to_string()

    def __eq__(self, other):
        """equality operator. need this to be able to check if
        the event_list in a collection is the same as another."""
        return bool(self._d == other._d)  # pylint: disable=protected-access

    def timestamp(self):
        """abstract, override in subclass"""
        raise NotImplementedError  # pragma: nocover

    def begin(self):
        """abstract, override in subclass"""
        raise NotImplementedError  # pragma: nocover

    def end(self):
        """abstract, override in subclass"""
        raise NotImplementedError  # pragma: nocover

    # static methods, primarily for arg processing.

    @staticmethod
    def timestamp_from_arg(arg):
        """extract timestamp from a constructor arg.

        :param arg: Time value as passed to one of the consctructors.
        :type arg: int (epoch ms) or datetime
        :returns: datetime -- Datetime object that has been sanitized.
        :raises: EventException
        """
        if isinstance(arg, int):
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

        :param arg: Time value as passed to one of the consctructors.
        :type arg: list/tuple/pvector or TimeRange
        :returns: TimeRange
        :raises: EventException
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

        :param arg: Index value as passed to one of the consctructors.
        :type arg: str or Index
        :returns: Index
        :raises: EventException
        """
        if isinstance(instance_or_index, str):
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

        :param arg: Data payload passed to one of the constructors.
        :type arg: dict/pmap/int/float/str
        :returns: pyrsisten.pmap
        :raises: EventException
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
    - a pyrsistent.PMap, or
    - a simple type such as an integer. In the case of the simple type
      this is a shorthand for supplying {"value": v}.

    :raises: EventException

    """
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

        super(Event, self).__init__(freeze(dict(time=time, data=data)))

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

        :return: dict -- time/data keys
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

        :param cols: List of data columns.
        :type cols: list/default of None.
        :returns: list -- ms since epoch folowed by data values.
        """
        points = [self._get_epoch_ms()]

        if isinstance(cols, list):
            points += [self.data().get(x, None) for x in cols]
        else:
            points += [x for x in self.data().values()]

        return points

    def timestamp_as_utc_string(self):
        """The timestamp of this data, in UTC time, as a formatted string.

        :returns: str -- formatted date string.
        """
        return format_dt(self.timestamp())

    def timestamp_as_local_string(self):
        """The timestamp of this data, in Local time, as a formatted string.

        :returns: str -- formatted date string.
        """
        return format_dt(self.timestamp(), localize=True)

    def timestamp(self):
        """The timestamp of this data

        :returns: datetime object
        """
        return self._d.get('time')

    def begin(self):
        """The begin time of this Event, which will be just the timestamp.

        :returns: datetime object
        """
        return self.timestamp()

    def end(self):
        """The end time of this Event, which will be just the timestamp.

        :returns: datetime object
        """
        return self.timestamp()

    # data setters, returns new object

    def set_data(self, data):
        """Sets the data portion of the event and returns a new Event.

        :param data: The new data portion for this event object.
        :type data: dict
        :returns: Event - a new Event object.
        """
        new_d = self._d.set('data', self.data_from_arg(data))
        return Event(new_d)

    def collapse(self, field_spec_list, name, reducer, append=False):
        """
        Collapses this event's columns, represented by the fieldSpecList
        into a single column. The collapsing itself is done with the reducer
        function. Optionally the collapsed column could be appended to the
        existing columns, or replace them (the default).

        :param field_spec_list: List of columns to collapse.
        :type field_spec_list: list
        :param name: Name of new column containing collapsed data.
        :type name: str
        :param reducer: Function to pass to reducer.
        :type reducer: func
        :param append: Append collapsed column to existing data dict or make new (default: False).
        :type append: bool
        """
        data = thaw(self.data()) if append else dict()
        vals = list()

        for i in field_spec_list:
            vals.append(self.get(i))

        data[name] = reducer(vals)

        return self.set_data(data)

    # Static class methods

    @staticmethod
    def same(event1, event2):
        """
        Different name for is() which is an invalid method name.
        Different than __eq__ - see Object.is() JS documentation.

        Check if the two objects are the same.

        :param event1: An event.
        :type event1: Event
        :param event2: An event.
        :type event2: Event
        """
        # pylint: disable=protected-access
        return bool(is_pmap(event1._d) and is_pmap(event2._d) and
                    event1._d == event2._d)

    @staticmethod
    def is_valid_value(event, field_spec='value'):
        """
        The same as Event.value() only it will return false if the
        value is either undefined, NaN or Null.

        :param field_spec: Data value to validate.
        :type field_spect: str
        """
        val = event.value(field_spec)

        return not bool(val is None or val == '' or is_nan(val))

    @staticmethod
    def selector(event, field_spec):
        """
        Function to select specific fields of an event using
        a fieldSpec and return a new event with just those fields.

        The fieldSpec currently can be:

        * A single field name
        * An list of field names

        The function returns a new event.

        :param field_spec: Fields to gather into a new object
        :type field_spec: str or list
        """
        new_dict = dict()

        if isinstance(field_spec, str):
            new_dict[field_spec] = event.get(field_spec)
        elif isinstance(field_spec, list):
            for i in field_spec:
                if isinstance(i, str):
                    new_dict[i] = event.get(i)
        else:
            return event

        return event.set_data(new_dict)

    # merge methods (deal in lists of events)

    @staticmethod
    def merge_events(events):
        """
        Merge a list of regular Event objects in to a single new Event.

        The events being merged must have the same type and must have the
        same timestamp.

        :param events: List of Events to merge.
        :type events: list
        :raises: EventException
        :returns: Event
        """
        ts_ref = events[0].timestamp()
        new_data = dict()

        for i in events:
            if not isinstance(i, Event):
                raise EventException('Events being merged must have the same type.')

            if ts_ref != i.timestamp():
                raise EventException('Events being merged need the same timestamp.')

            i_data = thaw(i.data())

            for k, v in i_data.items():
                if k in new_data:
                    raise EventException(
                        'Events being merged may not have the same key: {k}'.format(k=k))
                new_data[k] = v

        return Event(ts_ref, new_data)

    @staticmethod
    def merge_timerange_events(events):
        """
        Merge a list of TimeRangeEvent objects.

        The events being merged must have the same type and must have the
        same timestamp.

        :param events: List of TimeRangeEvents events to merge.
        :type events: list
        :raises: EventException
        :returns: TimeRangeEvent
        """

        ts_ref = events[0].timerange()
        new_data = dict()

        for i in events:
            if not isinstance(i, TimeRangeEvent):
                raise EventException('Events being merged must have the same type.')

            if i.timerange() != ts_ref:
                raise EventException('Events being merged need the same timestamp.')

            for k, v in i.data().items():
                if k in new_data:
                    raise EventException(
                        'Events being merged can not have the same key {key}'.format(key=k))
                new_data[k] = v

        return TimeRangeEvent(ts_ref, new_data)

    @staticmethod
    def merge_indexed_events(events):
        """
        Merge a list of IndexedEvent objects.

        The events being merged must have the same type and must have the
        same timestamp.

        :param events: List of IndexedEvents events to merge.
        :type events: list
        :raises: EventException
        :returns: IndexedEvent
        """
        idx_ref = events[0].index_as_string()
        new_data = dict()

        for i in events:
            if not isinstance(i, IndexedEvent):
                raise EventException('Events being merged must have the same type.')

            if idx_ref != i.index_as_string():
                raise EventException('Events being merged need the same index.')

            for k, v in i.to_json().get('data').items():
                if k in new_data:
                    raise EventException(
                        'Events being merged can not have the same key {key}'.format(key=k))

                new_data[k] = v

        return IndexedEvent(idx_ref, new_data)

    @staticmethod
    def merge(events):
        """
        This is an entry point that will grok the what kind of events
        are in the list and call one of the three Event class specific
        methods.

        :param events: - List of Event types
        :type events: list
        :returns: Event/TimeRangeEvent/IndexedEvent
        """
        if not isinstance(events, list):
            # need to be a list
            return
        elif len(events) < 1:
            # nothing to process
            return
        elif len(events) == 1:
            # just one, return it.
            return events[0]

        if isinstance(events[0], Event):
            return Event.merge_events(events)
        elif isinstance(events[0], TimeRangeEvent):
            return Event.merge_timerange_events(events)
        elif isinstance(events[0], IndexedEvent):
            return Event.merge_indexed_events(events)

    @staticmethod
    def combine(events, field_spec, reducer):
        """
        Combines multiple events with the same time together
        to form a new event. Doesn't currently work on IndexedEvents
        or TimeRangeEvents.

        :param events: List of Event objects.
        :type events: list
        :param field_spec: Columns to operate on.
        :type field_spec: str/list/None
        :param reducer: Reducer function to apply to column data.
        :type reducer: func
        :returns: List of Event
        """
        if len(events) < 1:
            return None

        def combine_mapper(event):
            """mapper function to make ts::k => value dicts"""
            map_event = dict()

            field_names = list()

            if field_spec is None:
                field_names = thaw(event.data()).keys()
            elif isinstance(field_spec, str):
                field_names = [field_spec]
            elif isinstance(field_spec, list):
                field_names = field_spec

            for i in field_names:
                map_event['{ts}::{fn}'.format(ts=ms_from_dt(event.timestamp()),
                                              fn=i)] = event.get(i)

            # return {ts::k => val, ts::k2 => val, ts::k3 => val}
            return map_event

        # dict w/ts::k => [val, val, val]
        mapped = Event.map(events, combine_mapper)

        event_data = dict()

        for k, v in Event.reduce(mapped, reducer).items():
            # ts::k with single reduced value
            tstamp, field = k.split('::')
            tstamp = int(tstamp)
            if tstamp not in event_data:
                event_data[tstamp] = dict()
            event_data[tstamp][field] = v

        # event_data 2 level dict {'1459283734515': {'a': 8, 'c': 14, 'b': 11}}

        return [Event(x[0], x[1]) for x in event_data.items()]

    # these call combine with appropriate reducer

    @staticmethod
    def sum(events, field_spec=None):
        """combine() called with a summing function as a reducer.

        :param events: List of Event.
        :type events: list
        :param field_spec: Column(s) to sum
        :type field_spec: str/list
        :returns: int/float or None
        """

        tstmp = None

        for i in events:
            if tstmp is None:
                tstmp = i.timestamp()
            else:
                if tstmp != i.timestamp():
                    msg = 'sum() expects all events to have the same timestamp'
                    raise EventException(msg)

        summ = Event.combine(events, field_spec, Functions.sum)

        if summ is not None:
            return Event.combine(events, field_spec, Functions.sum)[0]
        else:
            return None

    @staticmethod
    def avg(events, field_spec=None):
        """combine() called with a averaging function as a reducer.

        :param events: List of Event.
        :type events: list
        :param field_spec: Column(s) to sum
        :type field_spec: str/list
        :returns: int/float or None"""
        avg = Event.combine(events, field_spec, Functions.avg)
        if avg is not None:
            return Event.combine(events, field_spec, Functions.avg)[0]
        else:
            return None

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

        :param events: List of events.
        :type events: list
        :param field_spec: Data values/columns to map.
        :type field_spec: str, list, func or None
        :returns: dict -- Python dict of mapped columns/values.
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
        elif isinstance(field_spec, list):
            for spec in field_spec:
                for evt in events:
                    key_check(spec)
                    result[spec].append(evt.get(spec))
        elif is_function(field_spec):
            for evt in events:
                pairs = field_spec(evt)
                for k, v in pairs.items():
                    key_check(k)
                    result[k].append(v)
        else:
            # type not found or None or none - map everything
            for evt in events:
                for k, v in thaw(evt.data()).items():
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

        :param mapped: Dict as produced by map()
        :type mapped: dict
        :param reducer: The reducer function.
        :type reducer: func
        :returns: dict -- dict of reduced values.
        """
        result = dict()

        for k, v in mapped.items():
            result[k] = reducer(v)

        return result

    @staticmethod
    def map_reduce(events, field_spec, reducer):
        """map and reduce"""
        return Event.reduce(Event.map(events, field_spec), reducer)


class TimeRangeEvent(EventBase):
    """
    The creation of an TimeRangeEvent is done by combining two parts -
    the timerange and the data.

    To construct you specify a TimeRange, along with the data.

    The first arg can be:

    - a TimeRangeEvent instance (copy ctor)
    - a pyrsistent.PMap, or
    - a python tuple, list or pyrsistent.PVector object containing two
      python datetime objects or ms timestamps - the args for the
      TimeRange object.

    To specify the data you can supply either:

    - a python dict
    - a pyrsistent.PMap, or
    - a simple type such as an integer. In the case of the simple type
      this is a shorthand for supplying {"value": v}.


    """
    def __init__(self, instance_or_args, arg2=None):
        """
        Create a time range event.
        """
        # pylint doesn't like self._d but be consistent w/original code.
        # pylint: disable=invalid-name

        if isinstance(instance_or_args, TimeRangeEvent):
            super(TimeRangeEvent, self).__init__(instance_or_args._d)  # pylint: disable=protected-access
            return
        elif is_pmap(instance_or_args):
            super(TimeRangeEvent, self).__init__(instance_or_args)
            return

        rng = self.timerange_from_arg(instance_or_args)
        data = self.data_from_arg(arg2)

        super(TimeRangeEvent, self).__init__(freeze(dict(range=rng, data=data)))

        # Query/accessor methods

    def to_json(self):
        """
         Returns the TimeRangeEvent as a JSON object, essentially

        ::

            {timerange: tr, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual data structure from the object internal data.

        :return: dict -- timerange/data keys"""
        return dict(
            timerange=self.timerange().to_json(),
            data=thaw(self.data()),
        )

    def to_point(self, cols=None):
        """
        Returns a flat array starting with the timestamp, followed by the values.

        Can be given an optional list of columns so the returned list will
        have the values in order. Primarily for the TimeSeries wire format.

        :param cols: List of data columns.
        :type cols: list/default of None.
        :returns: list -- ms since epoch folowed by data values.
        """
        points = [self.timerange().to_json()]

        if isinstance(cols, list):
            points += [self.data().get(x, None) for x in cols]
        else:
            points += [x for x in self.data().values()]

        return points

    def timerange_as_utc_string(self):
        """The timerange of this data, in UTC time, as a string.

        :returns: str -- formatted time string.
        """
        return self.timerange().to_utc_string()

    def timerange_as_local_string(self):
        """The timerange of this data, in Local time, as a string.

        :returns: str -- formatted time string.
        """
        return self.timerange().to_local_string()

    def timestamp(self):
        """The timestamp of this data.

        :returns: datetime -- Datetime of the beginning of the range.
        """
        return self.begin()

    def timerange(self):
        """The TimeRange of this data.

        :returns: TimeRange -- the underlying TimeRange object.
        """
        return self._d.get('range')

    def begin(self):
        """The begin time of this Event, which will be just the timestamp.

        :returns: datetime -- Datetime of the beginning of the range.
        """
        return self.timerange().begin()

    def end(self):
        """The end time of this Event, which will be just the timestamp.

        :returns: datetime -- Datetime of the end of the range.
        """
        return self.timerange().end()

    # data setters, returns new object

    def set_data(self, data):
        """Sets the data portion of the event and returns a new TimeRangeEvent.

        :param data: The new data portion for this event object.
        :type data: dict
        :returns: TimeRangeEvent - a new TimeRangeEvent object.
        """
        _dnew = self._d.set('data', self.data_from_arg(data))
        return TimeRangeEvent(_dnew)

    # Humanize

    def humanize_duration(self):
        """Humanize the timerange.

        :returns: str -- humanized string of the timerange()
        """
        return self.timerange().humanize_duration()


class IndexedEvent(EventBase):
    """
    Associates a time range specified as an index.

    The creation of an IndexedEvent is done by combining two parts:
    the Index and the data.

    To construct you specify an Index, along with the data.

    The index may be an Index, or a string.

    To specify the data you can supply either:
        - a python dict containing key values pairs
        - an pyrsistent.pmap, or
        - a simple type such as an integer. In the case of the simple type
          this is a shorthand for supplying {"value": v}.

    :raises: EventException
    """
    def __init__(self, instance_or_begin, data=None, utc=True):
        """
        Create an indexed event.
        """
        # pylint doesn't like self._d but be consistent w/original code.
        # pylint: disable=invalid-name
        if isinstance(instance_or_begin, IndexedEvent):
            super(IndexedEvent, self).__init__(instance_or_begin._d)  # pylint: disable=protected-access
            return
        elif is_pmap(instance_or_begin):
            super(IndexedEvent, self).__init__(instance_or_begin)
            return

        index = self.index_from_args(instance_or_begin, utc)
        data = self.data_from_arg(data)

        super(IndexedEvent, self).__init__(freeze(dict(index=index, data=data)))

    def to_json(self):
        """
        Returns the Event as a JSON object, essentially:
        {time: t, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual vanilla data structure."""
        return dict(
            index=self.index_as_string(),
            data=thaw(self.data())
        )

    def to_point(self, cols=None):
        """
        Returns a flat array starting with the timestamp, followed by the values.
        Doesn't include the groupByKey (key).

        Can be given an optional list of columns so the returned list will
        have the values in order. Primarily for the TimeSeries wire format.

        :param cols: List of data columns.
        :type cols: list/default of None.
        :returns: list -- ms since epoch folowed by data values.
        """
        points = [self.index_as_string()]

        if isinstance(cols, list):
            points += [self.data().get(x, None) for x in cols]
        else:
            points += [x for x in self.data().values()]

        return points

    def index(self):
        """Returns the Index associated with the data in this Event.

        :returns: Index -- Underlying Index object.
        """
        return self._d.get('index')

    def timerange(self):
        """The TimeRange of this data.

        :returns: TimeRange -- Timerange from the underlying Index.
        """
        return self.index().as_timerange()

    def timerange_as_utc_string(self):
        """The timerange of this data, in UTC time, as a string.

        :returns: str -- Underlying TimeRange as UTC string.
        """
        return self.timerange().to_utc_string()

    def timerange_as_local_string(self):
        """The timerange of this data, in Local time, as a string.

        :returns: str -- Underlying TimeRange as localtime string.
        """
        return self.timerange().to_local_string()

    def begin(self):
        """The begin time of this Event, which will be just the timestamp.

        :returns: datetime -- Datetime of the beginning of the range.
        """
        return self.timerange().begin()

    def end(self):
        """The end time of this Event, which will be just the timestamp.

        :returns: datetime -- Datetime of the end of the range.
        """
        return self.timerange().end()

    def timestamp(self):
        """The timestamp of this beginning of the range.

        :returns: datetime -- Datetime of the beginning of the range.
        """
        return self.begin()

    def index_as_string(self):
        """Returns the Index as a string, same as event.index().toString().

        :returns: str -- String version of the underlying Index.
        """
        return self.index().as_string()

    # data setters, returns new object

    def set_data(self, data):
        """Sets the data portion of the event and returns a new IndexedEvent.

        :param data: The new data portion for this event object.
        :type data: dict
        :returns: IndexedEvent - a new IndexedEvent object.
        """
        new_d = self._d.set('data', self.data_from_arg(data))
        return IndexedEvent(new_d)
