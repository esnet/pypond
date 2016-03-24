"""
Implementation of the Pond Event classes.

http://software.es.net/pond/#/events
"""
import copy
import datetime
import json

from pyrsistent import pmap, thaw

from .exceptions import EventException
from .util import dt_from_ms, dt_from_dt, ms_from_dt, is_pmap


class EventBase(object):
    """
    Common code for the event classes.
    """
    @staticmethod
    def timestamp_from_arg(arg):
        """extract timestamp from a constructor arg."""
        if isinstance(arg, int):
            return dt_from_ms(arg)
        elif isinstance(arg, datetime.datetime):
            return dt_from_dt(arg)
        else:
            raise EventException('Unable to get datetime from {a} - should be a datetime object or an integer in epoch ms.'.format(a=arg))  # pylint: disable=line-too-long

    @staticmethod
    def timerange_from_arg(arg):
        """extract timerange from a constructor arg."""
        raise NotImplementedError

    @staticmethod
    def index_from_arg(arg):
        """extract index from a constructor arg."""
        raise NotImplementedError

    @staticmethod
    def data_from_arg(arg):
        """extract data from a constructor arg and make immutable."""
        if isinstance(arg, dict):
            return pmap(arg)
        elif isinstance(arg, pmap):
            return copy.copy(arg)
        elif isinstance(arg, int) or isinstance(arg, float) or isinstance(arg, str):
            return pmap({'value': arg})
        else:
            raise EventException('Could not interpret data from {a}'.format(a=arg))

    @staticmethod
    def key_from_arg(arg):
        """extract key from a constructor arg."""
        if isinstance(arg, str):
            return arg
        elif arg is None:
            return ''
        else:
            raise EventException('Could not get key from {a} - should be a string or None'.format(a=arg))  # pylint: disable=line-too-long


class Event(EventBase):  # pylint: disable=too-many-public-methods
    """
    A generic event

    This represents a data object at a single timestamp, supplied
    at initialization.

    The timestamp may be a python date object, datetime object, or
    ms since UNIX epoch. It is stored internally as a datetime object.

    The data may be any type.

    Asking the Event object for the timestamp returns an integer copy
    of the number of ms since the UNIX epoch. There's no method on
    the Event object to mutate the Event timestamp after it is created.
    """
    def __init__(self, instance_or_time, data=None, key=None):
        """
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
        """
        # pylint doesn't like self._d but be consistent w/original code.
        # pylint: disable=invalid-name

        if isinstance(instance_or_time, Event):
            self._d = instance_or_time._d  # pylint: disable=protected-access
            return

        if is_pmap(instance_or_time) and 'time' in instance_or_time \
                and 'data' in instance_or_time and 'key' in instance_or_time:
            self._d = instance_or_time
            return

        time = self.timestamp_from_arg(instance_or_time)
        data = self.data_from_arg(data)
        key = self.key_from_arg(key)

        self._d = pmap(dict(time=time, data=data, key=key))

    # Query/accessor methods

    def _get_epoch_ms(self):
        return ms_from_dt(self.timestamp())

    def to_json(self):
        """
        Returns the Event as a JSON object, essentially:
        {time: t, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual data structure."""
        return dict(
            time=self._get_epoch_ms(),
            data=thaw(self.data()),
            key=self.key(),
        )

    def to_string(self):
        """
        Retruns the Event as a string, useful for serialization.
        It's a JSON string of the whole object.

        In JS land, this is synonymous with __str__ or __unicode__
        """
        return json.dumps(self.to_json())

    def to_point(self):
        """
        Returns a flat array starting with the timestamp, followed by the values.
        Doesn't include the groupByKey (key).
        """
        return [self._get_epoch_ms()] + [x for x in self.data().values()]

    def timestamp_as_utc_string(self):
        """The timestamp of this data, in UTC time, as a string."""
        raise NotImplementedError

    def timestamp_as_local_string(self):
        """The timestamp of this data, in Local time, as a string."""
        raise NotImplementedError

    def timestamp(self):
        """The timestamp of this data"""
        return self._d.get('time')

    def begin(self):
        """The begin time of this Event, which will be just the timestamp"""
        return self.timestamp()

    def end(self):
        """The end time of this Event, which will be just the timestamp"""
        return self.timestamp()

    def data(self):
        """Direct access to the event data. The result will be a pysistent.PMap."""
        return self._d.get('data')

    def key(self):
        """Access the event groupBy key"""
        return self._d.get('key')

    # data setters, returns new object

    def set_data(self, data):
        """Sets the data portion of the event and returns a new Event."""
        raise NotImplementedError

    def set_key(self, key):
        """
        Sets the groupBy Key and returns a new Event
        """
        raise NotImplementedError

    def get(self, field_spec='value'):
        """
        Get specific data out of the Event. The data will be converted
        to a js object. You can use a fieldSpec to address deep data.
        A fieldSpec could be "a.b"

        The field spec can have an arbitrary number of "parts."

        Peter orginally did this:
        const value = fieldSpec.split(".").reduce((o, i) => o[i], eventData);
        """
        raise NotImplementedError

    def value(self, field_spec):
        """
        Alias for get()
        """
        raise NotImplementedError

    def stringify(self):
        """Produce a json string of the internal data."""
        return json.dumps(thaw(self.data()))

    def __str__(self):
        """call to_string()"""
        return self.to_string()

    # Static class methods

    @staticmethod
    def same(event1, event2):
        """
        Different name for is() which is an invalid method name.
        Different that __eq__ - see Object.is() JS documentation.
        """
        raise NotImplementedError

    @staticmethod
    def is_valid_value(event, field_spec='value'):
        """
        The same as Event.value() only it will return false if the
        value is either undefined, NaN or Null.
        """
        raise NotImplementedError

    @staticmethod
    def selector(event, field_spec):
        """
        Function to select specific fields of an event using
        a fieldSpec and return a new event with just those fields.

        The fieldSpec currently can be:
            * A single field name
            * An array of field names

        The function returns a new event.
        """
        raise NotImplementedError

    # merge methods (deal in lists of events)

    @staticmethod
    def merge_events(events):
        """
        Merge a list of regular Event objects.
        """
        raise NotImplementedError

    @staticmethod
    def merge_timerange_events(events):
        """
        Merge a list of TimeRangeEvent objects.
        """
        raise NotImplementedError

    @staticmethod
    def merge_indexed_events(events):
        """
        Merge a list of IndexedEvent objects.
        """
        raise NotImplementedError

    @staticmethod
    def merge(events):
        """
        This is an entry point that will grok the what kind of events
        are in the list and call one of the previous three methods.
        """
        raise NotImplementedError

    @staticmethod
    def combine(events, field_spec, reducer):
        """
        Combines multiple events with the same time together
        to form a new event. Doesn't currently work on IndexedEvents
        or TimeRangeEvents.
        """
        raise NotImplementedError

    # these call combine with appropriate reducer

    @staticmethod
    def sum(events, field_spec):
        """Reducer with sum."""
        raise NotImplementedError

    @staticmethod
    def avg(events, field_spec):
        """Reducer with avg."""
        raise NotImplementedError

    # map, reduce, etc

    @staticmethod
    def map(events, field_spec):
        """
        Maps a list of events according to the selection
        specification raise NotImplementedErrored in. The spec maybe a single
        field name, a list of field names, or a function
        that takes an event and returns a key/value pair.

        Example 1:
                in   out
         3am    1    2
         4am    3    4

        Mapper result:  { in: [1, 3], out: [2, 4]}
        """
        raise NotImplementedError

    @staticmethod
    def reduce(mapped, reducer):
        """
        Takes a list of events and a reducer function and returns
        a new Event with the result, for each column. The reducer is
        of the form:
            function sum(valueList) {
                return calcValue;
            }
        """
        raise NotImplementedError

    @staticmethod
    def map_reduce(events, field_spec, reducer):
        """map and reduce"""
        raise NotImplementedError


class TimeRangeEvent(EventBase):
    """
    Associates a TimeRange with some data.

        constructor(arg1, arg2, arg3) {
        if (arg1 instanceof TimeRangeEvent) {
            const other = arg1;
            this._d = other._d;
            return;
        }
        const range = timeRangeFromArg(arg1);
        const data = dataFromArg(arg2);
        const key = keyFromArg(arg3);
        this._d = new Immutable.Map({range, data, key});
    }
    """
    def __init__(self):
        """
        Create a time range event.
        """
        raise NotImplementedError

        # Query/accessor methods

    def to_json(self):
        """
        Returns the Event as a JSON object, essentially:
        {time: t, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual vanilla data structure."""
        raise NotImplementedError

    def to_string(self):
        """
        Retruns the Event as a string, useful for serialization.
        It's a JSON string of the whole object.

        In JS land, this is synonymous with __str__ or __unicode__
        """
        raise NotImplementedError

    def to_point(self):
        """
        Returns a flat array starting with the timestamp, followed by the values.
        Doesn't include the groupByKey (key).
        """
        raise NotImplementedError

    def timerange_as_utc_string(self):
        """The timerange of this data, in UTC time, as a string."""
        raise NotImplementedError

    def timerange_as_local_string(self):
        """The timerange of this data, in Local time, as a string."""
        raise NotImplementedError

    def timestamp(self):
        """The timestamp of this data"""
        raise NotImplementedError

    def timerange(self):
        """The TimeRange of this data."""
        raise NotImplementedError

    def begin(self):
        """The begin time of this Event, which will be just the timestamp"""
        raise NotImplementedError

    def end(self):
        """The end time of this Event, which will be just the timestamp"""
        raise NotImplementedError

    def data(self):
        """Direct access to the event data. The result will be an Immutable.Map."""
        raise NotImplementedError

    def key(self):
        """Access the event groupBy key"""
        raise NotImplementedError

    # data setters, returns new object

    def set_data(self, data):
        """Sets the data portion of the event and returns a new Event."""
        raise NotImplementedError

    def set_key(self, key):
        """
        Sets the groupBy Key and returns a new Event
        """
        raise NotImplementedError

    def get(self, field_spec='value'):
        """
        Get specific data out of the Event. The data will be converted
        to a js object. You can use a fieldSpec to address deep data.
        A fieldSpec could be "a.b"

        The field spec can have an arbitrary number of "parts."

        Peter orginally did this:
        const value = fieldSpec.split(".").reduce((o, i) => o[i], eventData);
        """
        raise NotImplementedError

    def value(self, field_spec):
        """
        Alias for get()
        """
        raise NotImplementedError

    # Humanize

    def humanize_duration(self):
        """Humanize the timerange."""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        raise NotImplementedError


class IndexedEvent(EventBase):
    """
    Associates a time range specified as an index.

        constructor(arg1, arg2, arg3, arg4) {
        if (arg1 instanceof IndexedEvent) {
            const other = arg1;
            this._d = other._d;
            return;
        }
        const index = indexFromArgs(arg1, arg3);
        const data = dataFromArg(arg2);
        const key = keyFromArg(arg4);
        this._d = new Immutable.Map({index, data, key});
    }
    """
    def __init__(self):
        """
        Create an indexed event.
        """
        raise NotImplementedError

    def to_json(self):
        """
        Returns the Event as a JSON object, essentially:
        {time: t, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual vanilla data structure."""
        raise NotImplementedError

    def to_string(self):
        """
        Retruns the Event as a string, useful for serialization.
        It's a JSON string of the whole object.

        In JS land, this is synonymous with __str__ or __unicode__
        """
        raise NotImplementedError

    def to_point(self):
        """
        Returns a flat array starting with the timestamp, followed by the values.
        Doesn't include the groupByKey (key).
        """
        raise NotImplementedError

    def timerange_as_utc_string(self):
        """The timerange of this data, in UTC time, as a string."""
        raise NotImplementedError

    def timerange_as_local_string(self):
        """The timerange of this data, in Local time, as a string."""
        raise NotImplementedError

    def timestamp(self):
        """The timestamp of this data"""
        raise NotImplementedError

    def timerange(self):
        """The TimeRange of this data."""
        raise NotImplementedError

    def begin(self):
        """The begin time of this Event, which will be just the timestamp"""
        raise NotImplementedError

    def end(self):
        """The end time of this Event, which will be just the timestamp"""
        raise NotImplementedError

    def data(self):
        """Direct access to the event data. The result will be an Immutable.Map."""
        raise NotImplementedError

    def key(self):
        """Access the event groupBy key"""
        raise NotImplementedError

    # data setters, returns new object

    def set_data(self, data):
        """Sets the data portion of the event and returns a new Event."""
        raise NotImplementedError

    def set_key(self, key):
        """
        Sets the groupBy Key and returns a new Event
        """
        raise NotImplementedError

    def get(self, field_spec='value'):
        """
        Get specific data out of the Event. The data will be converted
        to a js object. You can use a fieldSpec to address deep data.
        A fieldSpec could be "a.b"

        The field spec can have an arbitrary number of "parts."

        Peter orginally did this:
        const value = fieldSpec.split(".").reduce((o, i) => o[i], eventData);
        """
        raise NotImplementedError

    def value(self, field_spec):
        """
        Alias for get()
        """
        raise NotImplementedError

    def index(self):
        """Returns the Index associated with the data in this Event."""
        raise NotImplementedError

    def index_as_string(self):
        """Returns the Index as a string, same as event.index().toString()"""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        raise NotImplementedError
