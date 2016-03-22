"""
Implementation of the Pond Event classes.

http://software.es.net/pond/#/events
"""


class EventBase(object):
    """
    Common code for the event classes.
    """
    @staticmethod
    def timestamp_from_arg(arg):
        """extract timestamp from a constructor arg."""
        raise NotImplementedError

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
        raise NotImplementedError

    @staticmethod
    def key_from_arg(arg):
        """extract key from a constructor arg."""
        raise NotImplementedError


class Event(EventBase):  # pylint: disable=too-many-public-methods
    """
    A 'regular' event. Associates a timestamp with some data.

        constructor(arg1, arg2, arg3) {
        if (arg1 instanceof Event) {
            const other = arg1;
            this._d = other._d;
            return;
        }
        if (arg1 instanceof Immutable.Map &&
            arg1.has("time") && arg1.has("data") && arg1.has("key")) {
            this._d = arg1;
            return;
        }
        const time = timestampFromArg(arg1);
        const data = dataFromArg(arg2);
        const key = keyFromArg(arg3);
        this._d = new Immutable.Map({time, data, key});
    """
    def __init__(self):
        """
        Create a regular event.
        """
        raise NotImplementedError

    # Query/accessor methods

    def to_json(self):
        """
        Returns the Event as a JSON object, essentially:
        {time: t, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual data structure."""
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

    def timestamp_as_utc_string(self):
        """The timestamp of this data, in UTC time, as a string."""
        raise NotImplementedError

    def timestamp_as_local_string(self):
        """The timestamp of this data, in Local time, as a string."""
        raise NotImplementedError

    def timestamp(self):
        """The timestamp of this data"""
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

    def stringify(self):
        """Produce a json string of the internal data."""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        raise NotImplementedError

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
