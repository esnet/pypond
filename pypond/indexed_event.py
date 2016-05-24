"""
Event with a time range specified as an index.
"""

from pyrsistent import freeze, thaw

# from .event import EventBase
from .event import EventBase
from .util import is_pmap


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
