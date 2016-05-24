"""
TimeRangeEvent associates data with a specific time range rather than
at a discret time like Event does.
"""

from pyrsistent import freeze, thaw

from .event import EventBase
from .util import is_pmap


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
