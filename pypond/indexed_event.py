#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Event with a time range specified as an index.
"""

from pyrsistent import pmap, thaw

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

    Parameters
    ----------
    instance_or_begin : Index, pyrsistent.pmap, or str.
        Index for copy constructor, pmap as the fully
        formed internals or a string arg to the Index class.
    data : dict or pyrsistent.pmap, optional
        Data payload.
    utc : bool, optional
        UTC or localtime to create index in. Please don't not use UTC.
        Yes, that's a double negative.
    """
    __slots__ = ()  # inheriting relevant slots, stil need this

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

        super(IndexedEvent, self).__init__(pmap(dict(index=index, data=data)))

    def to_json(self):
        """
        Returns the Event as a JSON object, essentially:
        {time: t, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual vanilla data structure.

        Returns
        -------
        dict
            Dictionary representation of object internals.
        """
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

        Parameters
        ----------
        cols : list, optional
            List of columns to order the points in so the TimeSeries
            wire format is rendered corectly.

        Returns
        -------
        list
            Epoch ms followed by points.
        """
        points = [self.index_as_string()]

        data = thaw(self.data())

        if isinstance(cols, list):
            points += [data.get(x, None) for x in cols]
        else:
            points += [x for x in list(data.values())]

        return points

    def index(self):
        """Returns the Index associated with the data in this Event.

        Returns
        -------
        Index
            The underlying index object
        """
        return self._d.get('index')

    def timerange(self):
        """The TimeRange of this data.

        Returns
        -------
        TimeRange
            Time range from the underlying index.
        """
        return self.index().as_timerange()

    def timerange_as_utc_string(self):
        """The timerange of this data, in UTC time, as a string.

        Returns
        -------
        str
            Underlying TimeRange as UTC string.
        """
        return self.timerange().to_utc_string()

    def timerange_as_local_string(self):
        """The timerange of this data, in Local time, as a string..

        Returns
        -------
        str
            Underlying TimeRange as localtime string.
        """
        return self.timerange().to_local_string()

    def begin(self):
        """The begin time of this Event, which will be just the timestamp.

        Returns
        -------
        datetime.datetime
            Datetime of the beginning of the range.
        """
        return self.timerange().begin()

    def end(self):
        """The end time of this Event, which will be just the timestamp.

        Returns
        -------
        datetime.datetime
            Datetime of the end of the range.
        """
        return self.timerange().end()

    def timestamp(self):
        """The timestamp of this beginning of the range.

        Returns
        -------
        datetime.datetime
            Datetime of the beginning of the range.
        """
        return self.begin()

    def index_as_string(self):
        """Returns the Index as a string, same as event.index().toString().

        :returns: str -- String version of the underlying Index.

        Returns
        -------
        str
            String version of the underlying index.
        """
        return self.index().as_string()

    # data setters, returns new object

    def set_data(self, data):
        """Sets the data portion of the event and returns a new IndexedEvent.

        :param data: The new data portion for this event object.
        :type data: dict
        :returns: IndexedEvent - a new IndexedEvent object.

        Parameters
        ----------
        data : dict
            The new data payload for this event object.

        Returns
        -------
        IndexedEvent
            A new indexed event with the provided payload.
        """
        new_d = self._d.set('data', self.data_from_arg(data))
        return IndexedEvent(new_d)
