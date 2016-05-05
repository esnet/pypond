"""
Implements the Pond TimeSeries class.

http://software.es.net/pond/#/timeseries
"""

import copy
import json

from pyrsistent import freeze, thaw

from .bases import PypondBase
from .collection import Collection
from .event import Event, TimeRangeEvent, IndexedEvent
from .exceptions import TimeSeriesException
from .index import Index
from .util import ObjectEncoder


class TimeSeries(PypondBase):  # pylint: disable=too-many-public-methods
    """
    A TimeSeries is a a Series where each event is an association of a timestamp
    and some associated data.

    Data passed into it may have the following format, which is our wire format:

      {
        "name": "traffic",
        "columns": ["time", "value", ...],
        "points": [
           [1400425947000, 52, ...],
           [1400425948000, 18, ...],
           [1400425949000, 26, ...],
           [1400425950000, 93, ...],
           ...
         ]
      }

    Alternatively, the TimeSeries may be constructed from a list of Event objects.

    Internaly the above series is represented as two parts:
     * Collection - an Immutable.List of Events and associated methods
                      to query and manipulate that list
     * Meta data  - an Immutable.Map of extra data associated with the
                      TimeSeries

    The events stored in the collection may be Events (timestamp based),
    TimeRangeEvents (time range based) or IndexedEvents (an alternative form
    of a time range, such as "2014-08" or "1d-1234")

    The timerange associated with a TimeSeries is simply the bounds of the
    events within it (i.e. the min and max times).
    """

    event_type_map = dict(
        time=Event,
        timerange=TimeRangeEvent,
        index=IndexedEvent,
    )

    def __init__(self, instance_or_wire):
        """
        initialize a TimeSeries object from
            * Another TimeSeries/copy ctor
            * An event list
            * From the wire format
        """
        super(TimeSeries, self).__init__()

        self._collection = None
        self._data = None

        # used for generating the json in the case of the wire format.
        self._columns = list()

        if isinstance(instance_or_wire, TimeSeries):
            # copy ctor
            pass
        elif isinstance(instance_or_wire, dict):
            # got wire format/etc
            if 'events' in instance_or_wire:
                pass
            elif 'collection' in instance_or_wire:
                pass
            elif 'columns' in instance_or_wire and 'points' in instance_or_wire:
                # coming from the wire format

                event_type = instance_or_wire.get('columns')[0]
                event_fields = instance_or_wire.get('columns')[1:]

                events = list()

                for i in instance_or_wire.get('points'):
                    time = i[0]
                    event_values = i[1:]
                    data = dict(zip(event_fields, event_values))
                    try:
                        events.append(self.event_type_map[event_type](time, data))
                    except KeyError:
                        msg = 'invalid event type {et}'.format(et=event_type)
                        raise TimeSeriesException(msg)

                self._collection = Collection(events)

                meta2 = copy.copy(instance_or_wire)
                meta2.pop('columns')
                meta2.pop('points')

                self._data = self.build_metadata(meta2)

            else:
                msg = 'unable to determine dict format'
                raise TimeSeriesException(msg)
        else:
            # unable to determine
            msg = 'arg must be a TimeSeries instance or dict'
            raise TimeSeriesException(msg)

    @staticmethod
    def build_metadata(meta):
        """
        Build the metadata out of the incoming wire format
        """

        ret = copy.copy(meta) if meta else dict()

        ret['name'] = meta.get('name', '')

        if 'index' in meta:
            if isinstance(meta.get('index'), str):
                ret['index'] = Index(meta.get('index'))
            elif isinstance(meta.get('index'), Index):
                ret['index'] = meta.get('index')

        ret['utc'] = True
        if 'utc' in meta and isinstance(meta.get('utc'), bool):
            ret['utc'] = meta.get('utc')

        return freeze(ret)

    def to_json(self):
        """
        Returns the TimeSeries as a JSON object, essentially:
        {time: t, data: {key: value, ...}}

        This is actually like json.loads(s) - produces the
        actual vanilla data structure."""

        columns = list()
        points = list()

        if self._collection.type() == Event:
            columns += ['time']
        elif self._collection.type() == TimeRangeEvent:
            columns += ['timerange']
        elif self._collection.type() == IndexedEvent:
            columns += ['index']

        columns += self.columns()

        for i in self._collection.events():
            points.append(i.to_point())

        cols_and_points = dict(
            columns=columns,
            points=points,
        )

        # XXX: make sure this does the right thing with the index.

        cols_and_points.update(thaw(self._data))

        return cols_and_points

    def to_string(self):
        """
        Retruns the TimeSeries as a string, useful for serialization.

        In JS land, this is synonymous with __str__ or __unicode__
        """
        return json.dumps(self.to_json(), cls=ObjectEncoder)

    def timerange(self):
        """Returns the extents of the TimeSeries as a TimeRange.."""
        raise NotImplementedError

    def range(self):
        """Alias for timerange()"""
        raise NotImplementedError

    def begin(self):
        """Gets the earliest time represented in the TimeSeries."""
        raise NotImplementedError

    def end(self):
        """Gets the latest time represented in the TimeSeries."""
        raise NotImplementedError

    def at(self, i):  # pylint: disable=invalid-name
        """Access the series events via index"""
        raise NotImplementedError

    def bisect(self, t, b):  # pylint: disable=invalid-name
        """
        Finds the index that is just less than the time t supplied.
        In other words every event at the returned index or less
        has a time before the supplied t, and every sample after the
        index has a time later than the supplied t.

        Optionally supply a begin index to start searching from.
        """
        raise NotImplementedError

    def slice(self, begin, end):
        """
        Perform a slice of events within the TimeSeries, returns a new
        TimeSeries representing a portion of this TimeSeries from begin up to
        but not including end.
        """
        raise NotImplementedError

    def events(self):
        """
        Generator to allow for..of loops over series.events()
        """
        raise NotImplementedError

    # Access metadata about the series

    def name(self):
        """Get data name."""
        raise NotImplementedError

    def index(self):
        """Get the index."""
        raise NotImplementedError

    def index_as_string(self):
        """Index represented as a string."""
        raise NotImplementedError

    def index_as_range(self):
        """Index returnd as time range."""
        raise NotImplementedError

    def is_utc(self):
        """Get data utc."""
        raise NotImplementedError

    def columns(self):
        """access the underlying columns."""
        cret = dict()

        for i in self._collection.events():
            for v in i.to_json().values():
                if isinstance(v, dict):
                    for key in v.keys():
                        cret[key] = True

        return cret.keys()

    def collection(self):
        """Returns the internal collection of events for this TimeSeries"""
        raise NotImplementedError

    def meta(self):
        """Returns the meta data about this TimeSeries as a JSON object"""
        raise NotImplementedError

    # Access the series itself

    def size(self):
        """Number of rows in series."""
        raise NotImplementedError

    def size_valid(self):
        """Returns the number of rows in the series."""
        raise NotImplementedError

    # sum/min/max etc

    def sum(self, field_spec):
        """Get sum"""
        raise NotImplementedError

    def max(self, field_spec):
        """Get max"""
        raise NotImplementedError

    def min(self, field_spec):
        """Get min"""
        raise NotImplementedError

    def avg(self, field_spec):
        """Get avg"""
        raise NotImplementedError

    def mean(self, field_spec):
        """Get mean"""
        raise NotImplementedError

    def median(self, field_spec):
        """Get median"""
        raise NotImplementedError

    def stdev(self, field_spec):
        """Get std dev"""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        return self.to_string()

    # Static methods

    @staticmethod
    def equal(series1, series2):
        """Check equality."""
        raise NotImplementedError

    @staticmethod
    def same(series1, series2):
        """Implements JS Object.is()"""
        raise NotImplementedError

    @staticmethod
    def map(data, series_list, mapper):
        """for each series, map events to the same timestamp/index"""
        raise NotImplementedError

    @staticmethod
    def merge(data, series_list):
        """Merge."""
