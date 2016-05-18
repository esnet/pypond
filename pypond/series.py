#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Implements the Pond TimeSeries class.

http://software.es.net/pond/#/timeseries
"""

import collections
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

        if isinstance(instance_or_wire, TimeSeries):
            # copy ctor
            # pylint: disable=protected-access
            self._collection = instance_or_wire._collection
            self._data = instance_or_wire._data

        elif isinstance(instance_or_wire, dict):
            if 'events' in instance_or_wire:
                # list of events dict(name='events', events=[list, of, events])

                self._collection = Collection(instance_or_wire.get('events', []))

                meta = copy.copy(instance_or_wire)
                meta.pop('events')

                self._data = self.build_metadata(meta)

            elif 'collection' in instance_or_wire:
                # collection dict(name='collection', collection=collection_obj)

                self._collection = instance_or_wire.get('collection', None)

                meta = copy.copy(instance_or_wire)
                meta.pop('collection')

                self._data = self.build_metadata(meta)

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

                meta = copy.copy(instance_or_wire)
                meta.pop('columns')
                meta.pop('points')

                self._data = self.build_metadata(meta)

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
            points.append(i.to_point(columns[1:]))

        cols_and_points = dict(
            columns=columns,
            points=points,
        )

        # fold in the rest of the payload
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
        return self._collection.range()

    def range(self):
        """Alias for timerange()"""
        return self.timerange()

    def begin(self):
        """Gets the earliest time represented in the TimeSeries."""
        return self.range().begin()

    def end(self):
        """Gets the latest time represented in the TimeSeries."""
        return self.range().end()

    def at(self, i):  # pylint: disable=invalid-name
        """Access the series events via index"""
        return self._collection.at(i)

    def set_collection(self, coll):
        """Sets a new underlying collection for this TimeSeries."""
        res = TimeSeries(self)
        res._collection = coll  # pylint: disable=protected-access
        return res

    def bisect(self, dtime, b=0):  # pylint: disable=invalid-name
        """
        Finds the index that is just less than the time t supplied.
        In other words every event at the returned index or less
        has a time before the supplied t, and every sample after the
        index has a time later than the supplied t.

        Optionally supply a begin index to start searching from.
        """
        return self._collection.bisect(dtime, b)

    def slice(self, begin, end):
        """
        Perform a slice of events within the TimeSeries, returns a new
        TimeSeries representing a portion of this TimeSeries from begin up to
        but not including end.
        """
        sliced = self._collection.slice(begin, end)
        return self.set_collection(sliced)

    def clean(self, field_spec):
        """
        Generates new collection using a fieldspec
        """
        cleaned = self._collection.clean(field_spec)
        return self.set_collection(cleaned)

    def collapse(self, field_spec_list, name, reducer, append=True):
        """
        Takes a fieldSpecList (list of column names) and collapses
        them to a new column which is the reduction of the matched columns
        in the fieldSpecList.
        """
        collapsed = self._collection.collapse(field_spec_list, name, reducer, append)
        return self.set_collection(collapsed)

    def events(self):
        """
        Generator to allow for..of loops over series.events()
        """
        return iter(self._collection.event_list())

    # Access metadata about the series

    def name(self):
        """Get data name."""
        return self._data.get('name')

    def index(self):
        """Get the index."""
        return self._data.get('index')

    def index_as_string(self):
        """Index represented as a string."""
        return self.index().to_string() if self.index() else None

    def index_as_range(self):
        """Index returnd as time range."""
        return self.index().as_timerange() if self.index() else None

    def is_utc(self):
        """Get data utc."""
        return self._data.get('utc')

    def columns(self):
        """
        create a list of the underlying columns.

        Due to the nature of the event data and using dicts, the order
        of the column list might be somewhat unpredictable. When generating
        points, this is solved by passing the column list to .to_point()
        as an optional argument to ensure that the points and the columns
        are properly aligned.
        """
        cret = dict()

        for i in self._collection.events():
            for v in i.to_json().values():
                if isinstance(v, dict):
                    for key in v.keys():
                        cret[key] = True

        return cret.keys()

    def collection(self):
        """Returns the internal collection of events for this TimeSeries"""
        return self._collection

    def meta(self, key=None):
        """Returns the meta data about this TimeSeries as a JSON object"""
        if key is None:
            return thaw(self._data)
        else:
            return self._data.get(key)

    # Access the series itself

    def size(self):
        """Number of rows in series."""
        return self._collection.size()

    def size_valid(self, field_spec):
        """Returns the number of rows in the series."""
        return self._collection.size_valid(field_spec)

    def count(self):
        """alias for size."""
        return self.size()

    # sum/min/max etc

    # pylint: disable=dangerous-default-value

    def sum(self, field_spec=['value']):
        """Get sum"""
        return self._collection.sum(field_spec)

    def max(self, field_spec=['value']):
        """Get max"""
        return self._collection.max(field_spec)

    def min(self, field_spec=['value']):
        """Get min"""
        return self._collection.min(field_spec)

    def avg(self, field_spec=['value']):
        """Get avg"""
        return self._collection.avg(field_spec)

    def mean(self, field_spec=['value']):
        """Get mean"""
        return self._collection.mean(field_spec)

    def median(self, field_spec=['value']):
        """Get median"""
        return self._collection.median(field_spec)

    def stdev(self, field_spec=['value']):
        """Get std dev"""
        return self._collection.stdev(field_spec)

    def aggregate(self, func, field_spec=['value']):
        """Get std dev"""
        return self._collection.aggregate(func, field_spec)

    def pipeline(self):
        """get a pipeline from the collection."""
        raise NotImplementedError

    def select(self, field_spec, cb):  # pylint: disable=invalid-name
        """call select on the pipeline."""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        return self.to_string()

    # Static methods

    @staticmethod
    def equal(series1, series2):
        """Check equality - same instance."""
        # pylint: disable=protected-access
        return bool(
            series1._data is series2._data and
            series1._collection is series2._collection
        )

    @staticmethod
    def same(series1, series2):
        """Implements JS Object.is() - same values"""
        # pylint: disable=protected-access
        return bool(
            series1._data == series2._data and
            Collection.same(series1._collection, series2._collection)
        )

    @staticmethod
    def map(data, series_list, mapper, field_spec=None):
        """for each series, map events to the same timestamp/index"""

        event_map = collections.OrderedDict()

        for i in series_list:
            for evn in i.events():
                key = None
                if isinstance(evn, Event):
                    key = evn.timestamp()
                elif isinstance(evn, IndexedEvent):
                    key = evn.index()
                elif isinstance(evn, TimeRangeEvent):
                    key = evn.timerange().to_utc_string()

                if key not in event_map:
                    # keep keys in insert order
                    event_map.update({key: list()})

                event_map[key].append(evn)

        events = list()

        for v in event_map.values():
            if field_spec is None:
                event = mapper(v)
            else:
                event = mapper(v, field_spec)

            events.append(event)

        return TimeSeries(dict(events=events, **data))

    @staticmethod
    def merge(data, series_list):
        """Merge."""
        return TimeSeries.map(data, series_list, Event.merge)

    @staticmethod
    def sum_list(data, series_list, field_spec):
        """
        Takes a list of TimeSeries and sums them together to form a new
        Timeseries.

        const ts1 = new TimeSeries(weather1);
        const ts2 = new TimeSeries(weather2);
        const sum = TimeSeries.sum({name: "sum"}, [ts1, ts2], ["temp"]);
        """
        return TimeSeries.map(data, series_list, Event.sum, field_spec)
