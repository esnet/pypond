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

from pyrsistent import pmap, thaw

from .bases import PypondBase
from .collection import Collection
from .event import Event
from .exceptions import TimeSeriesException
from .index import Index
from .indexed_event import IndexedEvent
from .timerange_event import TimeRangeEvent
from .util import ObjectEncoder, ms_from_dt


class TimeSeries(PypondBase):  # pylint: disable=too-many-public-methods
    """
    A TimeSeries is a a Series where each event is an association of a timestamp
    and some associated data.

    Data passed into it may have the following format, which is our wire format

    ::

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

    Initialize a TimeSeries object from:

    * Another TimeSeries/copy ctor
    * An event list
    * From the wire format

    Parameters
    ----------
    instance_or_wire : TimeSeries, list of events, wire format
        See above

    Raises
    ------
    TimeSeriesException
        Raised when args can not be properly handled.

    Attributes
    ----------
    event_type_map : dict
        Map text keys from wire format to the appropriate Event class.
    """

    event_type_map = dict(
        time=Event,
        timerange=TimeRangeEvent,
        index=IndexedEvent,
    )

    def __init__(self, instance_or_wire):
        """
        create a time series object
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
                    data = dict(list(zip(event_fields, event_values)))
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

        if self._collection.is_chronological() is not True:
            msg = 'Events supplied to TimeSeries constructor must be chronological'
            raise TimeSeriesException(msg)

    @staticmethod
    def build_metadata(meta):
        """
        Build the metadata out of the incoming wire format

        Parameters
        ----------
        meta : dict
            Incoming wire format.

        Returns
        -------
        pyrsistent.pmap
            Immutable dict of metadata
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

        return pmap(ret)

    def to_json(self):
        """
        Returns the TimeSeries as a python dict.

        This is actually like json.loads(s) - produces the
        actual vanilla data structure.

        Returns
        -------
        dict
            Dictionary of columns and points
        """

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
        cols_and_points.update(self._data)

        # Turn the index back into a string for the json representation.
        # The Index object can still be accessed via TimeSeries.index()
        if 'index' in cols_and_points and \
                isinstance(cols_and_points.get('index'), Index):
            cols_and_points['index'] = cols_and_points.get('index').to_string()

        return cols_and_points

    def to_string(self):
        """
        Retruns the TimeSeries as a string, useful for serialization.

        In JS land, this is synonymous with __str__ or __unicode__

        Returns
        -------
        str
            String version of to_json() for transmission/etc.
        """
        return json.dumps(self.to_json(), cls=ObjectEncoder)

    def timerange(self):
        """Returns the extents of the TimeSeries as a TimeRange..

        Returns
        -------
        TimeRange
            TimeRange internal of the underly collection.
        """
        return self._collection.range()

    def range(self):
        """Alias for timerange()

        Returns
        -------
        TimeRange
            TimeRange internal of the underly collection.
        """
        return self.timerange()

    def begin(self):
        """Gets the earliest time represented in the TimeSeries.

        Returns
        -------
        datetime.datetime
            The begin time of the underlying time range.
        """
        return self.range().begin()

    def end(self):
        """Gets the latest time represented in the TimeSeries.

        Returns
        -------
        datetime.datetime
            The end time of the underlying time range.
        """
        return self.range().end()

    def begin_timestamp(self):
        """Gets the earliest time represented in the TimeSeries
        in epoch ms.

        Returns
        -------
        int
            The begin time of the underlying time range in epoch ms.
        """
        return ms_from_dt(self.range().begin())

    def end_timestamp(self):
        """Gets the latest time represented in the TimeSeries
        in epoch ms.

        Returns
        -------
        int
            The end time of the underlying time range in epoch ms.
        """
        return ms_from_dt(self.range().end())

    def at(self, i):  # pylint: disable=invalid-name
        """Access the series events via numeric index

        Parameters
        ----------
        i : int
            An array index

        Returns
        -------
        Event
            The Event object found at index i
        """
        return self._collection.at(i)

    def at_time(self, time):
        """Return an event in the series by its time. This is the same
        as calling `bisect` first and then using `at` with the index.

        Parameters
        ----------
        time : datetime.datetime
            A datetime object

        Returns
        -------
        Event
            The event at the designated time.
        """
        return self._collection.at_time(time)

    def at_first(self):
        """Return first event in the series

        Returns
        -------
        Event
            The first event in the series.
        """
        return self._collection.at_first()

    def at_last(self):
        """Return last event in the series

        Returns
        -------
        Event
            The last event in the series.
        """
        return self._collection.at_last()

    def set_collection(self, coll):
        """Sets a new underlying collection for this TimeSeries.

        Parameters
        ----------
        coll : Collection
            New collection to assign to this TimeSeries

        Returns
        -------
        TimeSeries
            New TimeSeries with Collection coll
        """
        res = TimeSeries(self)

        # pylint: disable=protected-access

        if coll is not None:
            res._collection = coll
        else:
            res._collection = Collection()

        return res

    def bisect(self, dtime, b=0):  # pylint: disable=invalid-name
        """
        Finds the index that is just less than the time t supplied.
        In other words every event at the returned index or less
        has a time before the supplied t, and every sample after the
        index has a time later than the supplied t.

        Optionally supply a begin index to start searching from. Returns
        index that is the greatest but still below t.

        Parameters
        ----------
        dtime : datetime.datetime
            Date time object to search with
        b : int, optional
            An index position to start searching from.

        Returns
        -------
        int
            The index of the Event searched for by dtime.
        """
        return self._collection.bisect(dtime, b)

    def slice(self, begin, end):
        """
        Perform a slice of events within the TimeSeries, returns a new
        TimeSeries representing a portion of this TimeSeries from begin up to
        but not including end. Uses typical python [slice:syntax].

        Parameters
        ----------
        begin : int
            Slice begin index
        end : int
            Slice end index

        Returns
        -------
        TimeSeries
            New instance with sliced collection.
        """
        sliced = self._collection.slice(begin, end)
        return self.set_collection(sliced)

    def crop(self, timerange):
        """Crop the TimeSeries to the specified TimeRange and return
        a new TimeSeries

        Parameters
        ----------
        timerange : TimeRange
            Bounds of the new TimeSeries

        Returns
        -------
        TimeSeries
            The new cropped TimeSeries instance.
        """

        begin = self.bisect(timerange.begin())
        end = self.bisect(timerange.end(), begin)
        return self.slice(begin, end)

    def clean(self, field_path=None):
        """
        Returns a new TimeSeries by testing the field_path
        values for being valid (not NaN, null or undefined).
        The resulting TimeSeries will be clean for that fieldSpec.

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        TimeSeries
            New time series from clean values from the field spec.
        """
        cleaned = self._collection.clean(field_path)
        return self.set_collection(cleaned)

    def events(self):
        """
        Generator to allow for..of loops over series.events()

        Returns
        -------
        iterator
            Generator for loops.
        """
        return iter(self._collection.event_list())

    # Access metadata about the series

    def name(self):
        """Get data name.

        Returns
        -------
        str
            Data name.
        """
        return self._data.get('name')

    def set_name(self, name):
        """Set name and generate a new TimeSeries

        Parameters
        ----------
        name : str
            New name

        Returns
        -------
        TimeSeries
            Return a TimeSeries with a new name.
        """
        return self.set_meta('name', name)

    def index(self):
        """Get the index.

        Returns
        -------
        Index
            Get the index.
        """
        return self._data.get('index')

    def index_as_string(self):
        """Index represented as a string.

        Returns
        -------
        str
            String format of Index or None.
        """
        return self.index().to_string() if self.index() else None

    def index_as_range(self):
        """Index returned as time range.

        Returns
        -------
        TimeRange
            Index as a TimeRange or None
        """
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

        Returns
        -------
        list
            List of column names.
        """
        cret = dict()

        for i in self._collection.events():
            for v in list(i.to_json().values()):
                if isinstance(v, dict):
                    for key in list(v.keys()):
                        cret[key] = True

        return list(cret.keys())

    def collection(self):
        """Returns the internal collection of events for this TimeSeries

        Returns
        -------
        Collection
            Internal collection.
        """
        return self._collection

    def meta(self, key=None):
        """Returns the meta data about this TimeSeries as a JSON object

        Parameters
        ----------
        key : str, optional
            Optional metadata key to fetch value for

        Returns
        -------
        dict or key/value
            Return a thawed metadata dict or the value specified by *key*.
        """
        if key is None:
            return thaw(self._data)
        else:
            return self._data.get(key)

    def set_meta(self, key, value):
        """Change the metadata of the TimeSeries

        Parameters
        ----------
        key : str
            The metadata key
        value : obj
            The value

        Returns
        -------
        TimeSeries
            A new TimeSeries with new metadata.
        """
        new_ts = TimeSeries(self)
        new_ts._data = new_ts._data.set(key, value)  # pylint: disable=protected-access

        return new_ts

    # Access the series itself

    def size(self):
        """Number of rows in series.

        Returns
        -------
        int
            Number in the series.
        """
        return self._collection.size()

    def size_valid(self, field_path):
        """
        Returns the number of valid items in this collection.

        Uses the fieldSpec to look up values in all events.
        It then counts the number that are considered valid,
        i.e. are not NaN, undefined or null.

        Parameters
        ----------
        field_path : str, list, tuple, None, optional
            Name of value to look up. If None, defaults to ['value'].
            "Deep" syntax either ['deep', 'value'], ('deep', 'value',)
            or 'deep.value.'

            If field_path is None, then ['value'] will be the default.

        Returns
        -------
        int
            Number of valid <field_path> values in the events.
        """
        return self._collection.size_valid(field_path)

    def count(self):
        """alias for size.

        Returns
        -------
        int
            Number of rows in series.
        """
        return self.size()

    # sum/min/max etc

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
            Summed values
        """
        return self._collection.sum(field_path, filter_func)

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
            Max value
        """
        return self._collection.max(field_path, filter_func)

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
            Min value
        """
        return self._collection.min(field_path, filter_func)

    def avg(self, field_spec=None, filter_func=None):
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
            Average value
        """
        return self._collection.avg(field_spec, filter_func)

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
            Mean value
        """
        return self._collection.mean(field_path, filter_func)

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
            Median value
        """
        return self._collection.median(field_path, filter_func)

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
            Standard deviation
        """
        return self._collection.stdev(field_path, filter_func)

    def percentile(self, perc, field_path, method='linear', filter_func=None):
        """Gets percentile perc within the Collection. Numpy under
        the hood.

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
        return self._collection.percentile(perc, field_path, method, filter_func)

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
        return self._collection.quantile(num, field_path, method)

    def aggregate(self, func, field_path=None):
        """Aggregates the events down using a user defined function to
        do the reduction.

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
        dict
            Dict of reduced values
        """
        return self._collection.aggregate(func, field_path)

    def pipeline(self):
        """Returns a new Pipeline with input source being initialized to
        this TimeSeries collection. This allows pipeline operations
        to be chained directly onto the TimeSeries to produce a new
        TimeSeries or Event result.

        Returns
        -------
        Pipeline
            New pipline.
        """
        # gotta avoid circular imports by deferring
        from .pipeline import Pipeline
        return Pipeline().from_source(self._collection)

    def map(self, op):  # pylint: disable=invalid-name
        """Takes an operator that is used to remap events from this TimeSeries to
         new set of Events. The result is returned via the callback.

        Parameters
        ----------
        op : function
            An operator which will be passed each event and which should
            return a new event.

        Returns
        -------
        TimeSeries
            A clone of this TimeSeries with a new Collection generated by
            the map operation.
        """
        coll = self.pipeline().map(op).to_keyed_collections()
        return self.set_collection(coll.get('all'))

    def select(self, field_spec=None):  # pylint: disable=invalid-name
        """call select on the pipeline.

        Parameters
        ----------
        field_spec : str, list, tuple, None, optional
            Column or columns to look up. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this.

            If None, the default 'value' column will be used.

        Returns
        -------
        TimeSeries
            A clone of this TimeSeries with a new Collection generated by
            the select operation.
        """
        coll = self.pipeline().select(field_spec).to_keyed_collections()
        return self.set_collection(coll.get('all'))

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
            Name of new column containing collapsed values.
        reducer : Function to pass to reducer.
            function
        append : bool, optional
            Append collapsed column to existing data or fresh data payload.

        Returns
        -------
        TimeSeries
            A new time series from the collapsed columns.
        """

        coll = (
            self.pipeline()
            .collapse(field_spec_list, name, reducer, append)
            .to_keyed_collections()
        )

        return self.set_collection(coll.get('all'))

    def rename_columns(self, rename_map):
        """TimeSeries.map() helper function to rename columns in the underlying
        events.

        Takes a dict of columns to rename::

            new_ts = ts.rename_columns({'in': 'new_in', 'out': 'new_out'})

        Returns a new time series containing new events. Columns not
        in the dict will be retained and not renamed.

        NOTE: as the name implies, this will only rename the main
        "top level" (ie: non-deep) columns. If you need more
        extravagant renaming, roll your own using map().

        Parameters
        ----------
        rename_map : dict
            Dict of columns to rename.

        Returns
        -------
        TimeSeries
            A clone of this TimeSeries with a new Collection generated by
            the map operation.
        """

        def rename(event):
            """renaming mapper function."""

            def renamed_dict(event):
                """Handle renaming the columns in the data regardless
                of event type."""

                new_dict = thaw(event.data())

                for old, new in list(rename_map.items()):
                    new_dict[new] = new_dict.pop(old)

                return new_dict

            renamed_data = renamed_dict(event)

            # reassemble as per apropos for the event type
            # with the newly renamed data payload

            if isinstance(event, Event):
                return Event(event.timestamp(), renamed_data)
            elif isinstance(event, TimeRangeEvent):
                return TimeRangeEvent(
                    (event.begin(), event.end()),
                    renamed_data
                )
            elif isinstance(event, IndexedEvent):
                return IndexedEvent(event.index(), renamed_data)

            # an else isn't possible since Collection sanitizes
            # the input.

        return self.map(rename)

    def fill(self, field_spec=None, method='zero', fill_limit=None):
        """Take the data in this timeseries and "fill" any missing
        or invalid values. This could be setting None values to zero
        so mathematical operations will succeed, interpolate a new
        value, or pad with the previously given value.

        Parameters
        ----------
        field_spec : str, list, tuple, None, optional
            Column or columns to look up. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this.

            If None, the default column field 'value' will be used.
        method : str, optional
            Filling method: zero | linear | pad
        fill_limit : None, optional
            Set a limit on the number of consecutive events will be filled
            before it starts returning invalid values. For linear fill,
            no filling will happen if the limit is reached before a valid
            value is found.

        Returns
        -------
        TimeSeries
            A clone of this TimeSeries with a new Collection generated by
            the fill operation.
        """

        pip = self.pipeline()

        if method in ('zero', 'pad') or \
                (method == 'linear' and not isinstance(field_spec, list)):
            # either not linear or linear with a single path, or None.
            # just install one Filler in the chain and go.
            pip = pip.fill(field_spec, method, fill_limit)
        elif method == 'linear' and isinstance(field_spec, list):
            # linear w/multiple paths, chain multiple Fillers for
            # asymmetric column filling.

            for fpath in field_spec:
                pip = pip.fill(fpath, method, fill_limit)
        else:
            msg = 'method {0} is not valid'.format(method)
            raise TimeSeriesException(msg)

        coll = (
            pip
            .to_keyed_collections()
        )

        return self.set_collection(coll.get('all'))

    def align(self, field_spec=None, window='5m', method='linear', limit=None):
        """
        Align entry point
        """
        coll = (
            self.pipeline()
            .align(field_spec, window, method, limit)
            .to_keyed_collections()
        )

        return self.set_collection(coll.get('all'))

    def rate(self, field_spec=None, allow_negative=True):
        """
        derive entry point
        """
        coll = (
            self.pipeline()
            .rate(field_spec, allow_negative)
            .to_keyed_collections()
        )

        return self.set_collection(coll.get('all'))

    def __str__(self):
        """call to_string()"""
        return self.to_string()  # pragma: no cover

    # Windowing and rollups

    def fixed_window_rollup(self, window_size, aggregation, to_events=False):
        """
        Builds a new TimeSeries by dividing events within the TimeSeries
        across multiple fixed windows of size `windowSize`.

        Note that these are windows defined relative to Jan 1st, 1970,
        and are UTC, so this is best suited to smaller window sizes
        (hourly, 5m, 30s, 1s etc), or in situations where you don't care
        about the specific window, just that the data is smaller.

        Each window then has an aggregation specification applied as
        `aggregation`. This specification describes a mapping of output
        columns to fieldNames to aggregation functions. For example::

            {
                'in_avg': {'in': Functions.avg()},
                'out_avg': {'out': Functions.avg()},
                'in_max': {'in': Functions.max()},
                'out_max': {'out': Functions.max()},
            }

        will aggregate both the "in" and "out" columns, using the avg
        aggregation function will perform avg and max aggregations on the
        in and out columns, across all events within each hour, and the
        results will be put into the 4 new columns in_avg, out_avg, in_max
        and out_max.

        Example::

            timeseries = TimeSeries(data)
            daily_avg = timeseries.fixed_window_rollup('1d',
                {'value_avg': {'value': Functions.avg()}}
            )

        Parameters
        ----------
        window_size : str
            The size of the window, e.g. '6h' or '5m'
        aggregation : Options
            The aggregation specification
        to_events : bool, optional
            Convert to events

        Returns
        -------
        TimeSeries
            The resulting rolled up TimeSeries
        """

        aggregator_pipeline = (
            self.pipeline()
            .window_by(window_size)
            .emit_on('discard')
            .aggregate(aggregation)
        )

        event_type_pipeline = aggregator_pipeline.as_events() if to_events \
            else aggregator_pipeline

        colls = event_type_pipeline.clear_window().to_keyed_collections()

        return self.set_collection(colls.get('all'))

    def hourly_rollup(self, aggregation, to_events=False):
        """
        Builds a new TimeSeries by dividing events into hours. The hours are
        in either local or UTC time, depending on if utc(true) is set on the
        Pipeline.

        Each window then has an aggregation specification applied as
        `aggregation`. This specification describes a mapping of output
        columns to fieldNames to aggregation functions. For example::

            {
                'in_avg': {'in': Functions.avg()},
                'out_avg': {'out': Functions.avg()},
                'in_max': {'in': Functions.max()},
                'out_max': {'out': Functions.max()},
            }

        will aggregate both the "in" and "out" columns, using the avg
        aggregation function will perform avg and max aggregations on the
        in and out columns, across all events within each hour, and the
        results will be put into the 4 new columns in_avg, out_avg, in_max
        and out_max.

        Example::

            timeseries = TimeSeries(data)
            hourly_max_temp = timeseries.hourly_rollup(
                {'max_temp': {'temperature': Functions.max()}}
            )

        Parameters
        ----------
        aggregation : dict
            The aggregation specification e.g. {'max_temp': {'temperature': Functions.max()}}
        to_event : bool, optional
            Do conversion to Event objects

        Returns
        -------
        TimeSeries
            The resulting rolled up TimeSeries.
        """
        return self.fixed_window_rollup('1h', aggregation, to_events)

    def daily_rollup(self, aggregation, to_events=False):
        """
        Builds a new TimeSeries by dividing events into days. The days are
        in either local or UTC time, depending on if utc(true) is set on the
        Pipeline.

        Each window then has an aggregation specification applied as
        `aggregation`. This specification describes a mapping of output
        columns to fieldNames to aggregation functions. For example::

            {
                'in_avg': {'in': Functions.avg()},
                'out_avg': {'out': Functions.avg()},
                'in_max': {'in': Functions.max()},
                'out_max': {'out': Functions.max()},
            }

        will aggregate both the "in" and "out" columns, using the avg
        aggregation function will perform avg and max aggregations on the
        in and out columns, across all events within each day, and the
        results will be put into the 4 new columns in_avg, out_avg, in_max
        and out_max.

        Example::

            timeseries = TimeSeries(data)
            hourly_max_temp = timeseries.daily_rollup(
                {'max_temp': {'temperature': Functions.max()}}
            )

        This helper function renders the aggregations in localtime. If you
        want to render in UTC use .fixed_window_rollup() with the appropriate
        window size.

        Parameters
        ----------
        aggregation : dict
            The aggregation specification e.g. {'max_temp': {'temperature': Functions.max()}}
        to_event : bool, optional
            Do conversion to Event objects

        Returns
        -------
        TimeSeries
            The resulting rolled up TimeSeries.
        """
        return self._rollup('daily', aggregation, to_events, utc=False)

    def monthly_rollup(self, aggregation, to_events=False):
        """
        Builds a new TimeSeries by dividing events into months. The months are
        in either local or UTC time, depending on if utc(true) is set on the
        Pipeline.

        Each window then has an aggregation specification applied as
        `aggregation`. This specification describes a mapping of output
        columns to fieldNames to aggregation functions. For example::

            {
                'in_avg': {'in': Functions.avg()},
                'out_avg': {'out': Functions.avg()},
                'in_max': {'in': Functions.max()},
                'out_max': {'out': Functions.max()},
            }

        will aggregate both the "in" and "out" columns, using the avg
        aggregation function will perform avg and max aggregations on the
        in and out columns, across all events within each month, and the
        results will be put into the 4 new columns in_avg, out_avg, in_max
        and out_max.

        Example::

            timeseries = TimeSeries(data)
            hourly_max_temp = timeseries.monthly_rollup(
                {'max_temp': {'temperature': Functions.max()}}
            )

        This helper function renders the aggregations in localtime. If you
        want to render in UTC use .fixed_window_rollup() with the appropriate
        window size.

        Parameters
        ----------
        aggregation : dict
            The aggregation specification e.g. {'max_temp': {'temperature': Functions.max()}}
        to_event : bool, optional
            Do conversion to Event objects

        Returns
        -------
        TimeSeries
            The resulting rolled up TimeSeries.
        """
        return self._rollup('monthly', aggregation, to_events, utc=False)

    def yearly_rollup(self, aggregation, to_events=False):
        """
        Builds a new TimeSeries by dividing events into years. The years are
        in either local or UTC time, depending on if utc(true) is set on the
        Pipeline.

        Each window then has an aggregation specification applied as
        `aggregation`. This specification describes a mapping of output
        columns to fieldNames to aggregation functions. For example::

            {
                'in_avg': {'in': Functions.avg()},
                'out_avg': {'out': Functions.avg()},
                'in_max': {'in': Functions.max()},
                'out_max': {'out': Functions.max()},
            }

        will aggregate both the "in" and "out" columns, using the avg
        aggregation function will perform avg and max aggregations on the
        in and out columns, across all events within each year, and the
        results will be put into the 4 new columns in_avg, out_avg, in_max
        and out_max.

        Example::

            timeseries = TimeSeries(data)
            hourly_max_temp = timeseries.monthly_rollup(
                {'max_temp': {'temperature': Functions.max()}}
            )

        This helper function renders the aggregations in localtime. If you
        want to render in UTC use .fixed_window_rollup() with the appropriate
        window size.

        Parameters
        ----------
        aggregation : dict
            The aggregation specification e.g. {'max_temp': {'temperature': Functions.max()}}
        to_event : bool, optional
            Do conversion to Event objects

        Returns
        -------
        TimeSeries
            The resulting rolled up TimeSeries.
        """
        return self._rollup('yearly', aggregation, to_events, utc=False)

    def _rollup(self, interval, aggregation, to_events=False, utc=True):

        aggregator_pipeline = (
            self.pipeline()
            .window_by(interval, utc=utc)
            .emit_on('discard')
            .aggregate(aggregation)
        )

        event_type_pipeline = aggregator_pipeline.as_events() if to_events \
            else aggregator_pipeline

        colls = event_type_pipeline.clear_window().to_keyed_collections()

        return self.set_collection(colls.get('all'))

    def collect_by_fixed_window(self, window_size):
        """Summary

        Parameters
        ----------
        window_size : str
            The window size - 1d, 6h, etc

        Returns
        -------
        list or dict
            Returns the _results attribute from a Pipeline object after processing.
            Will contain Collection objects.
        """
        return (
            self.pipeline()
            .window_by(window_size)
            .emit_on('discard')
            .to_keyed_collections()
        )

    # Static methods

    @staticmethod
    def equal(series1, series2):
        """Check equality - same instance.

        Parameters
        ----------
        series1 : TimeSeries
            A time series
        series2 : TimeSeries
            Another time series

        Returns
        -------
        bool
            Are the two the same instance?
        """
        # pylint: disable=protected-access
        return bool(
            series1._data is series2._data and
            series1._collection is series2._collection
        )

    @staticmethod
    def same(series1, series2):
        """Implements JS Object.is() - same values

        Parameters
        ----------
        series1 : TimeSeries
            A time series
        series2 : TimeSeries
            Another time series

        Returns
        -------
        bool
            Do the two have the same values?
        """
        # pylint: disable=protected-access
        return bool(
            series1._data == series2._data and
            Collection.same(series1._collection, series2._collection)
        )

    @staticmethod
    def timeseries_list_reduce(data, series_list, reducer, field_spec=None):
        """for each series, map events to the same timestamp/index

        Parameters
        ----------
        data : dict or pmap
            Data payload
        series_list : list
            List of TimeSeries objects.
        reducer : function
            reducer function
        field_spec : list, str, None, optional
            Column or columns to look up. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this.

            Can be set to None if the reducer does not require a field spec.

        Returns
        -------
        TimeSeries
            New time series containing the mapped events.
        """

        event_map = collections.OrderedDict()

        # sort on the begin times which might be out of order. this
        # ensures that the event map wil be generated chronologically.
        series_list = sorted(series_list, key=lambda x: x.begin())

        for i in series_list:
            for evn in i.events():
                key = None
                if isinstance(evn, Event):
                    key = evn.timestamp()
                elif isinstance(evn, IndexedEvent):
                    key = evn.index_as_string()
                elif isinstance(evn, TimeRangeEvent):
                    key = evn.timerange().to_utc_string()

                if key not in event_map:
                    # keep keys in insert order
                    event_map.update({key: list()})

                event_map[key].append(evn)

        events = list()

        for v in list(event_map.values()):
            if field_spec is None:
                event = reducer(v)
            else:
                event = reducer(v, field_spec)

            events.append(event)

        return TimeSeries(dict(events=events, **data))

    @staticmethod
    def timeseries_list_merge(data, series_list):
        """Merge a list of time series.

        Parameters
        ----------
        data : dict or pvector
            Data payload
        series_list : list
            List of TimeSeries instances.

        Returns
        -------
        TimeSeries
            New TimeSeries from merge.
        """
        return TimeSeries.timeseries_list_reduce(data, series_list, Event.merge)

    @staticmethod
    def timeseries_list_sum(data, series_list, field_spec):
        """
        Takes a list of TimeSeries and sums them together to form a new
        Timeseries.

        const ts1 = new TimeSeries(weather1)
        const ts2 = new TimeSeries(weather2)
        const sum = TimeSeries.timeseries_list_sum({name: "sum"}, [ts1, ts2], ["temp"])

        Parameters
        ----------
        data : dict
            Data payload
        series_list : list
            List of TimeSeries objects
        field_spec : list, str, None, optional
            Column or columns to look up. If you need to retrieve multiple deep
            nested values that ['can.be', 'done.with', 'this.notation'].
            A single deep value with a string.like.this.  If None, all columns
            will be operated on.

        Returns
        -------
        TimeSeries
            New time series with summed values.
        """
        return TimeSeries.timeseries_list_reduce(data, series_list, Event.sum, field_spec)
