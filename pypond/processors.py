#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Offset is a simple processor used by the testing code to verify Pipeline behavior.
"""

import copy
from operator import truediv

import six

from pyrsistent import thaw

from .bases import Observable
from .collection import Collection
from .event import Event
from .exceptions import ProcessorException, ProcessorWarning
from .index import Index
from .indexed_event import IndexedEvent
from .pipeline_out import Collector, CollectionOut, EventOut
from .range import TimeRange
from .timerange_event import TimeRangeEvent
from .util import (
    dt_from_ms,
    is_function,
    is_pipeline,
    is_valid,
    ms_from_dt,
    nested_get,
    nested_set,
    Options,
    unique_id,
)

# Base for all pipeline processors


def add_prev_to_chain(n, chain):  # pylint: disable=invalid-name
    """
    Recursive function to add values to the chain.
    """
    chain.append(n)

    if is_pipeline(n.prev()):
        chain.append(n.prev().input())
        return chain
    else:
        add_prev_to_chain(n.prev(), chain)


class Processor(Observable):
    """
    Base class for all pipeline processors.
    """

    def __init__(self, arg1, options):
        super(Processor, self).__init__()

        self._log('Processor.init')

        self._id = unique_id('processor-')

        self._pipeline = None
        self._prev = None

        if is_pipeline(arg1):
            self._pipeline = arg1
            self._prev = options.prev

    def prev(self):
        """Return prev"""
        return self._prev

    def pipeline(self):
        """Return the pipeline"""
        return self._pipeline

    def chain(self):
        """Return the chain"""
        chain = [self]

        if is_pipeline(self.prev()):
            chain.append(self.prev().input())
            return chain
        else:
            return add_prev_to_chain(self.prev(), chain)

    # flush() is inherited from Observable


class Offset(Processor):
    """
    A simple processor used by the testing code to verify Pipeline behavior.
    """

    def __init__(self, arg1, options=Options()):
        """A simple processor used by the testing code to verify Pipeline behavior.

        Parameters
        ----------
        arg1 : Offset or Pipeline
            Pipeline or copy constructor
        options : Options, optional
            Pipeline Options object.

        Raises
        ------
        ProcessorException
            Raised on bad arg types.
        """
        super(Offset, self).__init__(arg1, options)

        self._log('Offset.init', 'uid: {0}'.format(self._id))

        self._by = None
        self._field_spec = None

        if isinstance(arg1, Offset):
            # Copy constructor
            # pylint: disable=protected-access
            self._by = arg1._by
            self._field_spec = arg1._field_spec
        elif is_pipeline(arg1):
            self._by = options.by
            self._field_spec = options.field_spec
        else:
            msg = 'Unknown arg to Offset constructor: {a}'.format(a=arg1)
            raise ProcessorException(msg)

    def clone(self):
        """Clone this Offset processor.

        Returns
        -------
        Offset
            Cloned offset object.
        """
        return Offset(self)

    def add_event(self, event):
        """
        Output an even that is offset by a certain value.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """

        self._log('Offset.add_event', event)

        if self.has_observers():
            selected = Event.selector(event, self._field_spec)
            data = dict()

            for k, v in list(selected.data().items()):
                offset_value = v + self._by
                data[k] = offset_value

            output_event = event.set_data(data)

            self._log('Offset.add_event', 'emitting: {0}'.format(output_event))

            self.emit(output_event)


class Collapser(Processor):
    """
    A processor which takes a fieldSpec and returns a new event
    with a new column that is a collapsed result of the selected
    columns. To collapse the columns it uses the supplied reducer
    function. Optionally the new column can completely replace
    the existing columns in the event.

    Parameters
    ----------
    arg1 : Collapser or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        super(Collapser, self).__init__(arg1, options)

        if isinstance(arg1, Collapser):
            # pylint: disable=protected-access
            self._field_spec_list = arg1._field_spec_list
            self._name = arg1._name
            self._reducer = arg1._reducer
            self._append = arg1._append
        elif is_pipeline(arg1):
            self._field_spec_list = options.field_spec_list
            self._name = options.name
            self._reducer = options.reducer
            self._append = options.append
        else:
            msg = 'Unknown arg to Collapser: {0}'.format(arg1)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Collapser(self)

    def add_event(self, event):
        """
        Perform the collapse operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            evn = event.collapse(
                self._field_spec_list,
                self._name,
                self._reducer,
                self._append
            )

        self._log('Collapser.add_event', 'emitting: {0}'.format(evn))
        self.emit(evn)


class Selector(Processor):
    """
    A processor which takes a fieldSpec as its only argument
    and returns a new event with only the selected columns

    Parameters
    ----------
    arg1 : Selector or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        super(Selector, self).__init__(arg1, options)

        self._log('Selector.init', 'uid: {0}'.format(self._id))

        self._field_spec = None

        if isinstance(arg1, Selector):
            self._field_spec = arg1._field_spec  # pylint: disable=protected-access
        elif is_pipeline(arg1):
            self._field_spec = options.field_spec
        else:
            msg = 'Unknown arg to Selector: {0}'.format(arg1)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Selector(self)

    def add_event(self, event):
        """
        Perform the select operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            evn = Event.selector(event, self._field_spec)
            self._log('Selector.add_event', 'emitting: {0}'.format(evn))
            self.emit(evn)


class Filter(Processor):
    """
    A processor which takes an operator as its only option
    and uses that to either output a new event.

    Parameters
    ----------
    arg1 : Filter or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the mapper"""

        super(Filter, self).__init__(arg1, options)

        self._log('Filter.init', 'uid: {0}'.format(self._id))

        self._op = None

        if isinstance(arg1, Filter):
            self._op = arg1._op  # pylint: disable=protected-access
        elif is_pipeline(arg1):
            self._op = options.op
        else:
            msg = 'Unknown arg to Filter: {0}'.format(arg1)
            raise ProcessorException(msg)

        if callable(self._op) is False:
            msg = 'op: {0} is not a callable function'.format(self._op)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Filter(self)

    def add_event(self, event):
        """
        Perform the filter operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            if self._op(event):
                self._log('Filter.add_event', 'emitting: {0}'.format(event))
                self.emit(event)


class Taker(Processor):
    """
    A processor which takes an operator as its only option
    and uses that to either output the event or skip the
    event

    Parameters
    ----------
    arg1 : Taker or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the mapper"""

        super(Taker, self).__init__(arg1, options)

        self._log('Taker.init', 'uid: {0}'.format(self._id))

        self._limit = None
        self._window_type = None
        self._window_duration = None
        self._group_by = None

        self._count = dict()

        if isinstance(arg1, Taker):
            # pylint: disable=protected-access
            self._limit = arg1._limit
            self._window_type = arg1._window_type
            self._window_duration = arg1._window_duration
            self._group_by = arg1._group_by
        elif is_pipeline(arg1):
            self._limit = options.limit
            self._window_type = arg1.get_window_type()
            self._window_duration = arg1.get_window_duration()
            self._group_by = arg1.get_group_by()
        else:
            msg = 'Unknown arg to Taker: {0}'.format(arg1)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Taker(self)

    def add_event(self, event):
        """
        Output an event that is offset.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():

            ts = event.timestamp()
            window_key = None

            if self._window_type == 'fixed':
                window_key = Index.get_index_string(self._window_duration, ts)
            else:
                window_key = self._window_type

            group_by_key = self._group_by(event)

            coll_key = '{wk}::{gbk}'.format(wk=window_key, gbk=group_by_key) if \
                group_by_key is not None else window_key

            if coll_key not in self._count:
                self._count[coll_key] = 0

            self._count[coll_key] += 1

            if self._count.get(coll_key) <= self._limit:
                self._log('Taker.add_event', 'collection key: {0}'.format(coll_key))
                self._log(
                    'Taker.add_event',
                    'count: {0} limit: {1}'.format(
                        self._count.get(coll_key),
                        self._limit
                    )
                )
                self._log('Taker.add_event', 'emitting: {0}'.format(event))
                self.emit(event)


class Aggregator(Processor):
    """
    An Aggregator takes incoming events and adds them to a Collector
    with given windowing and grouping parameters. As each Collection is
    emitted from the Collector it is aggregated into a new event
    and emitted from this Processor.

    Parameters
    ----------
    arg1 : Aggregator or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the aggregator"""

        super(Aggregator, self).__init__(arg1, options)

        self._log('Aggregator.init', 'uid: {0}'.format(self._id))

        self._fields = None
        self._window_type = None
        self._window_duration = None
        self._group_by = None
        self._emit_on = None
        self._utc = None

        if isinstance(arg1, Aggregator):
            self._log('Aggregator.init', 'copy ctor')
            # pylint: disable=protected-access
            self._fields = arg1._fields
            self._window_type = arg1._window_type
            self._window_duration = arg1._window_duration
            self._group_by = arg1._group_by
            self._emit_on = arg1._emit_on
            self._utc = arg1._utc

        elif is_pipeline(arg1):
            self._log('Aggregator.init', 'pipeline')

            pipeline = arg1

            self._window_type = pipeline.get_window_type()
            self._window_duration = pipeline.get_window_duration()
            self._group_by = pipeline.get_group_by()
            self._emit_on = pipeline.get_emit_on()
            self._utc = pipeline.get_utc()

            # yes it does have a fields member pylint, it's just magic
            # pylint: disable=no-member

            if options.fields is None:
                msg = 'Aggregator: constructor needs an aggregator field mapping'
                raise ProcessorException(msg)

            if not isinstance(options.fields, dict):
                msg = 'options.fields must be a dict'
                raise ProcessorException(msg)

            for k, v in list(options.fields.items()):
                if not isinstance(k, str) and not isinstance(k, tuple):
                    msg = 'Aggregator: field of unknown type: {0}'.format(k)
                    raise ProcessorException(msg)

                if not is_function(v):
                    msg = 'Aggregator: field values must be a function, got: {0}'.format(v)
                    raise ProcessorException(msg)

            if pipeline.mode() == 'stream':
                if pipeline.get_window_type() is None \
                        or pipeline.get_window_duration() is None:
                    msg = 'Unable to aggregate/no windowing strategy specified in pipeline'
                    raise ProcessorException(msg)

            self._fields = options.fields

        else:
            msg = 'Unknown arg to Aggregator: {0}'.format(arg1)
            raise ProcessorException(msg)

        self._collector = Collector(
            Options(
                window_type=self._window_type,
                window_duration=self._window_duration,
                group_by=self._group_by,
                emit_on=self._emit_on,
                utc=self._utc,
            ),
            self._collector_callback
        )

    def _collector_callback(self, collection, window_key, group_by_key='all'):
        """
        This is the callback passed to the collector, normally done
        as an inline in the Javascript source.
        """

        self._log(
            'Aggregator._collector_callback',
            'coll:{0}, wkey: {1}, gbkey: {2}'.format(collection, window_key, group_by_key)
        )

        new_d = dict()

        for fld, func in list(self._fields.items()):

            field_list = [fld] if isinstance(fld, str) else list(fld)

            self._log(
                'Aggregator._collector_callback',
                'fld: {0}, func: {1} field_list: {2}'.format(fld, func, field_list)
            )

            for field_path in field_list:
                field_value = collection.aggregate(func, field_path)
                self._log(
                    'Aggregator._collector_callback',
                    'field_value: {0}'.format(field_value)
                )
                field_name = field_path.split('.').pop()
                new_d[field_name] = field_value

        event = None

        self._log(
            'Aggregator._collector_callback',
            'new_d: {0}'.format(new_d)
        )

        if window_key == 'global':
            event = TimeRangeEvent(collection.range(), new_d)
        else:
            # Pipeline.window_by() will force utc=True if
            # a fixed window size is being used. Otherwise,
            # the default is True but can be changed.
            event = IndexedEvent(window_key, new_d, self._utc)  # pylint: disable=redefined-variable-type

        self._log(
            'Aggregator._collector_callback',
            'emitting: {0}'.format(event)
        )

        self.emit(event)

    def clone(self):
        """clone it."""
        return Aggregator(self)

    def flush(self):
        """flush."""
        self._log('Aggregator.flush')
        self._collector.flush_collections()
        super(Aggregator, self).flush()

    def add_event(self, event):
        """Add an event to the collector.

        Parameters
        ----------
        event : Event
            An event object
        """
        if self.has_observers():
            self._log('Aggregator.add_event', 'adding: {0}'.format(event))
            self._collector.add_event(event)


class Converter(Processor):
    """
    A processor that converts an event type to another event type.

    Parameters
    ----------
    arg1 : Converter or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the aggregator"""

        super(Converter, self).__init__(arg1, options)

        self._log('Converter.init', 'uid: {0}'.format(self._id))

        self._convert_to = None
        self._duration = None
        self._duration_string = None
        self._alignment = None

        if isinstance(arg1, Converter):
            # pylint: disable=protected-access
            self._convert_to = arg1._convert_to
            self._duration = arg1._duration
            self._duration_string = arg1._duration_string
            self._alignment = arg1._alignment
        elif is_pipeline(arg1):

            if options.type is None:
                msg = 'Converter: ctor needs type in options'
                raise ProcessorException(msg)

            if options.type == Event or options.type == TimeRangeEvent \
                    or options.type == IndexedEvent:
                self._convert_to = options.type
            else:
                msg = 'Unable to interpret type argument passed to Converter constructor'
                raise ProcessorException(msg)

            if options.type == TimeRangeEvent or options.type == IndexedEvent:
                if options.duration is not None and isinstance(options.duration, str):
                    self._duration = Index.window_duration(options.duration)
                    self._duration_string = options.duration

            self._alignment = options.alignment if options.alignment is not None \
                else 'center'

        else:
            msg = 'Unknown arg to Converter: {0}'.format(arg1)
            raise ProcessorException(msg)

        self._log('Converter.init', 'options: {0}'.format(options))

    def clone(self):
        """clone it."""
        return Converter(self)

    def convert_event(self, event):
        """Convert an Event

        Parameters
        ----------
        event : Event
            An incoming Event object for conversion.

        Returns
        -------
        TimeRangeEvent or IndexedEvent
            The converted Event.
        """
        if self._convert_to == Event:
            return event
        elif self._convert_to == TimeRangeEvent:

            begin = None
            end = None

            if self._duration is None:
                msg = 'Duration expected in converter'
                raise ProcessorException(msg)

            if self._alignment == 'front':
                begin = ms_from_dt(event.timestamp())
                end = begin + self._duration
            elif self._alignment == 'center':
                begin = ms_from_dt(event.timestamp()) - int(self._duration) / 2
                end = ms_from_dt(event.timestamp()) + int(self._duration) / 2
            elif self._alignment == 'behind':
                end = ms_from_dt(event.timestamp())
                begin = end - self._duration
            else:
                msg = 'Unknown alignment of converter'
                raise ProcessorException(msg)

            range_list = [int(begin), int(end)]

            self._log('Converter.convert_event', 'range: {0}'.format(range_list))

            rng = TimeRange(range_list)
            return TimeRangeEvent(rng, event.data())

        elif self._convert_to == IndexedEvent:
            ts = event.timestamp()
            istr = Index.get_index_string(self._duration_string, ts)
            return IndexedEvent(istr, event.data())

    def convert_time_range_event(self, event):
        """Convert a TimeRangeEvent

        Parameters
        ----------
        event : TimeRangeEvent
            An incoming TimeRangeEvent object for conversion.

        Returns
        -------
        Event
            The converted TimeRangeEvent. Can not convert to IndexedEvent.
        """
        if self._convert_to == TimeRangeEvent:
            return event
        elif self._convert_to == Event:

            ts = None

            if self._alignment == 'lag':
                ts = event.begin()
            elif self._alignment == 'center':
                epoch = (ms_from_dt(event.begin()) + ms_from_dt(event.end())) // 2
                ts = dt_from_ms(epoch)
            elif self._alignment == 'lead':
                ts = event.end()

            self._log(
                'Converter.convert_time_range_event',
                'Event - align: {0} ts: {1}'.format(self._alignment, ts)
            )

            return Event(ts, event.data())

        elif self._convert_to == IndexedEvent:
            msg = 'Can not convert TimeRangeEvent to an IndexedEvent'
            raise ProcessorException(msg)

    def convert_indexed_event(self, event):
        """Convert an IndexedEvent

        Parameters
        ----------
        event : IndexedEvent
            An incoming IndexedEvent object for conversion.

        Returns
        -------
        TimeRangeEvent or Event
            The converted IndexedEvent.
        """

        if self._convert_to == IndexedEvent:
            return event
        elif self._convert_to == Event:

            ts = None

            if self._alignment == 'lag':
                ts = event.begin()
            elif self._alignment == 'center':
                epoch = (ms_from_dt(event.begin()) + ms_from_dt(event.end())) // 2
                ts = dt_from_ms(epoch)
            elif self._alignment == 'lead':
                ts = event.end()

            self._log(
                'Converter.convert_indexed_event',
                'Event - align: {0} ts: {1}'.format(self._alignment, ts)
            )

            return Event(ts, event.data())
        elif self._convert_to == TimeRangeEvent:
            return TimeRangeEvent(event.timerange(), event.data())

    def add_event(self, event):
        """
        Perform the conversion on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            output_event = None

            # pylint: disable=redefined-variable-type

            if isinstance(event, Event):
                output_event = self.convert_event(event)
            elif isinstance(event, TimeRangeEvent):
                output_event = self.convert_time_range_event(event)
            elif isinstance(event, IndexedEvent):
                output_event = self.convert_indexed_event(event)
            else:
                msg = 'Unknown event type received'
                raise ProcessorException(msg)

            self._log('Converter.add_event', 'emitting: {0}'.format(output_event))

            self.emit(output_event)


class Filler(Processor):
    """
    A processor that fills missing/invalid values in the event
    with new values (zero, interpolated or padded).

    Number of filled events in new series can be controlled by
    putting .take() in the pipeline chain.

    Parameters
    ----------
    arg1 : Filler or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the mapper"""

        super(Filler, self).__init__(arg1, options)

        self._log('Filler.init', 'uid: {0}'.format(self._id))

        # options
        self._field_spec = None
        self._method = None
        self._mode = None
        self._emit_on = None

        # internal members
        self._previous_event = None

        if isinstance(arg1, Filler):
            # pylint: disable=protected-access
            self._field_spec = arg1._field_spec
            self._method = arg1._method
            self._mode = arg1._mode
            self._emit_on = arg1._emit_on
        elif is_pipeline(arg1):
            self._field_spec = options.field_spec
            self._method = options.method
            self._mode = arg1.mode()
            self._emit_on = arg1.get_emit_on()
        else:
            msg = 'Unknown arg to Filler: {0}'.format(arg1)
            raise ProcessorException(msg)

        self._log('Filler.init.Options', options)

        if self._method not in ('zero', 'pad', 'linear'):
            msg = 'Unknown method {0} passed to Filler'.format(self._method)
            raise ProcessorException(msg)

        if self._method == 'linear' and self._mode == 'stream':
            msg = 'Can not do linear interpolation in stream mode'
            raise ProcessorException(msg)

        if self._method == 'linear' and self._emit_on != 'flush':
            msg = 'Set emit_on to "flush" when doing linear interpolation'
            raise ProcessorException(msg)

        if isinstance(self._field_spec, six.string_types):
            self._field_spec = [self._field_spec]

    def clone(self):
        """clone it."""
        return Filler(self)

    def _recurse(self, data, keys=()):
        """
        Do the actual recursion and yield the keys to _generate_paths()
        """
        if isinstance(data, dict):
            for key in list(data.keys()):
                for path in self._recurse(data[key], keys + (key,)):
                    yield path
        else:
            yield keys

    def _generate_paths(self, new_data):
        """
        Return a list of field spec paths for the entire
        data dict that can  be used by pypond.util.nested_set
        and nested_get for filling. Mostly just a
        wrapper to aggregate the results from _recurse().
        """

        paths = list()

        for key in self._recurse(new_data):
            paths.append(key)

        return paths

    def _fill_specs(self, data, paths):
        """
        Process and fill the values at the paths as apropos.
        """
        for path in paths:

            field_path = self._field_path_to_array(path)

            val = nested_get(data, field_path)

            # this is pointing at a path that does not exist
            if val == 'bad_path':
                self._warn('path does not exist: {0}'.format(field_path), ProcessorWarning)
                continue

            # if the terminal value is a list, fill the list
            if isinstance(val, list):
                raise NotImplementedError
                # return

            if not is_valid(val):
                # massage the path per selected method

                if self._method == 'zero':  # set to zero
                    nested_set(data, field_path, 0)

                elif self._method == 'pad':  # set to previous value
                    if self._previous_event is not None:
                        if is_valid(self._previous_event.get(field_path)):
                            nested_set(
                                data, field_path,
                                self._previous_event.get(field_path)
                            )

                elif self._method == 'linear':
                    # no-op here, interpolation has to happen
                    # when the operation is flushed.
                    # just stubbing in the condition
                    pass

    def add_event(self, event):
        """
        Perform the fill operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():

            new_data = thaw(event.data())

            if self._field_spec is None:
                # generate a list of all possible field paths
                # if no field spec is specified.
                paths = self._generate_paths(new_data)
                self._fill_specs(new_data, paths)
            else:
                self._fill_specs(new_data, self._field_spec)

            emitted_event = None

            # yes pylint, we know
            # pylint: disable=redefined-variable-type

            if isinstance(event, Event):
                emitted_event = Event(event.timestamp(), new_data)
            elif isinstance(event, TimeRangeEvent):
                emitted_event = TimeRangeEvent(
                    (event.begin(), event.end()),
                    new_data
                )
            elif isinstance(event, IndexedEvent):
                emitted_event = IndexedEvent(event.index(), new_data)

            # end filling logic

            self._log('Filler.add_event', 'emitting: {0}'.format(emitted_event))
            self.emit(emitted_event)

            # remember previous event for padding/etc.
            self._previous_event = emitted_event

    def _interpolate_event_list(self, events):
        """
        The fundamental linear interpolation workhorse code.  Process
        a list of events and return a new list. Does a pass for
        every field_spec.

        This is abstracted out like this because we probably want
        to interpolate a list of events not tied to a Collection.
        A Pipeline result list, etc etc.
        """
        base_events = copy.copy(events)

        for i in self._field_spec:

            # new array of interpolated events for each field path
            new_events = list()

            # boolean to keep track if there are no longer any valid
            # events "forward" in the sequence for a given field_path.
            # if there are no more valid values, there is no reason
            # to keep seeking every time.
            seek_forward = True

            field_path = self._field_path_to_array(i)

            for event_enum in enumerate(base_events):

                # cant interpolate first or last event so just save it
                # as-is and move on.
                if event_enum[0] == 0 or event_enum[0] == len(base_events) - 1:
                    new_events.append(event_enum[1])
                    continue

                # found a bad value so start calculating.
                if not is_valid(event_enum[1].get(field_path)):

                    previous_value = None
                    next_value = None

                    # look to the previous event in the new_event list since
                    # that's where previously interpolated values will be.

                    previous_value = new_events[event_enum[0] - 1].get(field_path)

                    # see about finding the next valid value in the original
                    # list.

                    next_idx = event_enum[0] + 1

                    while next_value is None and next_idx < len(base_events):

                        # no more good values "forward" so don't bother.
                        if seek_forward is False:
                            break

                        val = base_events[next_idx].get(field_path)

                        if is_valid(val):
                            next_value = val  # terminates the loop

                        next_idx += 1

                    # previous_value should only be none if there are a string
                    # of bad values at the beginning of the sequence.
                    # next_value will be none if that value no longer has
                    # valid values in the rest of the sequence.

                    if previous_value is not None and next_value is not None:
                        # pry the data from current event
                        new_data = thaw(event_enum[1].data())
                        # average the two values
                        new_val = truediv((previous_value + next_value), 2)
                        # set that value to the field spec in new data
                        nested_set(new_data, field_path, new_val)
                        # call .set_data() to create a new event
                        new_events.append(event_enum[1].set_data(new_data))
                    else:
                        # couldn't calculate new value either way, just
                        # keep the old event.
                        new_events.append(event_enum[1])

                        if next_value is None:
                            # no more good values for this field spec in the
                            # sequence, so don't bother looking on subsequent
                            # events in this field_spec
                            seek_forward = False

                else:
                    new_events.append(event_enum[1])

            # save the current state before doing another pass
            # on a different field_path
            base_events = new_events

        return base_events

    def _interpolated_collection(self, coll):
        """
        Generate a new collection of interpolated values.
        """
        return Collection(self._interpolate_event_list(list(coll.events())))

    def _interpolate_collection_out(self, cout):
        """
        Handle linear method when CollectionOut is an observer.

        Massage the contents of the collections in the Collector before
        the flush() keeps moving up the food chain.
        """

        self._log('Filler._interpolate_collection_out')

        cols = cout._collector._collections  # pylint: disable=protected-access

        for v in list(cols.values()):
            v.collection = self._interpolated_collection(v.collection)

    def _interpolate_event_out(self, eout):
        """
        Handle linear method when EventOut is an observer.

        Massage results before flush() keeps moving up the food chain.
        """
        self._log('Filler._interpolate_event_out')

        # sorry pylint, it's just gotta be that way
        # pylint: disable=protected-access

        pip = eout._pipeline

        new_results = self._interpolate_event_list(pip._results)

        # flush and replace with filled results
        pip.clear_results()
        # .add_results() would be nicer but no reason to loop all
        # those method calls.
        pip._results = new_results

    def flush(self):
        """Don't delegate flush to superclass yet. Linear interpolation
        needs to happen after the events have been processed but before
        they are finally emitted."""
        self._log('Filler.flush')

        if self.has_observers() and self._method == 'linear':
            self._log('Filler.flush.linear')

            for i in self._observers:
                if isinstance(i, CollectionOut):
                    self._interpolate_collection_out(i)
                elif isinstance(i, EventOut):
                    self._interpolate_event_out(i)
                else:  # pragma: no cover
                    # this is just future proofing
                    msg = 'Unknown observer for linear interpolation: {0}'.format(i)
                    raise ProcessorException(msg)

        super(Filler, self).flush()


class Mapper(Processor):
    """
    A processor which takes an operator as its only option
    and uses that to either output a new event.

    Parameters
    ----------
    arg1 : Mapper or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the mapper"""

        super(Mapper, self).__init__(arg1, options)

        self._log('Mapper.init', 'uid: {0}'.format(self._id))

        self._op = None

        if isinstance(arg1, Mapper):
            self._op = arg1._op  # pylint: disable=protected-access
        elif is_pipeline(arg1):
            self._op = options.op
        else:
            msg = 'Unknown arg to Mapper: {0}'.format(arg1)
            raise ProcessorException(msg)

        if callable(self._op) is False:
            msg = 'op: {0} is not a callable function'.format(self._op)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Mapper(self)

    def add_event(self, event):
        """
        Perform the map operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            evn = self._op(event)
            self._log('Mapper.add_event', 'emitting: {0}'.format(evn))
            self.emit(evn)
