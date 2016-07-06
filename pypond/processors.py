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

from .bases import Observable
from .event import Event
from .exceptions import ProcessorException
from .index import Index
from .pipeline_io import Collector
from .util import Options, is_pipeline, unique_id

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
            self._field_spec = arg1._field_spec
            self._name = arg1._name
            self._reducer = arg1._reducer
            self._append = arg1._append
        elif is_pipeline(arg1):
            self._field_spec = options.field_spec
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
                self._field_spec,
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

        if isinstance(arg1, Aggregator):
            # pylint: disable=protected-access
            self._fields = arg1._fields
            self._window_type = arg1._window_type
            self._window_duration = arg1._window_duration
            self._group_by = arg1._group_by
            self._emit_on = arg1._emit_on

        elif is_pipeline(arg1):

            pipeline = arg1

            self._window_type = pipeline.get_window_type()
            self._window_duration = pipeline.get_window_duration()
            self._group_by = pipeline.get_group_by()
            self._emit_on = pipeline.get_emit_on()

            # yes it does have a fields member pylint, it's just magic
            # pylint: disable=no-member

            if Options.fields is None:
                msg = 'Aggregator: constructor needs an aggregator field mapping'
                raise ProcessorException(msg)

            if not isinstance(Options.fields, dict):
                msg = 'Options.fields must be iterable'
                raise ProcessorException(msg)

            for i in list(Options.fields.keys()):
                if not isinstance(i, str) and not isinstance(i, tuple):
                    msg = 'Aggregator: field of unknown type: {0}'.format(i)
                    raise ProcessorException(msg)

            if pipeline.mode() == 'stream':
                if pipeline.get_window_type() is None \
                        or pipeline.get_window_duration() is None:
                    msg = 'Unable to aggregate/no windowing strategy specified in pipeline'
                    raise ProcessorException(msg)

            self._fields = Options.fields

        else:
            msg = 'Unknown arg to Aggregator: {0}'.format(arg1)
            raise ProcessorException(msg)

        self._collector = Collector(
            Options(
                window_type=self._window_type,
                window_duration=self._window_duration,
                group_by=self._group_by,
                emit_on=self._emit_on,
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
