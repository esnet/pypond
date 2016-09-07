#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Processor that adds events to collector given windowing and grouping options.
"""

from .base import Processor
from ..exceptions import ProcessorException
from ..indexed_event import IndexedEvent
from ..io.output import Collector
from ..timerange_event import TimeRangeEvent
from ..util import is_pipeline, Options


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

                if not isinstance(v, dict):
                    msg = 'Aggregator: field values must be a dict, got: {0}'.format(v)
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

        group_by_key is unused - this is because the collector supplies all of
        those args to any callback that is fed to it. That doesn't break thing
        in JS apparently.
        """

        self._log(
            'Aggregator._collector_callback',
            'coll:{0}, wkey: {1}, gbkey: {2}',
            (collection, window_key, group_by_key)
        )

        new_d = dict()

        for field_name, field_map in list(self._fields.items()):

            # field_list = [field_name] if isinstance(field_name, str) else list(field_name)

            # self._log(
            #     'Aggregator._collector_callback',
            #     'field_name: {0}, map: {1}'.format(field_name, field_map)
            # )

            if len(field_map) != 1:
                msg = 'Fields should contain exactly one field'
                raise ProcessorException(msg)

            field = list(field_map.keys())[0]
            func = field_map[field]

            new_d[field_name] = collection.aggregate(func, field)

        event = None

        self._log(
            'Aggregator._collector_callback',
            'new_d: {0}', (new_d,)
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
            'emitting: {0}', (event,)
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
            self._log('Aggregator.add_event', 'adding: {0}', (event,))
            self._collector.add_event(event)
