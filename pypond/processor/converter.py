#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Convert an event into another event type.
"""

from .base import Processor
from ..event import Event
from ..exceptions import ProcessorException
from ..index import Index
from ..indexed_event import IndexedEvent
from ..range import TimeRange
from ..timerange_event import TimeRangeEvent
from ..util import is_pipeline, Options, ms_from_dt, dt_from_ms


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

        # self._log('Converter.init', 'options: {0}'.format(options))

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

            # self._log('Converter.convert_event', 'range: {0}'.format(range_list))

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

            self._log('Converter.add_event', 'emitting: {0}', (output_event,))

            self.emit(output_event)
