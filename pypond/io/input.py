#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Classes to handle pipeline input.
"""

from ..event import Event
from ..bases import Observable
from ..exceptions import PipelineIOException
from ..indexed_event import IndexedEvent
from ..timerange_event import TimeRangeEvent
from ..util import unique_id


class PipelineIn(Observable):
    """
    For the pipeline - raise exceptions if an attempt is made to
    add heterogenous types.
    """

    def __init__(self):

        super(PipelineIn, self).__init__()

        self._id = unique_id('in-')
        self._type = None

    def _check(self, event):
        """verify the internal types.

        Parameters
        ----------
        event : Event
            An Event

        Returns
        -------
        no return value
            Sets internal value in subclass or raises exception

        Raises
        ------
        PipelineIOException
            Raised if events are not all of one type.
        """

        if self._type is None:
            if isinstance(event, Event):
                self._type = Event
            elif isinstance(event, TimeRangeEvent):
                self._type = TimeRangeEvent
            elif isinstance(event, IndexedEvent):
                self._type = IndexedEvent
        else:
            if not isinstance(event, self._type):
                raise PipelineIOException('Homogeneous events expected')


class Bounded(PipelineIn):
    """For the pipeline - source of a fixed size - like a collection."""

    def __init__(self):
        super(Bounded, self).__init__()

    # pylint: disable=no-self-use, missing-docstring

    def start(self):
        raise PipelineIOException('start() not supported on bounded source')

    def stop(self):
        raise PipelineIOException('stop() not supported on bounded source')

    def on_emit(self):
        raise PipelineIOException('You can not setup a listener to a bounded source')


class Stream(PipelineIn):
    """For the pipeline - a source that has no container of its own."""

    def __init__(self):
        super(Stream, self).__init__()
        self._running = True

    def start(self):
        """start"""
        self._running = True

    def stop(self):
        """stop"""
        self._running = False
        self.flush()  # emit a flush to let processors cleanly exit.

    def add_event(self, event):
        """Type check and event and emit it if we are running have have observers.

        Parameters
        ----------
        event : Event
            Some Event class
        """
        self._check(event)
        if self.has_observers() is True and self._running is True:
            self.emit(event)

    def events(self):  # pylint: disable=no-self-use
        """Raise an exception - can't iterate an unbounded source."""
        msg = 'Iteration across unbounded sources is not suported.'
        raise PipelineIOException(msg)
