#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Base classes and components of pipeline sources and processors.
"""

from .event import Event
from .bases import Observable
from .exceptions import PipelineException
from .indexed_event import IndexedEvent
from .timerange_event import TimeRangeEvent
from .util import unique_id, is_pipeline


class In(Observable):
    """
    For the pipeline - raise exceptions if an attempt is made to
    add heterogenous types.
    """

    def __init__(self):

        super(In, self).__init__()

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
        PipelineException
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
                raise PipelineException('Homogeneous events expected')


class BoundedIn(In):
    """For the pipeline - source of a fixed size - like a collection."""

    def __init__(self):
        super(BoundedIn, self).__init__()

    # pylint: disable=no-self-use, missing-docstring

    def start(self):
        raise PipelineException('start() not supported on bounded source')

    def stop(self):
        raise PipelineException('stop() not supported on bounded source')

    def on_emit(self):
        raise PipelineException('You can not setup a listener to a bounded source')


class UnboundedIn(In):
    """For the pipeline - a source that has no container of its own."""

    def __init__(self):
        super(UnboundedIn, self).__init__()
        self._running = True

    def start(self):
        """start"""
        self._running = True

    def stop(self):
        """stop"""
        self._running = False

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
        raise PipelineException(msg)


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
