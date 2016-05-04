"""
Base classes and components of pipeline sources.
"""

from .event import Event, IndexedEvent, TimeRangeEvent
from .bases import Observable
from .exceptions import PipelineException
from .util import unique_id


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
        """verify the internal types."""

        # gotta do.it.this way to avoid circular import with event.py
        # I don't think these pipeline source bases need their own
        # module.

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
    pass
