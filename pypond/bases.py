"""
Common base classes and mixins.
"""

import warnings

import pypond.event  # avoiding circular imports

from .exceptions import PipelineException
from .util import unique_id


class PypondBase(object):
    """
    Universal base class. Used to provide common functionality (logging, etc)
    to all the other classes.
    """
    def __init__(self):
        """ctor"""

        self._log = None

    def _warn(self, msg, warn_type):  # pylint: disable=no-self-use
        """issue warning"""
        warnings.warn(msg, warn_type, stacklevel=2)


# base classes for pipeline sources, etc


class Observable(PypondBase):
    """
     Base class for objects in the processing chain which
     need other object to listen to them. It provides a basic
     interface to define the relationships and to emit events
     to the interested observers.
    """
    def __init__(self):
        super(Observable, self).__init__()

        self._observers = list()

    def emit(self, event):
        """add event to observers."""
        for i in self._observers:
            i.addEvent(event)

    def flush(self):
        """flush observers."""
        for i in self._observers:
            if issubclass(i, Observable):
                i.flush()

    def add_observer(self, observer):
        """add an observer if it does not already exist."""
        should_add = True

        for i in self._observers:
            if i == observer:
                should_add = False

        if should_add:
            self._observers.append(observer)

    def has_observers(self):
        """does the object have observers?"""
        return bool(len(self._observers) > 0)


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
            if isinstance(event, pypond.event.Event):
                self._type = pypond.event.Event
            elif isinstance(event, pypond.event.TimeRangeEvent):
                self._type = pypond.event.TimeRangeEvent
            elif isinstance(event, pypond.event.IndexedEvent):
                self._type = pypond.event.IndexedEvent
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
