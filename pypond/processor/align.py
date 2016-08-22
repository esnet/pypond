"""
Simple processor to change the event values by a certain offset.

Primarily for testing.
"""

from operator import truediv
import re

import six

from pyrsistent import thaw

from .base import Processor
from ..event import Event
from ..exceptions import ProcessorException
from ..index import Index
from ..range import TimeRange
from ..util import is_pipeline, Options, ms_from_dt, nested_set


class Align(Processor):
    """A processor to align the data into bins of regular time period.

    Parameters
    ----------
    arg1 : Align or Pipeline
        Pipeline or copy constructor
    options : Options, optional
        Pipeline Options object.

    Raises
    ------
    ProcessorException
        Raised on bad arg types.
    """

    def __init__(self, arg1, options=Options()):
        """create the aligner."""

        super(Align, self).__init__(arg1, options)

        self._log('Align.init', 'uid: {0}'.format(self._id))

        # options
        self._window = None
        self._limit = None
        self._field_spec = None

        # instance attrs
        self._previous = None
        self._current = None
        self._window_size_key = None

        if isinstance(arg1, Align):
            # Copy constructor
            # pylint: disable=protected-access
            self._window = arg1._window
            self._limit = arg1._limit
            self._field_spec = arg1._field_spec
        elif is_pipeline(arg1):
            self._window = options.window
            self._limit = options.limit
            self._field_spec = options.field_spec
        else:
            msg = 'Unknown arg to Align constructor: {a}'.format(a=arg1)
            raise ProcessorException(msg)

        self._log('Align.init.Options', options)

        # work out field specs
        if isinstance(self._field_spec, six.string_types):
            self._field_spec = [self._field_spec]
        elif self._field_spec is None:
            self._field_spec = ['value']

        # extract the window size key
        range_re = re.match('([0-9]+)([smhd])', self._window)
        if range_re is not None:
            self._window_size_key = range_re.group(2)
        else:
            msg = 'could not extract s/m/h/d window key from window: {0}'.format(self._window)
            raise ProcessorException(msg)

    def clone(self):
        """Clone this Align processor.

        Returns
        -------
        Align
            Cloned Align object.
        """
        return Align(self)

    def _get_interpolation_boundaries(self, event):
        """
        Return a list of indexes of window boundaries if the current event
        and the previous event do not lie in the same window. If in the same,
        return an empty list.
        """

        if Index.get_index_string(self._window, self._previous.timestamp()) != \
                Index.get_index_string(self._window, event.timestamp()):
            # generate a list of indexes to lay new points on. skip the first
            # one because the previous point is in an "old" window, interpolate
            # the point at the beginning of the rest of the ones in the list.
            trange = TimeRange(self._previous.timestamp(), event.timestamp())
            return Index.get_index_string_list(self._window, trange)[1:]
        else:
            return list()

    def _interpolate_event(self, boundary, event):
        """
        Given the current event and a boundary edge to drop place a new
        event on, construct a new event.

        Implementing:

        y = mx + b (but b is zero so _previous is the origin)
        """
        print('_interpolate_event')
        new_data = thaw(event.data())

        # XXX: Fuck with UTC here?
        idx = Index(boundary)

        previous_ts = ms_from_dt(self._previous.timestamp())
        boundary_ts = ms_from_dt(idx.begin())
        current_ts = ms_from_dt(event.timestamp())

        for i in self._field_spec:
            # calculate "m" which is delta y / delta x
            # delta_y is the difference between values
            # delta_x is the proportion of a single window between the two values

            field_path = self._field_path_to_array(i)

            # difference in values
            value_delta = event.get(i) - self._previous.get(field_path)

            # difference in time
            time_delta = current_ts - previous_ts

            m_value = truediv(value_delta, time_delta)
            # print('m:', m_value)

            # calculate delta_x3 between ms timestamps
            delta_x3 = boundary_ts - previous_ts
            # print('x3:', delta_x3)

            # calculate delta_y3 between values
            delta_y3 = m_value * delta_x3
            # print('y3:', delta_y3)

            # final points
            x_final, y_final = (
                (previous_ts + delta_x3), (self._previous.get(field_path) + delta_y3))
            # print('finals:', x_final, y_final)

            # the x_final value should be the exact same as the boundary_ts
            # we already know, sanity check it.
            if x_final != boundary_ts:
                msg = 'interpolation error x_final: {0} != boundary_ts: {1}'.format(
                    x_final, boundary_ts)
                raise ProcessorException(msg)

            # alright, lets set the value
            nested_set(new_data, field_path, y_final)

        # XXX: revisit this for different Event types
        return Event(boundary_ts, new_data)

    def add_event(self, event):
        """
        Output an even that is Align by a certain value.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """

        self._log('Align.add_event', event)

        if self.has_observers():

            if self._previous is None:
                self._previous = event
                # takes two to tango so...
                return

            for bound in self._get_interpolation_boundaries(event):
                # if the returned list is not empty, interpolate an event
                # on each of the boundaries and emit them.
                self._log('Align.add_event', 'boundary: {0}'.format(bound))
                ievent = self._interpolate_event(bound, event)
                self._log('Align.add_event', 'emitting: {0}'.format(ievent))
                self.emit(ievent)

            # one way or another, the current event will now become previous
            # because either:
            # 1) the events were in the same window; or
            # 2) any necessary events were emitted in the previous loop
            self._previous = event


