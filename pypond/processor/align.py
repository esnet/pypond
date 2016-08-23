"""
Simple processor to change the event values by a certain offset.

Primarily for testing.
"""

import numbers
from operator import truediv

import six

from pyrsistent import thaw

from .base import Processor
from ..event import Event
from ..exceptions import ProcessorException, ProcessorWarning
from ..index import Index
from ..indexed_event import IndexedEvent
from ..range import TimeRange
from ..timerange_event import TimeRangeEvent
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
        self._method = None

        # instance attrs
        self._previous = None

        if isinstance(arg1, Align):
            # Copy constructor
            # pylint: disable=protected-access
            self._window = arg1._window
            self._limit = arg1._limit
            self._field_spec = arg1._field_spec
            self._method = arg1._method
        elif is_pipeline(arg1):
            self._window = options.window
            self._limit = options.limit
            self._field_spec = options.field_spec
            self._method = options.method
        else:
            msg = 'Unknown arg to Align constructor: {a}'.format(a=arg1)
            raise ProcessorException(msg)

        self._log('Align.init.Options', options)

        # work out field specs
        if isinstance(self._field_spec, six.string_types):
            self._field_spec = [self._field_spec]
        elif self._field_spec is None:
            self._field_spec = ['value']

        # check input
        if self._method not in ('linear', 'hold'):
            msg = 'Unknown method {0}'.format(self._method)
            raise ProcessorException(msg)

        if self._limit is not None and not isinstance(self._limit, int):
            msg = 'limit arg must be None or an integer'
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

    def _interpolate_hold(self, boundary, event, set_none=False):
        """
        Generate a new event on the requested boundary and carry over the
        value from the previous event.

        A variation just sets the values to None - this is used when the
        limit is hit.
        """
        new_data = thaw(event.data())

        idx = Index(boundary)

        boundary_ts = ms_from_dt(idx.begin())

        for i in self._field_spec:

            field_path = self._field_path_to_array(i)

            if set_none is False:
                nested_set(new_data, field_path, self._previous.get(field_path))
            else:
                nested_set(new_data, field_path, None)

        return Event(boundary_ts, new_data)

    def _interpolate_linear(self, boundary, event):  # pylint: disable=too-many-locals
        """
        Given the current event and a boundary edge between that and the
        previous event, generate a new event to place on that boundary.

        This implements the equation of a straight line to determine the
        value(s) of the interpolated event that is being aligned to arg
        boundary:

            y = mx + b

        Where b is zero because the trailing point is the origin. See:
        https://www.mathsisfun.com/equation_of_line.html

        Yes pylint, I used a lot of local variables instead of writing one huge
        nested parenthetical equation from hell that would be a PITA for
        someone else to look at.
        """

        new_data = thaw(event.data())

        # We are dealing in UTC only with the Index because the events
        # all have internal timestamps in UTC and that's what we're
        # aligning. Let the user display in local time if that's
        # what they want.
        idx = Index(boundary)

        previous_ts = ms_from_dt(self._previous.timestamp())
        boundary_ts = ms_from_dt(idx.begin())
        current_ts = ms_from_dt(event.timestamp())

        for i in self._field_spec:
            # calculate "m" (slope) which is delta y / delta x
            # delta_y is the difference between values
            # delta_x is the proportion of a single window between the two values

            field_path = self._field_path_to_array(i)

            # generate the delta between the values and
            # bulletproof against non-numeric/bad path

            val1 = self._previous.get(field_path)
            val2 = event.get(i)

            if not isinstance(val1, numbers.Number) or \
                    not isinstance(val2, numbers.Number):
                msg = 'Path {0} contains non-numeric values or does not exist - '
                msg += 'value will be set to None'

                self._warn(msg, ProcessorWarning)

                nested_set(new_data, field_path, None)
                continue

            # good values, calculate the delta and move on
            value_delta = val2 - val1

            # difference in time
            time_delta = current_ts - previous_ts

            slope = truediv(value_delta, time_delta)

            # calculate delta_x3 between ms timestamps
            delta_x3 = boundary_ts - previous_ts

            # calculate delta_y3 between values
            delta_y3 = slope * delta_x3

            # final points
            x_final, y_final = (
                (previous_ts + delta_x3), (self._previous.get(field_path) + delta_y3))

            # the x_final value should be the exact same as the boundary_ts
            # we already know, sanity check it.
            if x_final != boundary_ts:
                msg = 'interpolation error x_final: {0} != boundary_ts: {1}'.format(
                    x_final, boundary_ts)
                raise ProcessorException(msg)

            # alright, lets set the value
            nested_set(new_data, field_path, y_final)

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

        if isinstance(event, (TimeRangeEvent, IndexedEvent)):
            msg = 'TimeRangeEvent and IndexedEvent series can not be aligned.'
            raise ProcessorException(msg)

        if self.has_observers():

            if self._previous is None:
                self._previous = event
                # takes two to tango so...
                return

            boundaries = self._get_interpolation_boundaries(event)
            fill_count = len(boundaries)

            for bound in boundaries:
                # if the returned list is not empty, interpolate an event
                # on each of the boundaries and emit them.
                self._log('Align.add_event', 'boundary: {0}'.format(bound))

                # check to see if we have hit the limit first, if so
                # fill with None

                if self._limit is not None and \
                        fill_count > self._limit:
                    ievent = self._interpolate_hold(bound, event, set_none=True)
                else:
                    # otherwise, fill
                    if self._method == 'linear':
                        ievent = self._interpolate_linear(bound, event)
                    elif self._method == 'hold':
                        ievent = self._interpolate_hold(bound, event)

                self._log('Align.add_event', 'emitting: {0}'.format(ievent))
                self.emit(ievent)

            # one way or another, the current event will now become previous
            # because either:
            # 1) the events were in the same window and nothing was emitted; or
            # 2) any necessary events were emitted in the previous loop
            self._previous = event
