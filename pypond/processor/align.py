#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Simple processor to change the event values by a certain offset.

Primarily for testing.
"""

import numbers
from operator import truediv

import six

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

        # self._log('Align.init.Options', options)

        # work out field specs
        if isinstance(self._field_spec, six.string_types):
            self._field_spec = [self._field_spec]
        elif self._field_spec is None:
            self._field_spec = ['value']

        # check input
        if self._method not in ('linear', 'hold',):
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

    def _get_boundary_ms(self, boundary_index):  # pylint: disable=no-self-use
        """
        Return the index string as ms.

        We are dealing in UTC only with the Index because the events
        all have internal timestamps in UTC and that's what we're
        aligning. Let the user display in local time if that's
        what they want.
        """
        idx = Index(boundary_index)
        return ms_from_dt(idx.begin())

    def _is_aligned(self, event):
        """
        Test to see if an event is perfectly aligned. Used on first event.
        """
        bound = Index.get_index_string(self._window, event.timestamp())
        return bool(self._get_boundary_ms(bound) == ms_from_dt(event.timestamp()))

    def _interpolate_hold(self, boundary, set_none=False):
        """
        Generate a new event on the requested boundary and carry over the
        value from the previous event.

        A variation just sets the values to None - this is used when the
        limit is hit.
        """
        new_data = dict()

        boundary_ts = self._get_boundary_ms(boundary)

        for i in self._field_spec:

            field_path = self._field_path_to_array(i)

            if set_none is False:
                nested_set(new_data, field_path, self._previous.get(field_path))
            else:
                nested_set(new_data, field_path, None)

        return Event(boundary_ts, new_data)

    def _interpolate_linear(self, boundary, event):
        """
        Generate a linear differential between two counter values that lie
        on either side of a window boundary.
        """

        new_data = dict()

        previous_ts = ms_from_dt(self._previous.timestamp())
        boundary_ts = self._get_boundary_ms(boundary)
        current_ts = ms_from_dt(event.timestamp())

        # this ratio will be the same for all values being processed
        boundary_frac = truediv((boundary_ts - previous_ts), (current_ts - previous_ts))

        for i in self._field_spec:

            field_path = self._field_path_to_array(i)

            # generate the delta between the values and
            # bulletproof against non-numeric/bad path

            previous_val = self._previous.get(field_path)
            current_val = event.get(i)

            if not isinstance(previous_val, numbers.Number) or \
                    not isinstance(current_val, numbers.Number):
                msg = 'Path {0} contains non-numeric values or does not exist - '.format(i)
                msg += 'field: {0} will be set to None'.format(i)

                self._warn(msg, ProcessorWarning)

                nested_set(new_data, field_path, None)
                continue

            # just being clear with that irrelevant outer set of grouping parens
            differential = previous_val + ((current_val - previous_val) * boundary_frac)

            nested_set(new_data, field_path, differential)

        return Event(boundary_ts, new_data)

    def add_event(self, event):
        """
        Output an even that is Align by a certain value.

        Parameters
        ----------
        event : Event
            An Event.
        """

        self._log('Align.add_event', 'incoming: {0}', (event,))

        if isinstance(event, (TimeRangeEvent, IndexedEvent)):
            msg = 'TimeRangeEvent and IndexedEvent series can not be aligned.'
            raise ProcessorException(msg)

        if self.has_observers():

            # first event handling
            if self._previous is None:
                self._previous = event
                # If perfectly aligned, emit or it will get lost.
                if self._is_aligned(event):
                    self.emit(event)
                return

            boundaries = self._get_interpolation_boundaries(event)
            fill_count = len(boundaries)

            for bound in boundaries:
                # if the returned list is not empty, interpolate an event
                # on each of the boundaries and emit them.
                self._log('Align.add_event', 'boundary: {0}', (bound,))

                if self._limit is not None and fill_count > self._limit:
                    # check to see if we have hit the limit first, if so
                    # this span of boundaries with None in the field spec
                    ievent = self._interpolate_hold(bound, set_none=True)
                else:
                    # otherwise, interpolate new points
                    if self._method == 'linear':
                        ievent = self._interpolate_linear(bound, event)
                    elif self._method == 'hold':
                        ievent = self._interpolate_hold(bound)

                self._log('Align.add_event', 'emitting: {0}', (ievent,))
                self.emit(ievent)

            # one way or another, the current event will now become previous
            # because either:
            # 1) the events were in the same window and nothing was emitted; or
            # 2) any necessary events were emitted in the previous loop
            self._previous = event
