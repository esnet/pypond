#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Simple processor generate the Rate of two Event objects and
emit them as a TimeRangeEvent. Can be used alone or chained
with the Align processor for snmp rates, etc.
"""

import copy
import numbers
from operator import truediv

import six

from .base import Processor
from ..exceptions import ProcessorException, ProcessorWarning
from ..indexed_event import IndexedEvent
from ..timerange_event import TimeRangeEvent
from ..util import is_pipeline, Options, ms_from_dt, nested_set


class Rate(Processor):
    """Generate rate from two events.

    Parameters
    ----------
    arg1 : Rate or Pipeline
        Pipeline or copy constructor
    options : Options, optional
        Pipeline Options object.

    Raises
    ------
    ProcessorException
        Raised on bad arg types.
    """

    def __init__(self, arg1, options=Options()):
        """create Rate"""

        super(Rate, self).__init__(arg1, options)

        self._log('Rate.init', 'uid: {0}'.format(self._id))

        # options
        self._field_spec = None
        self._allow_negative = None

        # instance attrs
        self._previous = None

        if isinstance(arg1, Rate):
            # Copy constructor
            # pylint: disable=protected-access
            self._field_spec = arg1._field_spec
            self._allow_negative = arg1._allow_negative
        elif is_pipeline(arg1):
            self._field_spec = options.field_spec
            self._allow_negative = options.allow_negative
        else:
            msg = 'Unknown arg to Rate constructor: {a}'.format(a=arg1)
            raise ProcessorException(msg)

        # work out field specs
        if isinstance(self._field_spec, six.string_types):
            self._field_spec = [self._field_spec]
        elif self._field_spec is None:
            self._field_spec = ['value']

    def clone(self):
        """Clone this Rate processor.

        Returns
        -------
        Rate
            Cloned Rate object.
        """
        return Rate(self)

    def _get_rate(self, event):
        """
        Generate a new TimeRangeEvent containing the rate in seconds
        from two events.
        """

        new_data = dict()

        previous_ts = ms_from_dt(self._previous.timestamp())
        current_ts = ms_from_dt(event.timestamp())

        ts_delta = truediv(current_ts - previous_ts, 1000)  # do it in seconds

        for i in self._field_spec:

            field_path = self._field_path_to_array(i)
            rate_path = copy.copy(field_path)
            rate_path[-1] += '_rate'

            previous_val = self._previous.get(field_path)
            current_val = event.get(i)

            if not isinstance(previous_val, numbers.Number) or \
                    not isinstance(current_val, numbers.Number):
                msg = 'Path {0} contains non-numeric values or does not exist - '
                msg += 'value will be set to None'

                self._warn(msg, ProcessorWarning)

                nested_set(new_data, rate_path, None)
                continue

            rate = truediv((current_val - previous_val), ts_delta)

            if self._allow_negative is False and rate < 0:
                # don't allow negative differentials in certain cases
                nested_set(new_data, rate_path, None)
            else:
                nested_set(new_data, rate_path, rate)

        return TimeRangeEvent([previous_ts, current_ts], new_data)

    def add_event(self, event):
        """
        Output an even that is Rate by a certain value.

        Parameters
        ----------
        event : Event
            An Event.
        """

        self._log('Rate.add_event', '{0}', (event,))

        if isinstance(event, (TimeRangeEvent, IndexedEvent)):
            msg = 'Expecting Event object input.'
            raise ProcessorException(msg)

        if self.has_observers():

            if self._previous is None:
                # takes two to tango
                self._previous = event
                return

            output_event = self._get_rate(event)

            self._log('Rate.add_event', 'emitting: {0}', (output_event,))

            self.emit(output_event)

            self._previous = event
