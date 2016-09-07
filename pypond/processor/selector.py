#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Processor to return events with only selected columns
"""

from .base import Processor
from ..event import Event
from ..exceptions import ProcessorException
from ..util import is_pipeline, Options


class Selector(Processor):
    """
    A processor which takes a fieldSpec as its only argument
    and returns a new event with only the selected columns

    Parameters
    ----------
    arg1 : Selector or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        super(Selector, self).__init__(arg1, options)

        self._log('Selector.init', 'uid: {0}'.format(self._id))

        self._field_spec = None

        if isinstance(arg1, Selector):
            self._field_spec = arg1._field_spec  # pylint: disable=protected-access
        elif is_pipeline(arg1):
            self._field_spec = options.field_spec
        else:
            msg = 'Unknown arg to Selector: {0}'.format(arg1)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Selector(self)

    def add_event(self, event):
        """
        Perform the select operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            evn = Event.selector(event, self._field_spec)
            self._log('Selector.add_event', 'emitting: {0}', (evn,))
            self.emit(evn)
