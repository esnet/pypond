#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Processor that takes an operator to control event flow.
"""

from .base import Processor
from ..exceptions import ProcessorException
from ..util import is_pipeline, Options


class Filter(Processor):
    """
    A processor which takes an operator as its only option
    and uses that to either output a new event.

    Parameters
    ----------
    arg1 : Filter or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the mapper"""

        super(Filter, self).__init__(arg1, options)

        self._log('Filter.init', 'uid: {0}'.format(self._id))

        self._op = None

        if isinstance(arg1, Filter):
            self._op = arg1._op  # pylint: disable=protected-access
        elif is_pipeline(arg1):
            self._op = options.op
        else:
            msg = 'Unknown arg to Filter: {0}'.format(arg1)
            raise ProcessorException(msg)

        if callable(self._op) is False:
            msg = 'op: {0} is not a callable function'.format(self._op)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Filter(self)

    def add_event(self, event):
        """
        Perform the filter operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            if self._op(event):
                self._log('Filter.add_event', 'emitting: {0}', (event,))
                self.emit(event)
