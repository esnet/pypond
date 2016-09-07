#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Collapse the columns and return a new event
"""

from .base import Processor
from ..exceptions import ProcessorException
from ..util import is_pipeline, Options


class Collapser(Processor):
    """
    A processor which takes a fieldSpec and returns a new event
    with a new column that is a collapsed result of the selected
    columns. To collapse the columns it uses the supplied reducer
    function. Optionally the new column can completely replace
    the existing columns in the event.

    Parameters
    ----------
    arg1 : Collapser or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        super(Collapser, self).__init__(arg1, options)

        if isinstance(arg1, Collapser):
            # pylint: disable=protected-access
            self._field_spec_list = arg1._field_spec_list
            self._name = arg1._name
            self._reducer = arg1._reducer
            self._append = arg1._append
        elif is_pipeline(arg1):
            self._field_spec_list = options.field_spec_list
            self._name = options.name
            self._reducer = options.reducer
            self._append = options.append
        else:
            msg = 'Unknown arg to Collapser: {0}'.format(arg1)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Collapser(self)

    def add_event(self, event):
        """
        Perform the collapse operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            evn = event.collapse(
                self._field_spec_list,
                self._name,
                self._reducer,
                self._append
            )

        self._log('Collapser.add_event', 'emitting: {0}', (evn,))
        self.emit(evn)
