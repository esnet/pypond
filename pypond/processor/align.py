"""
Simple processor to change the event values by a certain offset.

Primarily for testing.
"""

from .base import Processor
from ..exceptions import ProcessorException
from ..util import is_pipeline, Options


class Align(Processor):
    """A processor to align the data into bins of regular time periods.

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

        self._field_spec = None

        if isinstance(arg1, Align):
            # Copy constructor
            # pylint: disable=protected-access
            self._field_spec = arg1._field_spec
        elif is_pipeline(arg1):
            self._field_spec = options.field_spec
        else:
            msg = 'Unknown arg to Align constructor: {a}'.format(a=arg1)
            raise ProcessorException(msg)

    def clone(self):
        """Clone this Align processor.

        Returns
        -------
        Align
            Cloned Align object.
        """
        return Align(self)

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

            output_event = event

            self._log('Align.add_event', 'emitting: {0}'.format(output_event))

            self.emit(output_event)
