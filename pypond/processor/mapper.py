"""
Take and operator and perform map operations
"""

from .base import Processor
from ..exceptions import ProcessorException
from ..util import is_pipeline, Options


class Mapper(Processor):
    """
    A processor which takes an operator as its only option
    and uses that to either output a new event.

    Parameters
    ----------
    arg1 : Mapper or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the mapper"""

        super(Mapper, self).__init__(arg1, options)

        self._log('Mapper.init', 'uid: {0}'.format(self._id))

        self._op = None

        if isinstance(arg1, Mapper):
            self._op = arg1._op  # pylint: disable=protected-access
        elif is_pipeline(arg1):
            self._op = options.op
        else:
            msg = 'Unknown arg to Mapper: {0}'.format(arg1)
            raise ProcessorException(msg)

        if callable(self._op) is False:
            msg = 'op: {0} is not a callable function'.format(self._op)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Mapper(self)

    def add_event(self, event):
        """
        Perform the map operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():
            evn = self._op(event)
            self._log('Mapper.add_event', 'emitting: {0}', (evn,))
            self.emit(evn)
