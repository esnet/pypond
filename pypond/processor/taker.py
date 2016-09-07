#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Processor that limits the number of events that are processed.
"""

from .base import Processor
from ..exceptions import ProcessorException
from ..index import Index
from ..util import is_pipeline, Options


class Taker(Processor):
    """
    A processor which limits the number of events that are processed.

    Parameters
    ----------
    arg1 : Taker or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the mapper"""

        super(Taker, self).__init__(arg1, options)

        self._log('Taker.init', 'uid: {0}'.format(self._id))

        # options
        self._limit = None
        self._window_type = None
        self._window_duration = None
        self._group_by = None

        # instance memebers
        self._count = dict()
        self._flush_sent = False

        if isinstance(arg1, Taker):
            # pylint: disable=protected-access
            self._limit = arg1._limit
            self._window_type = arg1._window_type
            self._window_duration = arg1._window_duration
            self._group_by = arg1._group_by
        elif is_pipeline(arg1):
            self._limit = options.limit
            self._window_type = arg1.get_window_type()
            self._window_duration = arg1.get_window_duration()
            self._group_by = arg1.get_group_by()
        else:
            msg = 'Unknown arg to Taker: {0}'.format(arg1)
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Taker(self)

    def add_event(self, event):
        """
        Output an event that is offset.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():

            ts = event.timestamp()
            window_key = None

            if self._window_type == 'fixed':
                window_key = Index.get_index_string(self._window_duration, ts)
            else:
                window_key = self._window_type

            group_by_key = self._group_by(event)

            coll_key = '{wk}::{gbk}'.format(wk=window_key, gbk=group_by_key) if \
                group_by_key is not None else window_key

            if coll_key not in self._count:
                self._count[coll_key] = 0

            self._count[coll_key] += 1

            # emit the events for each collection key that has not reached
            # the limit. This is the main point of this processor.
            if self._count.get(coll_key) <= self._limit:
                self._log('Taker.add_event', 'collection key: {0}', (coll_key,))
                self._log(
                    'Taker.add_event',
                    'count: {0} limit: {1}',
                    (self._count.get(coll_key), self._limit)
                )
                self._log('Taker.add_event', 'emitting: {0}', (event,))
                self.emit(event)

    def flush(self):
        """flush"""
        super(Taker, self).flush()
