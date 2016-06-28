#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Objects to handle Pipeline I/O.
"""

from .bases import PypondBase
from .collection import Collection
from .exceptions import PipelineIOException
from .util import unique_id, Options, Capsule

#
# The collector
#


class Collector(PypondBase):
    """
    A Collector is used to accumulate events into multiple collections,
    based on potentially many strategies. In this current implementation
    a collection is partitioned based on the window that it falls in
    and the group it is part of.

    Collections are emitted from this class to the supplied onTrigger
    callback.

    Parameters
    ----------
    options : Options
        A pipeline options instance
    on_trigger : function
        Callback to handle the emitted collection
    """

    def __init__(self, options, on_trigger):
        super(Collector, self).__init__()
        # options
        self._group_by = options.group_by
        self._emit_on = options.emit_on
        self._window_type = options.window_type
        self._window_duration = options.window_duration

        # callback for trigger
        self._on_trigger = on_trigger

        # maintained collections
        self._collections = dict()


    def flush_collections(self):
        raise NotImplementedError

    def emit_collections(self, collection):
        raise NotImplementedError

    def add_event(self, event):

        ts = event.timestamp()

        # window_key
        window_key = None

        if self._window_type == 'fixed':
            pass
        elif self._window_type == 'daily':
            pass
        elif self._window_type == 'monthly':
            pass
        elif self._window_type == 'yearly':
            pass
        else:
            window_key = self._window_type

        # groupby key
        group_by_key = self._group_by(event)

        # collection key

        collection_key = '{wk}::{gbk}'.format(wk=window_key, gbk=group_by_key) if \
            group_by_key is not None else window_key

        discard = False

        if collection_key not in self._collections:
            self._collections[collection_key] = Capsule(
                window_key=window_key,
                group_by_key=group_by_key,
                collection=Collection(),
            )
            discard = True

        self._collections[collection_key].collection = \
            self._collections[collection_key].collection.add_event(event)

        # if fixed windows, collect together old collections that
        # will be discarded.

        discards = dict()

        if discard is True and self._window_type == 'fixed':
            for k, v in list(self._collections.items()):
                if v.window_key == window_key:
                    discards[k] = v

        # emit

        print(self._emit_on)

        if self._emit_on == 'each_event':
            self.emit_collections(self._collections)
        elif self._emit_on == 'discard':
            self.emit_collections(discards)
            for k in list(discards.keys()):
                self._collections.pop(k, None)
        elif self._emit_on == 'flush':
            pass
        else:
            msg = 'Unknown emit type supplied to Collector'
            raise PipelineIOException(msg)

#
# Output classes
#


class PipelineOut(PypondBase):  # pylint: disable=too-few-public-methods
    """
    Base class for pipeline output classes

    Parameters
    ----------
    pipeline : Pipeline
        The Pipeline
    """

    def __init__(self, pipeline):
        """
        Base class for pipeline output classes
        """
        super(PipelineOut, self).__init__()
        self._id = unique_id('out-')
        self._pipeline = pipeline


class CollectionOut(PipelineOut):

    def __init__(self, pipeline, options, callback):
        super(CollectionOut, self).__init__(pipeline)

        self._callback = callback
        self._collector = Collector(
            Options(
                window_type=pipeline.get_window_type(),
                window_duration=pipeline.get_window_duration(),
                group_by=pipeline.get_group_by(),
                emit_on=pipeline.get_emit_on(),
            ),
            self._collector_callback,
        )

    def _collector_callback(self, collection, window_key, group_by_key='all'):
        """
        This is the callback passed to the collector, normally done
        as an inline in the Javascript source.
        """
        group_by = group_by_key

        if self._callback is not None:
            self._callback(collection, window_key, group_by)
        else:
            keys = list()
            if window_key != 'global':
                keys.append(window_key)
            if group_by != 'all':
                keys.append(group_by)

            k = '--'.join(keys) if len(keys) > 0 else 'all'
            self._pipeline.add_result(k, collection)

    def add_event(self, event):
        self._collector.add_event(event)

    def on_emit(self, callback):
        self._callback = callback

    def flush(self):
        self._collector.flush_collections()
        if self._callback is None:
            self._pipeline.results_done()
