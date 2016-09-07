#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Objects to handle Pipeline output and event collection.
"""

from collections import OrderedDict

from ..bases import PypondBase
from ..collection import Collection
from ..exceptions import PipelineIOException
from ..index import Index
from ..util import unique_id, Options, Capsule

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
        Callback to handle the emitted Collection
    """

    def __init__(self, options, on_trigger):
        super(Collector, self).__init__()

        # self._log('Collector.init', 'opt: {0} trigger: {1}'.format(options.to_dict(), on_trigger))

        # options
        self._group_by = options.group_by
        self._emit_on = options.emit_on
        self._window_type = options.window_type
        self._window_duration = options.window_duration
        self._utc = True

        # If the optional utc option is passed in by one of the processors
        # (generally the Aggregator), honor it.
        if options.utc is not None and isinstance(options.utc, bool):
            self._utc = options.utc

        # callback for trigger
        self._on_trigger = on_trigger

        # maintained collections
        self._collections = OrderedDict()

    def flush_collections(self):
        """Emit the remaining collections."""

        self._log('Collector.flush_collections')

        self.emit_collections(self._collections)

    def emit_collections(self, collections):
        """Emit all of the collections to the trigger callback that was
        passed in by the Processor

        Parameters
        ----------
        collections : dict
            A dict of string keys and Capsule objects containing the
            window_key, group_by_key and a Collection.
        """

        self._log('Collector.emit_collections')

        if self._on_trigger:
            for v in list(collections.values()):
                self._on_trigger(v.collection, v.window_key, v.group_by_key)

    def add_event(self, event):  # pylint: disable=too-many-branches
        """Add and event to the _collections dict and act accordingly
        depending on how _emit_on is set.

        Parameters
        ----------
        event : Event
            An event.

        Raises
        ------
        PipelineIOException
            Raised on bad args.
        """

        self._log('Collector.add_event', '{0} utc: {1}', (event, self._utc))

        # window_key
        window_key = None

        ts = event.timestamp()

        if self._window_type == 'fixed':
            # if fixed, always utc
            window_key = Index.get_index_string(self._window_duration, ts)
        elif self._window_type == 'daily':
            window_key = Index.get_daily_index_string(ts, utc=self._utc)
        elif self._window_type == 'monthly':
            window_key = Index.get_monthly_index_string(ts, utc=self._utc)
        elif self._window_type == 'yearly':
            window_key = Index.get_yearly_index_string(ts, utc=self._utc)
        else:
            window_key = self._window_type

        # groupby key
        group_by_key = self._group_by(event)

        # collection key

        collection_key = '{wk}::{gbk}'.format(wk=window_key, gbk=group_by_key) if \
            group_by_key is not None else window_key

        self._log('Collector.add_event', 'collection_key: {0}', (collection_key))

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

        # OK, so what is happening here is that when we are processing
        # fixed windows and self._emit_on == 'discards' the Collector
        # starts adding the events to the internal self._collections
        # structure and will continue doing so until we see the "next"
        # window key i.e.: the first point of the next day when the
        # duration is '1d'. At that point, all of the points from
        # the previous day are copied from self._collections into
        # the discards dict, the discards dict is emitted, and then
        # those values are pop()'ed out of self._collections.

        discards = OrderedDict()

        if discard is True and self._window_type == 'fixed':
            for k, v in list(self._collections.items()):
                if v.window_key != window_key:
                    discards[k] = v

        # emit

        self._log(
            'Collector.add_event',
            'emit_on: {0}, discard: {1} discards: {2}',
            (self._emit_on, discard, discards)
        )

        if self._emit_on == 'eachEvent':  # keeping mixedCase tokens for consistancy.
            self.emit_collections(self._collections)
        elif self._emit_on == 'discard':
            self.emit_collections(discards)
            for k in list(discards.keys()):
                self._collections.pop(k, None)
        elif self._emit_on == 'flush':
            # this is not an overlooked/unimplemented case.
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


class EventOut(PipelineOut):
    """Output object for when processor results are being returned
    as events.

    Parameters
    ----------
    pipeline : Pipeline
        A reference to the calling Pipeline instance.
    callback : function or None
        Will either be a function that the collector callback will
        pass things to or None which will pass the results back to
        the calling Pipeline.
    options : Options
        An Options object.
    """

    def __init__(self, pipeline, callback=None, options=Options()):
        """Output object for when processor results are being returned.
        """
        super(EventOut, self).__init__(pipeline)

        self._log('EventOut.init')

        self._callback = callback
        self._options = options

    def add_event(self, event):
        """Add an event to the pipeline or callback.

        Parameters
        ----------
        event : Event
            An event object
        """
        if self._callback is not None:
            self._callback(event)
        else:
            self._pipeline.add_result(event)

    def flush(self):
        """Mark the results_done = True in the pipeline if there is no longer
        an observer.
        """
        if self._callback is None:
            self._pipeline.results_done()

    def on_emit(self, callback):
        """Sets the internal callback.

        Parameters
        ----------
        callback : function or None
            Value to set the intenal _callback to.
        """
        self._callback = callback


class CollectionOut(PipelineOut):
    """Output object for when processor results are being returned
    as a collection.

    Parameters
    ----------
    pipeline : Pipeline
        A reference to the calling Pipeline instance.
    callback : function or None
        Will either be a function that the collector callback will
        pass things to or None which will pass the results back to
        the calling Pipeline.
    options : Options
        An Options object.
    """

    def __init__(self, pipeline, callback, options):
        """Output object for when processor results are being returned.
        """
        super(CollectionOut, self).__init__(pipeline)

        self._log('CollectionOut.init')

        self._callback = callback
        self._options = options

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

        self._log(
            'CollectionOut._collector_callback',
            'coll:{0}, wkey: {1}, gbkey: {2} cback: {3}',
            (collection, window_key, group_by_key, self._callback)
        )

        group_by = group_by_key

        if self._callback is not None:
            self._callback(collection, window_key, group_by)
        else:
            keys = list()
            if window_key != 'global':
                keys.append(window_key)
            if group_by != 'all' and group_by is not None:
                keys.append(group_by)

            k = '--'.join(keys) if len(keys) > 0 else 'all'
            self._pipeline.add_result(k, collection)

    def add_event(self, event):
        """Add an event to the collector.

        Parameters
        ----------
        event : Event
            An event object
        """
        self._collector.add_event(event)

    def on_emit(self, callback):
        """Sets the internal callback.

        Parameters
        ----------
        callback : function or None
            Value to set the intenal _callback to.
        """
        self._callback = callback

    def flush(self):
        """Flush the collector and mark the results_done = True in the
        pipeline if there is no longer an observer.
        """

        self._log('CollectionOut.flush')

        self._collector.flush_collections()
        if self._callback is None:
            self._pipeline.results_done()
