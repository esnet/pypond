#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
A processor to fill missing and invalid values.
"""

import copy
import numbers
from operator import truediv

from pyrsistent import thaw

import six

from .base import Processor
from ..exceptions import ProcessorException, ProcessorWarning
from ..util import (
    is_pipeline,
    is_valid,
    ms_from_dt,
    nested_get,
    nested_set,
    Options,
)


class Filler(Processor):  # pylint: disable=too-many-instance-attributes
    """
    A processor that fills missing/invalid values in the event
    with new values (zero, interpolated or padded).

    When doing a linear fill, Filler instances should be chained.
    See the Fill/sanitize doc (sanitize.md) for details.

    If no field_spec is supplied, the default field 'value' will be used.

    Parameters
    ----------
    arg1 : Filler or Pipeline
        Copy constructor or the pipeline.
    options : Options
        Options object.
    """

    def __init__(self, arg1, options=Options()):
        """create the mapper"""

        super(Filler, self).__init__(arg1, options)

        self._log('Filler.init', 'uid: {0}'.format(self._id))

        # options
        self._field_spec = None
        self._method = None
        self._mode = None
        self._fill_limit = None

        # internal members
        # state for pad to refer to previous event
        self._previous_event = None
        # key count for zero and pad fill
        self._key_count = dict()
        # special state for linear fill
        self._last_good_linear = None
        # cache of events pending linear fill
        self._linear_fill_cache = list()

        if isinstance(arg1, Filler):
            # pylint: disable=protected-access
            self._field_spec = arg1._field_spec
            self._method = arg1._method
            self._mode = arg1._mode
            self._fill_limit = arg1._fill_limit
        elif is_pipeline(arg1):
            self._field_spec = options.field_spec
            self._method = options.method
            self._mode = arg1.mode()
            self._fill_limit = options.fill_limit
        else:
            msg = 'Unknown arg to Filler: {0}'.format(arg1)
            raise ProcessorException(msg)

        self._log('Filler.init.Options', '{0}', (options,))

        if self._method not in ('zero', 'pad', 'linear'):
            msg = 'Unknown method {0} passed to Filler'.format(self._method)
            raise ProcessorException(msg)

        if self._fill_limit is not None and not isinstance(self._fill_limit, int):
            msg = 'Arg fill_limit must be an integer'
            raise ProcessorException(msg)

        if isinstance(self._field_spec, six.string_types):
            self._field_spec = [self._field_spec]
        elif self._field_spec is None:
            self._field_spec = ['value']

        # when using linear mode, only a single column will be processed
        # per instance. more details in sanitize.md
        if self._method == 'linear' and len(self._field_spec) != 1:
            msg = 'linear fill takes a path to a single column\n'
            msg += ' - see the sanitize documentation for usage details.'
            raise ProcessorException(msg)

    def clone(self):
        """clone it."""
        return Filler(self)

    def _pad_and_zero(self, data):
        """
        Process and fill the values at the paths as apropos when the
        fill method is either pad or zero.
        """
        for path in self._field_spec:

            field_path = self._field_path_to_array(path)

            # initialize a counter for this column
            if tuple(field_path) not in self._key_count:
                self._key_count[tuple(field_path)] = 0

            val = nested_get(data, field_path)

            # this is pointing at a path that does not exist
            if val == 'bad_path':
                self._warn('path does not exist: {0}'.format(field_path), ProcessorWarning)
                continue

            if not is_valid(val):
                # massage the path per selected method

                # have we hit the limit?
                if self._fill_limit is not None and \
                        self._key_count[tuple(field_path)] >= self._fill_limit:
                    continue

                if self._method == 'zero':  # set to zero
                    nested_set(data, field_path, 0)
                    # note that this column has been zeroed
                    self._key_count[tuple(field_path)] += 1

                elif self._method == 'pad':  # set to previous value
                    if self._previous_event is not None:
                        if is_valid(self._previous_event.get(field_path)):
                            nested_set(
                                data, field_path,
                                self._previous_event.get(field_path)
                            )
                            # note that this column has been padded
                            # on success
                            self._key_count[tuple(field_path)] += 1

            else:
                # it is a valid value, so reset the counter for
                # this column
                self._key_count[tuple(field_path)] = 0

    def _is_valid_linear_event(self, event):
        """
        Check to see if an even has good values when doing
        linear fill since we need to keep a completely intact
        event for the values.

        While we are inspecting the data payload, make a note if
        any of the paths are pointing at a list. Then it
        will trigger that filling code later.
        """

        valid = True

        field_path = self._field_path_to_array(self._field_spec[0])

        val = nested_get(thaw(event.data()), field_path)

        # this is pointing at a path that does not exist, issue a warning
        # can call the event valid so it will be emitted. can't fill what
        # isn't there.
        if val == 'bad_path':
            self._warn('path does not exist: {0}'.format(field_path), ProcessorWarning)
            return valid

        # a tracked field path is not valid so this is
        # not a valid linear event. also, if it is not a numeric
        # value, mark it as invalid and let _interpolate_event_list()
        # complain about/skip it.
        if not is_valid(val) or not isinstance(val, numbers.Number):
            valid = False

        return valid

    def _linear_fill(self, event):
        """
        This handles the linear filling. It returns a list of
        events to be emitted. That list may only contain a single
        event.

        If an event is valid - it has valid values for all of
        the field paths - it is cached as "last good" and
        returned to be emitted. The return value is a list
        of one event.

        If an event has invalid values, it is cached to be
        processed later and an empty list is returned.

        Additional invalid events will continue to be cached until
        a new valid value is seen, then the cached events will
        be filled and returned. That will be a list of indeterminate
        length.
        """

        # see if the event is valid and also if it has any
        # list values to be filled.
        is_valid_event = self._is_valid_linear_event(event)

        # Deal with the event as apropos depending on if it is
        # valid or not and if we have nor have not seen a valid
        # event yet.

        events = list()

        if is_valid_event and not self._linear_fill_cache:
            # valid event, no cached events, use as last good
            # and return the event. This is what we want to see.
            self._last_good_linear = event
            events.append(event)
        elif not is_valid_event and self._last_good_linear is not None:
            # an invalid event was received and we have previously
            # seen a valid event, so add to the cache for fill processing
            # later.
            self._linear_fill_cache.append(event)

            # now make sure we have not exceeded the fill_limit
            # if it has been set. if it has, emit all the cached
            # events and reset the main state such that the next
            # condition will continue to trigger until we see another
            # valid event.

            if self._fill_limit is not None and \
                    len(self._linear_fill_cache) >= self._fill_limit:

                for i in self._linear_fill_cache:
                    self.emit(i)

                self._linear_fill_cache = list()
                self._last_good_linear = None

        elif not is_valid_event and self._last_good_linear is None:
            # an invalid event but we have not seen a good
            # event yet so there is nothing to start filling "from"
            # so just return and live with it.
            events.append(event)
        elif is_valid_event and self._linear_fill_cache:
            # a valid event was received, and there are cached events
            # to be processed, so process and return the filled events
            # to be emitted.

            event_list = [self._last_good_linear] + self._linear_fill_cache + [event]

            # the first event a.k.a. self._last_good_linear has
            # already been emitted either as a "good"
            # event or as the last event in the previous filling pass.
            # that's why it's being shaved off here.
            for i in self._interpolate_event_list(event_list)[1:]:
                events.append(i)

            # reset the cache, note as last good
            self._linear_fill_cache = list()
            self._last_good_linear = event

        return events

    def add_event(self, event):
        """
        Perform the fill operation on the event and emit.

        Parameters
        ----------
        event : Event, IndexedEvent, TimerangeEvent
            Any of the three event variants.
        """
        if self.has_observers():

            to_emit = list()

            new_data = thaw(event.data())

            if self._method in ('zero', 'pad'):
                # zero and pad use much the same method in that
                # they both will emit a single event every time
                # add_event() is called.
                self._pad_and_zero(new_data)
                emit = event.set_data(new_data)
                to_emit.append(emit)
                # remember previous event for padding
                self._previous_event = emit
            elif self._method == 'linear':
                # linear filling follows a somewhat different
                # path since it might emit zero, one or multiple
                # events every time add_event() is called.
                for emit in self._linear_fill(event):
                    to_emit.append(emit)

            # end filling logic

            for emitted_event in to_emit:
                self._log('Filler.add_event', 'emitting: {0}', (emitted_event,))
                self.emit(emitted_event)

    def _interpolate_event_list(self, events):  # pylint: disable=too-many-branches, too-many-locals
        """
        The fundamental linear interpolation workhorse code.  Process
        a list of events and return a new list. Does a pass for
        every field_spec.

        This is abstracted out like this because we probably want
        to interpolate a list of events not tied to a Collection.
        A Pipeline result list, etc etc.

        Sorry pylint, sometime you need to write a complex method.
        """
        base_events = copy.copy(events)
        # new array of interpolated events for each field path
        new_events = list()

        field_path = self._field_path_to_array(self._field_spec[0])

        # setup done, loop through the events.
        for event_enum in enumerate(base_events):
            # cant interpolate first or last event so just save it
            # as-is and move on.
            if event_enum[0] == 0 or event_enum[0] == len(base_events) - 1:
                new_events.append(event_enum[1])
                continue

            # if a non-numeric value is encountered, stop processing
            # this field spec and hand back the original unfilled events.
            if is_valid(event_enum[1].get(field_path)) and \
                    not isinstance(event_enum[1].get(field_path),
                                   numbers.Number):
                self._warn(
                    'linear requires numeric values - skipping this field_spec',
                    ProcessorWarning
                )
                return base_events

            # found a bad value so start calculating.
            if not is_valid(event_enum[1].get(field_path)):

                previous_value = None
                previous_ts = None
                next_value = None
                next_ts = None

                # look to the previous event in the new_event list since
                # that's where previously interpolated values will be.
                # if found, get the timestamp as well.

                previous_value = new_events[event_enum[0] - 1].get(field_path)

                if previous_value:
                    previous_ts = ms_from_dt(new_events[event_enum[0] - 1].timestamp())

                # see about finding the next valid value and its timestamp
                # in the original list.

                next_idx = event_enum[0] + 1

                while next_value is None and next_idx < len(base_events):

                    val = base_events[next_idx].get(field_path)

                    if is_valid(val):
                        next_ts = ms_from_dt(base_events[next_idx].timestamp())
                        next_value = val  # terminates the loop

                    next_idx += 1

                # previous_value should only be none if there are a string
                # of bad values at the beginning of the sequence.
                # next_value will be none if that value no longer has
                # valid values in the rest of the sequence.

                if previous_value is not None and next_value is not None:
                    # pry the data from current event
                    new_data = thaw(event_enum[1].data())
                    current_ts = ms_from_dt(event_enum[1].timestamp())

                    if previous_ts == next_ts:
                        # average the two values
                        new_val = truediv((previous_value + next_value), 2)
                    else:
                        point_frac = truediv(
                            (current_ts - previous_ts), (next_ts - previous_ts))
                        new_val = previous_value + ((next_value - previous_value) * point_frac)
                    # set that value to the field spec in new data
                    nested_set(new_data, field_path, new_val)
                    # call .set_data() to create a new event
                    new_events.append(event_enum[1].set_data(new_data))
                else:
                    # couldn't calculate new value either way, just
                    # keep the old event.
                    new_events.append(event_enum[1])

            else:
                # theoretically never called because the incoming lists
                # will be bookended by valid events now that we're only
                # processing a single column per Filler instance.
                # leaving here in case we start feeding this new data.
                new_events.append(event_enum[1])  # pragma: no cover

        # save the current state before doing another pass
        # on a different field_path

        return new_events

    def flush(self):
        """Don't delegate flush to superclass yet. Make sure
        there are no cached events (could happen if we stop
        seeing valid events) before passing it up the food
        chain."""
        self._log('Filler.flush')

        if self.has_observers() and self._method == 'linear':
            self._log('Filler.flush.linear')
            # are there any left-over events like if a path
            # just stops seeing any good events so they are
            # never filled and emitted.
            for i in self._linear_fill_cache:
                self.emit(i)

        super(Filler, self).flush()
