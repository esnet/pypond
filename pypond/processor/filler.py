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
from ..util import is_pipeline, Options, nested_set, nested_get, is_valid


class Filler(Processor):  # pylint: disable=too-many-instance-attributes
    """
    A processor that fills missing/invalid values in the event
    with new values (zero, interpolated or padded).

    Number of filled events in new series can be controlled by
    putting .take() in the pipeline chain.

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
        self._emit_on = None

        # internal members
        # state for pad to refer to previous event
        self._previous_event = None
        # record of filled list values for linear to
        # alternately skip or fill depending on context.
        self._filled_lists = list()
        # special state for linear fill
        self._last_good_linear = None
        # cache of events pending linear fill
        self._linear_fill_cache = list()

        if isinstance(arg1, Filler):
            # pylint: disable=protected-access
            self._field_spec = arg1._field_spec
            self._method = arg1._method
            self._mode = arg1._mode
            self._emit_on = arg1._emit_on
        elif is_pipeline(arg1):
            self._field_spec = options.field_spec
            self._method = options.method
            self._mode = arg1.mode()
            self._emit_on = arg1.get_emit_on()
        else:
            msg = 'Unknown arg to Filler: {0}'.format(arg1)
            raise ProcessorException(msg)

        self._log('Filler.init.Options', options)

        if self._method not in ('zero', 'pad', 'linear'):
            msg = 'Unknown method {0} passed to Filler'.format(self._method)
            raise ProcessorException(msg)

        if isinstance(self._field_spec, six.string_types):
            self._field_spec = [self._field_spec]

    def clone(self):
        """clone it."""
        return Filler(self)

    def _recurse(self, data, keys=()):
        """
        Do the actual recursion and yield the keys to _generate_paths()
        """
        if isinstance(data, dict):
            for key in list(data.keys()):
                for path in self._recurse(data[key], keys + (key,)):
                    yield path
        else:
            yield keys

    def _generate_paths(self, new_data):
        """
        Return a list of field spec paths for the entire
        data dict that can  be used by pypond.util.nested_set
        and nested_get for filling. Just a
        wrapper to aggregate the results from _recurse().
        """

        paths = list()

        for key in self._recurse(new_data):
            paths.append(key)

        return paths

    def _pad_and_zero(self, data, paths):
        """
        Process and fill the values at the paths as apropos when the
        fill method is either pad or zero.
        """
        for path in paths:

            field_path = self._field_path_to_array(path)

            val = nested_get(data, field_path)

            # this is pointing at a path that does not exist
            if val == 'bad_path':
                self._warn('path does not exist: {0}'.format(field_path), ProcessorWarning)
                continue

            # if the terminal value is a list, fill the list
            # make a note of any field spec containing lists
            # so the main interpolation code will ignore it.
            if isinstance(val, list):
                if field_path not in self._filled_lists:
                    # don't add it more than once
                    self._filled_lists.append(field_path)
                self._fill_list(val)

            if not is_valid(val):
                # massage the path per selected method

                if self._method == 'zero':  # set to zero
                    nested_set(data, field_path, 0)

                elif self._method == 'pad':  # set to previous value
                    if self._previous_event is not None:
                        if is_valid(self._previous_event.get(field_path)):
                            nested_set(
                                data, field_path,
                                self._previous_event.get(field_path)
                            )

                elif self._method == 'linear':
                    # no-op here, interpolation has to happen
                    # when the operation is flushed.
                    # just stubbing in the condition
                    pass

    def _is_valid_linear_event(self, event, paths):
        """
        Check to see if an even has good values when doing
        linear fill since we need to keep a completely intact
        event for the values.

        While we are inspecting the data payload, make a note if
        any of the paths are pointing at a list. Then it
        will trigger that filling code later.
        """

        valid = True

        for i in paths:

            field_path = self._field_path_to_array(i)

            val = nested_get(thaw(event.data()), field_path)

            # this is pointing at a path that does not exist
            if val == 'bad_path':
                self._warn('path does not exist: {0}'.format(i), ProcessorWarning)
                continue

            # a tracked field path is not valid so this is
            # not a valid linear event.
            if not is_valid(val):
                valid = False

            # make a note that there is a list that needs to
            # be filled if need be.  No need to look at
            # the paths twice.
            if isinstance(val, list):
                if field_path not in self._filled_lists:
                    self._filled_lists.append(field_path)

        return valid

    def _linear_fill(self, event, paths):
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
        is_valid_event = self._is_valid_linear_event(event, paths)

        # Are we filling any list values?  This needs to happen
        # as it's own step regardless as to if the event is
        # valid or not.
        if self._filled_lists:
            new_data = thaw(event.data())
            for i in self._filled_lists:
                val = nested_get(new_data, i)
                self._fill_list(val)
            event = event.set_data(new_data)

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
            for i in self._interpolate_event_list(event_list, paths)[1:]:
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

            if self._field_spec is None:
                # generate a list of all possible field paths
                # if no field spec is specified.
                paths = self._generate_paths(new_data)
            else:
                paths = self._field_spec

            if self._method in ('zero', 'pad'):
                # zero and pad use much the same method in that
                # they both will emit a single event every time
                # add_event() is called.
                self._pad_and_zero(new_data, paths)
                emit = event.set_data(new_data)
                to_emit.append(emit)
                # remember previous event for padding
                self._previous_event = emit
            elif self._method == 'linear':
                # linear filling follows a somewhat different
                # path since it might emit zero, one or multiple
                # events every time add_event() is called.
                for emit in self._linear_fill(event, paths):
                    to_emit.append(emit)

            # end filling logic

            for emitted_event in to_emit:
                self._log('Filler.add_event', 'emitting: {0}'.format(emitted_event))
                self.emit(emitted_event)

    def _fill_list(self, obj):
        """
        Do basic filling if a terminal value is a list.
        """

        for val_enum in enumerate(obj):

            # can't do linear on non-numeric values
            if self._method == 'linear' and is_valid(val_enum[1]) and \
                    not isinstance(val_enum[1], numbers.Number):
                self._warn(
                    'linear requires numeric values - skipping this list',
                    ProcessorWarning
                )

                break

            # we got a bad value so fill as apropos
            if not is_valid(val_enum[1]):

                if self._method == 'zero':
                    # set the invalid value to 0
                    obj[val_enum[0]] = 0

                if self._method == 'pad' and val_enum[0] - 1 >= 0 and \
                        is_valid(obj[val_enum[0] - 1]):
                    # pad current value with previous value if the
                    # prevous value was valid.
                    obj[val_enum[0]] = obj[val_enum[0] - 1]

                if self._method == 'linear':
                    # do a linear fill on each values if it can find a
                    # valid previous and future value.

                    previous = None
                    next_val = None

                    # is the previous value valid?
                    if val_enum[0] - 1 >= 0 and \
                            is_valid(obj[val_enum[0] - 1]):
                        previous = obj[val_enum[0] - 1]

                    # let's look for the next valid value.
                    next_idx = val_enum[0] + 1

                    while next_val is None and next_idx < len(obj):

                        val = obj[next_idx]

                        if is_valid(val):
                            next_val = val  # breaks loop

                        next_idx += 1

                    # we nailed two values to fill from
                    if previous is not None and next_val is not None:
                        inval = truediv((previous + next_val), 2)
                        obj[val_enum[0]] = inval

                    if next_val is None:
                        # there are no more valid values "forward"
                        # so we're done
                        break

    def _interpolate_event_list(self, events, paths):  # pylint: disable=too-many-branches
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

        for i in paths:

            # new array of interpolated events for each field path
            new_events = list()

            # boolean to keep track if there are no longer any valid
            # events "forward" in the sequence for a given field_path.
            # if there are no more valid values, there is no reason
            # to keep seeking every time.
            seek_forward = True

            field_path = self._field_path_to_array(i)

            # make sure this field path is not a list type that
            # has already been filled.

            if field_path in self._filled_lists:
                continue

            # setup done, loop through the events.
            for event_enum in enumerate(base_events):
                # cant interpolate first or last event so just save it
                # as-is and move on.
                if event_enum[0] == 0 or event_enum[0] == len(base_events) - 1:
                    new_events.append(event_enum[1])
                    continue

                # if a non-numeric value is encountered, stop processing
                # this field spec.
                if is_valid(event_enum[1].get(field_path)) and \
                        not isinstance(event_enum[1].get(field_path),
                                       numbers.Number):
                    self._warn(
                        'linear requires numeric values - skipping this field_spec',
                        ProcessorWarning
                    )
                    break

                # found a bad value so start calculating.
                if not is_valid(event_enum[1].get(field_path)):

                    previous_value = None
                    next_value = None

                    # look to the previous event in the new_event list since
                    # that's where previously interpolated values will be.

                    previous_value = new_events[event_enum[0] - 1].get(field_path)

                    # see about finding the next valid value in the original
                    # list.

                    next_idx = event_enum[0] + 1

                    while next_value is None and next_idx < len(base_events):

                        # no more good values "forward" so don't bother.
                        if seek_forward is False:
                            break

                        val = base_events[next_idx].get(field_path)

                        if is_valid(val):
                            next_value = val  # terminates the loop

                        next_idx += 1

                    # previous_value should only be none if there are a string
                    # of bad values at the beginning of the sequence.
                    # next_value will be none if that value no longer has
                    # valid values in the rest of the sequence.

                    if previous_value is not None and next_value is not None:
                        # pry the data from current event
                        new_data = thaw(event_enum[1].data())
                        # average the two values
                        new_val = truediv((previous_value + next_value), 2)
                        # set that value to the field spec in new data
                        nested_set(new_data, field_path, new_val)
                        # call .set_data() to create a new event
                        new_events.append(event_enum[1].set_data(new_data))
                    else:
                        # couldn't calculate new value either way, just
                        # keep the old event.
                        new_events.append(event_enum[1])

                        if next_value is None:
                            # no more good values for this field spec in the
                            # sequence, so don't bother looking on subsequent
                            # events in this field_spec
                            seek_forward = False

                else:
                    new_events.append(event_enum[1])

            # save the current state before doing another pass
            # on a different field_path
            base_events = new_events

        return base_events

    def flush(self):
        """Don't delegate flush to superclass yet. Linear interpolation
        needs to happen after the events have been processed but before
        they are finally emitted."""
        self._log('Filler.flush')

        if self.has_observers() and self._method == 'linear':
            self._log('Filler.flush.linear')
            # are there any left-over events like if a path
            # just stops seeing any good events so they are
            # never filled and emitted.
            for i in self._linear_fill_cache:
                self.emit(i)

        super(Filler, self).flush()