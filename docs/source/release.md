# Release notes

Notes about releases, API changes, etc.

## 0.4

First stable release with full feature parity with the Pond JS code base.

## 0.5

### 0.5.0

NOTE: `key` means "the specific timestamp, index or time range an event object exists at."

#### Major changes:

* `Event.merge()` has been changed and is not backwards compatible with the 0.4 version. Previously it took a list of Event objects at the same key and returns a single, merged Event. Now it takes a list of Event objects that can be of differing keys and returns a list of Events where the events at the same key have their values merged into a single event. To wit: `[e(1, {'a': 1}), e(1, {'b': 2}), e(2, {'a': 3}), e(2, {'b': 4})] -> [e(1, {'a': 1, 'b': 2}), e(2, {'a': 3, 'b': 4})]`
* `Event.combine()` has been re-worked to accommodate this, but this is mostly for performance and should be transparent to the user.
* `Event.avg()` and `Event.sum()` (which are helper functions that use `Event.combine()`) now behave like `Event.merge()` and return a list of summed/averaged events, rather than a single event at one key.

#### Additions:

* `Collection.at_key()` retrieves all the events in a `Collection` at a specified key.
* `Collection.dedup()` removes duplicate (events at the key) Event objects from a `Collection`.
* `Collection.event_list_as_map()` returns the Event objects in a `Collection` as a `dict` of `list` where the key is the `key` and the list contains the events at that `key`.
* `Event.key()` and `Event.type()` have been added but are mostly used internally. Have been added to all three event variants.
* `Event.is_duplicate()` compares two events and returns `True` if they are of the same time and exist at the same key. Can also be used to compare payload values as well with an optional flag.

#### Various:

* Added a boolean flag to allow `TimeSeries.daily_rollup()` `.monthly_rollup()` and `.yearly_rollup()` to render results in UTC rather than localtime. They default to rendering in localtime due to client-side concerns (like rendering a chart), but can now render in UTC since it is being used in server-side applications.
* Fixed a bug that impacted `TimeSeries/Collection.at_time()` when the first event should be returned.
