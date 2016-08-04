# Fill and other sanitizing methods

Real world data can have gaps, bad names, or occur at irregular intervals. The pypond toolkit contains some methods to adjust or sanitize a series of less than optimal data. As with all other mutation operations in pypond, these methods will return new `Event` objects, new `Collections` and new `TimeSeries` as apropos.

## Fill

Data might contain missing or otherwise invalid values. `TimeSeries.fill()` can perform a variety of fill operations to smooth or make sure that the data can be processed in math operations without blowing up.

In pypond, a value is considered "invalid" if it is python `None`, a `NaN` (not a number) value, or an empty string.

### Usage

The method prototype looks like this:

```
def fill(self, field_spec=None, method='zero', limit=None):
```

* the `field_spec` argument is the same as it is in the rest of the code - a string or list of strings denoting "columns" in the data. It can point `to.deep.values` using the usual dot notation.
* the `method` arg denotes the fill method to use. Valid values are **zero**, **pad** and **linear**.
* the `limit` arg places a limit on the number of events that will be filled and returned in the new `TimeSeries`. The default is to fill all the events with no limit.

Complete sample usage could look like this:

```
    ts = TimeSeries(simple_missing_data)

    new_ts = ts.fill(field_spec=['direction.in', 'direction.out'],
                     method='linear', limit=6)
```

### Fill methods

There are three fill options:

* `zero` - the default - will transform any invalid value to a zero.
* `pad` - replaces an invalid value with the the previous good value: `[1, None, None, 3]` becomes `[1, 1, 1, 3]`.
* `linear` - interpolate the gaps based on the surrounding good values: `[1, None, None, 3]` becomes `[1, 2, 2.5, 3]`.

Neither `pad` or `linear` can fill the first value in a series if it is invalid, and they can't start filling until good value has been seen: `[None, None, None, 1, 2, 3]` would remain unchanged. Similarly, `linear` can not fill the last value in a series.

### Filling with the `Pipeline`

Using `TimeSeries.fill()` will be a common entry point to this functionality, but the processor can be used directly in a roll your own `Pipeline` as well:

```
    elist = (
        Pipeline()
        .from_source(ts)
        .emit_on('flush')  # it's linear
        .fill(field_spec='direction.in', method='linear')
        .to_event_list()
    )
```

It is like any other `Pipeline` construction, but the `linear` method has the following restrictions:

* It can not be used in `stream` mode since the entire result set needs to be collected before filling; and
* `emit_on` needs to be set to `flush` so only the filled collection will be emitted.

The `Filler` processor will raise `ProcessorException` otherwise.

## Rename

It might be necessary to rename the columns/data keys in the events in a `TimeSeries`. It is preferable to just give the columns/keys the desired names when the `Event` objects are being instantiated. This is because using `TimeSeries.rename()` will create all new `Event` objects and a new `TimeSeries` as well. But if that is necessary, use this method.

### Usage

This method takes a python dict of strings in the format `{'key': 'new_key'}`. This example:

```
    ts = TimeSeries(TICKET_RANGE)

    renamed = ts.rename_columns({'title': 'event', 'esnet_ticket': 'ticket'})
```
will rename the existing column `title` to `event`, etc.

### Limitations

Unlike other uses of a `field_spec` to point at a `deep.nested.value` in pypond, `.rename()` only allows renaming a 'top level' column/key. If the data payload looks like this:

```
    {'direction': {'in': 5, 'out': 7}}
```
The top level key `direction` can be renamed but the nested keys `in` and `out` can not.

## Align

TBA