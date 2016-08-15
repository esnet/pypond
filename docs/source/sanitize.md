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

#### Linear fill and multiple field specs.

It is possible to fill multiple field specs using all three fill methods. Filling multiple columns using the `linear` mode introduces an additional wrinkle that should be noted. For an event to be considered "valid" (and used as a bookend in filling a sequence) there needs to be a valid value in **all** columns that are being filled.  Consider the following `TimeSeries` and fill directive:

```
    simple_missing_data = dict(
        name="traffic",
        columns=["time", "direction"],
        points=[
            [1400425947000, {'in': 1, 'out': None}],
            [1400425948000, {'in': None, 'out': None}],
            [1400425949000, {'in': None, 'out': None}],
            [1400425950000, {'in': 3, 'out': 5}],
            [1400425960000, {'in': None, 'out': None}],
            [1400425970000, {'in': 5, 'out': 12}],
            [1400425980000, {'in': 6, 'out': 13}],
        ]
    )

    ts = TimeSeries(simple_missing_data)

    new_ts = ts.fill(field_spec=['direction.in', 'direction.out'],
                     method='linear')
```
The `Filler` processor will not start filling until it hits the 4th `Event` in the series. That is because it is the first "completely valid" event in the series since we are looking at multiple columns. So even though points 2 and 3 of `direction.in` could theoretically be filled, they will not be.

This behavior may be the desired effect. But if this presents a potential problem in the data you are filling, consider chaining multiple `Filler` processors together in a `Pipeline`:

```
    elist = (
        Pipeline()
        .from_source(ts)
        .fill(field_spec='direction.in', method='linear')
        .fill(field_spec='direction.out', method='linear')
        .to_event_list()
    )
```
This would ensure that both columns are filled independently of each other.

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

It is like any other `Pipeline` construction, but the `linear` method has the following restrictions and behaviors:

* If a non numeric value (as determined by `isinstance(val, numbers.Number)`) is encountered when doing a `linear` fill, a warning will be issued and that field spec will cease being processed.
* When using a streaming input like `UnboundedIn`, it is a best practice to set a limit using the optional arg `cache_limit`. This is because if a given field spec stops seeing a valid value, it will be necessary to limit how many events will get cached for filling because the cached values will not be emitted until a valid value is seen.
* Setting `cache_limit` is not needed when using a bounded source because the calls to `.flush()` will make sure any cached values will be emitted.
* Also, when using an unbounded source, make sure to shut it down "cleanly" using `.stop()`. This will ensure `.flush()` is called (see previous point).

The `Filler` processor will raise `ProcessorException` otherwise.

### List values

If `TimeSeries.fill()` is being used on a series where an actual value is a list of values:

```
    simple_list_data = dict(
        name="traffic",
        columns=["time", "series"],
        points=[
            [1400425947000, [None, None, 3, 4, 5, 6, 7]],
            [1400425948000, [1, None, None, 4, 5, 6, 7]],
            [1400425949000, [1, 2, 3, 4, None, None, None]],
            [1400425950000, [1, 2, 3, 4, None, None, 7]],
        ]
    )
```
Filling will be performed on the values inside the lists as well. As above, if the method is `linear` and it encounters a non-numeric value, a warning will be issued and the list will not be processed.

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