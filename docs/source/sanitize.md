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

#### The `fill_limit` arg

The optional arg `fill_limit` controls how many values will be filled before it gives up and starts returning the invalid data until a valid value is seen again.

There might be a situation where it makes sense to fill in a couple of missing values, but no sense to pad out long spans of missing data. This arg sets the limit of the number of missing values will be filled - or in the case of `linear` *attempt* to be filled - before it just starts returning invalid data until the next valid value is seen.

So given `fill_limit=2` the following values will be filled in the following ways:

```
Original:
    [1, None, None, None, 5, 6, 7]

Zero:
    [1, 0, 0, None, 5, 6, 7]

Pad:
    [1, 1, 1, None, 5, 6, 7]

Linear:
    [1, None, None, None, 5, 6, 7]
```

Using methods `zero` and `pad` the first two missing values are filled and the third is skipped. When using the `linear` method, nothing gets filled because a valid value has not been seen before the limit has been reached, so it just gives up and returns the missing data.

When filling multiple columns, the count is maintained on a per-column basis.  So given the following data:

```
    simple_missing_data = dict(
        name="traffic",
        columns=["time", "direction"],
        points=[
            [1400425947000, {'in': 1, 'out': None}],
            [1400425948000, {'in': None, 'out': None}],
            [1400425949000, {'in': None, 'out': None}],
            [1400425950000, {'in': 3, 'out': 8}],
            [1400425960000, {'in': None, 'out': None}],
            [1400425970000, {'in': None, 'out': 12}],
            [1400425980000, {'in': None, 'out': 13}],
            [1400425990000, {'in': 7, 'out': None}],
            [1400426000000, {'in': 8, 'out': None}],
            [1400426010000, {'in': 9, 'out': None}],
            [1400426020000, {'in': 10, 'out': None}],
        ]
    )
```

The `in` and `out` sub-columns will be counted and filled independently of each other.

If `fill_limit` is not set, no limits will be placed on the fill and all values will be filled as apropos to the selected method.

#### Constructing `linear` fill `Pipeline` chains

`TimeSeries.fill()` will be the common entry point for the `Filler`, but a `Pipeline` can be constructed as well. Even though the default behavior of `TimeSeries.fill()` applies to all fill methods, the `linear` fill logic is somewhat different than the `zero` and `pad` methods. Note the following points when creating your own `method='linear'` processing chain.

This:

```
    Pipeline()
    .from_source(ts)
    .fill(field_spec='direction.in', method='linear')
    .fill(field_spec='direction.out', method='linear')
    .to_keyed_collections()
```

and this:

```
    Pipeline()
    .from_source(ts)
    .fill(field_spec=['direction.in', 'direction.out'], method='linear')
    .to_keyed_collections()
```

Are not functionally identical.

In the former example, the two columns will be filled independently of each other. That is the behavior of `TimeSeries.fill()` and the desired behavior most of the time. In the latter case, the two columns will be treated like a **composite key** when determining if an `Event` is valid or not.

Generally speaking, the first use case will be the one you're looking for.

Other points to note:

* If a non numeric value (as determined by `isinstance(val, numbers.Number)`) is encountered when doing a `linear` fill, a warning will be issued and that field spec will cease being processed.
* When using streaming input like `UnboundedIn`, it is a best practice to set a limit using the optional arg `fill_limit`. This will ensure events will continue being emitted if the data hits a long run of invalid values.
* When using an unbounded source, make sure to shut it down "cleanly" using `.stop()`. This will ensure `.flush()` is called so any unfilled cached events are emitted.

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