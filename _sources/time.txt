# Notes on time handling

## UTC vs. local time

### Initializing Event objects

All of the Event variants can be initialized with  UTC milliseconds since the epoch, or an **aware** python `datetime` object. If a **naive** `datetime` object is passed in, an exception will be raised. When passing in a `datetime` object, [it is encouraged](https://www.youtube.com/watch?v=-5wpm-gesOY) that they be in UTC as well.

Be aware that if an aware non-UTC/local time `datetime` object is passed in, a warning will be issued, it will be converted to a UTC `datetime` object and that's what will be used internally.  The conversion will be done like this using the python `datetime` library and the third party `pytz` library thusly:

```
    dtime.astimezone(pytz.UTC)
```

And the resulting `datetime` object will be converted to milliseconds (see section on precision).

This is for consistency, for parity with the JavaScript Date library that uses epoch ms at its core, and because the Pond/PyPond wire format relies on epoch ms. One really can't go wrong with initially reporting all of their events using milliseconds since the epoch. *Please consider doing that.*

### Rendering in local time

Of course there are cases where is desirable to represent time series data in the user's local time zone. Like in a graphing application. Even though PyPond does only business in UTC internally, this is possible. This can be changed on how you window and aggregate the data.

See the section on Aggregation in the [main Pond Pipeline documentation](http://software.es.net/pond/#/pipeline). Note how you specify the `.windowBy()` (`.window_by()` in PyPond) value in the pipeline chain. This can be a fixed value like `1d` where it will aggregate the data into daily buckets. Fixed windows like that can **only** be rendered in UTC. Or it can be a non-fixed value like `daily` which will also aggregate the data into daily buckets, but the user can choose how to render the data in that case.

The default will be to render in UTC - any such choice will always default to UTC, the user will always need to set `utc=False` where appropriate. But when using a non-fixed window, the optional utc boolean can be set:

```
    kcol = (
        Pipeline()
        .from_source(timeseries)
        .window_by('daily', utc=False)
        .emit_on('eachEvent')
        .aggregate({'in': Functions.avg(), 'out': Functions.avg()})
        .to_keyed_collections()
    )
```
Then the aggregation key/buckets be daily averages in the local time zone.

There is also a trio of helper functions in the `TimeSeries` class that presents a higher level access to this functionality:

```
    TimeSeries.daily_rollup()
    TimeSeries.monthly_rollup()
    TimeSeries.yearly_rollup()
```
They all take a `dict` of a column name and an aggregation function as in the above example:

```
    TimeSeries.monthly_rollup({'in': Functions.avg(), 'out': Functions.avg()})
```
And the data will automatically be rendered in local time.

#### Conversion to local time

When the conversion covered in the previous section happens, the user has no control over **what** time zone it will be rendered to.  All conversions will automatically happen using the local time zone as determined by the `tzlocal` library:

```
    LOCAL_TZ = tzlocal.get_localzone()
```
This is primarily for parity with the JavaScript library which will be running browser-side and will be localizing as apropos. Moreover, the scope of this library is not to be a time handling swiss army knife.

### Local time and the IndexedEvent class

The only `Event` class that explicitly takes a `utc=False` flag is the `IndexedEvent` class. It behaves somewhat differently than the `Event` and `TimeRangeEvent` classes which do not.  Rather than being initialized with an epoch ms timestamp or a `datetime` object they are initialized with strings of the following formats:

```
        The index string arg will may be of two forms:

        - 2015-07-14  (day)
        - 2015-07     (month)
        - 2015        (year)

        or:

        - 1d-278      (range, in n x days, hours, minutes or seconds)

        and return a TimeRange for that time. The TimeRange may be considered to be
        local time or UTC time, depending on the utc flag passed in.
```
A UTC conversion will still happen under the hood, just a little differently.

If an `Index` (which is the underlying time-handling structure to `IndexedEvent`) is initialized thusly:

```
    utc = Index('2015-07-14')
```
That is a daily index and is internally creating a range spanning that entire day. So looking at the internal timestamps yields this:

```
    print(utc.begin(), utc.end())
    2015-07-14 00:00:00+00:00 2015-07-14 23:59:59+00:00
```

But doing the same thing with `utc=False` (if you are in Pacific Time) yields this:

```
    local = Index('2015-07-14', utc=False)
    print(local.begin(), local.end())
    2015-07-14 07:00:00+00:00 2015-07-15 06:59:59+00:00
```
The time range is **not** internally held as spanning that day in the local time zone, it is converted and reflected in UTC.

Yet another example of why it is preferred to input and store the data in UTC and view it in a localized way.

## Precision

Internal timestamps are precise down to the millisecond even though the python `datetime` object is precise down to the microsecond.  This is primarily for parity with the JavaScript library - the JS `Date` object is only accurate down to the millisecond. Unit testing showed that allowing microsecond accuracy exposed discrepancies between times that should have been "the same."

It is perfectly fine to pass in python `datetime` objects that have microsecond accuracy, just be aware that it will be rounded to milliseconds automatically.