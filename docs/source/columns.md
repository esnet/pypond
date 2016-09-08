# Data columns: field_spec and field_path

There are some points to note about the nomenclature that the `pypond` and `pond` code bases use to refer to the "columns of data" in the time series event objects. This `TimeSeries`:

```
DATA = dict(
    name="traffic",
    columns=["time", "value", "status"],
    points=[
        [1400425947000, 52, "ok"],
        [1400425948000, 18, "ok"],
        [1400425949000, 26, "fail"],
        [1400425950000, 93, "offline"]
    ]
)
```
contains two columns: `value` and `status`.

However this `TimeSeries`:

```
DATA_FLOW = dict(
    name="traffic",
    columns=["time", "direction"],
    points=[
        [1400425947000, {'in': 1, 'out': 2}],
        [1400425948000, {'in': 3, 'out': 4}],
        [1400425949000, {'in': 5, 'out': 6}],
        [1400425950000, {'in': 7, 'out': 8}]
    ]
)
```
contains only one column `direction`, but that column has two more columns - `in` and `out` - nested under it. In the following examples, these nested columns will be referred to as "deep paths."

When specifying columns to the methods that set, retrieve and manipulate data, we use two argument types: `field_spec` and `field_path`. They are similar yet different enough to warrant this document.

## field_path

A `field_path` refers to a **single** column in a series. Any method that takes a `field_path` as an argument only acts on one column at a time. The value passed to this argument can be either a string, a list or `None`.

### String variant

When a string is passed, it can be one of the following formats:

* simple path - the name of a single "top level" column. In the `DATA` example above, this would be either `value` or `status`.
* deep path - the path pointing to a single nested columns with each "segment" of the path delimited with a period. In the `DATA_FLOW` example above, the incoming data could be retrieved with `direction.in` as the `field_path`.

### List variant

When a `list` (or `tuple`) is passed as a `field_path`, each element in the iterable is **a single segment of the path to a column**. So to compare with the string examples:

* `['value']` would be equivalent to the string `value`.
* `['direction', 'in']` would be equivalent to the string `direction.in`.

This is particularly important to note because **this behavior is different** than passing a list to a `field_spec` arg.

### `None`

If no `field_path` is specified (defaulting to `None`), then the default column `value` will be used.

## field_spec

A `field_spec` refers to **one or more** columns in a series. When a method takes a `field_spec`, it may act on multiple columns in a `TimeSeries`. The value passed to this argument can be either a string, a list or `None`.

### String variant

The string variant is essentially identical to the `field_path` string variant - it is a path to a single column of one of the following formats:

* simple path - the name of a single "top level" column. In the `DATA` example above, this would be either `value` or `status`.
* deep path - the path pointing to a single nested columns with each "segment" of the path delimited with a period. In the `DATA_FLOW` example above, the incoming data could be retrieved with `direction.in` as the `field_path`.

### List variant

Passing a `list` (or `tuple`) to `field_spec` is different than the aforementioned behavior in that it is explicitly referring to **one or more columns**. Rather than each element being segments of a path, **each element is a full path to a single column**.

Using the previous examples:

* `['in', 'out']` would act on both the `in` and `out` columns from the `DATA` example.
* `['direction.in', 'direction.out']` - here each element is a fully formed "deep path" to the two data columns in the `DATA_FLOW` example.

The lists do not have to have more than one element: `['value'] == 'value'`.

NOTE: accidentally passing this style of list to an arg that is actually a `field_path` will most likely result in an `EventException` being raised. Passing something like `['in', 'out']` as a `field_path` will attempt to retrieve the nested column `in.out` which probably doesn't exist.

### `None`

If no `field_spec` is specified (defaulting to `None`), then the default column `value` will be used.

## field_spec_list

This is a less common variant of the `field_spec`. It is used in the case where the method is going to **specifically act on multiple columns**. Otherwise, it's usage is identical to the list variant of `field_spec`.
