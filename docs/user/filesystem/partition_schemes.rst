.. _fsds_partition_schemes:

Partition Schemes
=================

Partition schemes define how data is stored on the filesystem. The scheme is important because it determines how
the data is queried. When evaluating a query filter, the partition scheme is leveraged to prune data files that
do not match the filter. There are three main types of partition schemes provided: spatial, temporal and attribute.

The partition scheme must be provided when creating a schema. The scheme is defined by a well-known name
and additional configuration options. See :ref:`partition_scheme_config` for details on how to specify a partition
scheme.

Temporal Schemes
----------------

Temporal schemes partition data based on time. The following names are supported:

* ``year`` (or ``years`` / ``yearly``)
* ``month`` (or ``months`` / ``monthly``)
* ``week`` (or ``weeks`` / ``weekly``)
* ``day`` (or ``days`` / ``daily``)
* ``hour`` (or ``hours`` / ``hourly``)

The following options are supported:

* ``attribute`` - The name of a ``Date``\ -type attribute from the SimpleFeatureType to use. If not specified, the default
  date attribute is used.
* ``step`` - The number of time units (hours, days, etc) to include in each partition. If not specified, the default is ``1``.

Spatial Schemes
---------------

Spatial schemes lay out data based on a space-filling curve. The following names are supported:

* ``z2`` - A curve suitable for point-type geometries
* ``xz2`` - A curve suitable for geometries with extents (e.g. non-points such as line strings or polygons)

The following options is required:

* ``bits`` - The number of bits to use for the curve, which defines the area of each partition. For example, 2 bits would
  create ``2 ^ 2`` (4) regions, while 3 bits would create ``2 ^ 3`` (8) regions.

The following options are supported:

* ``attribute`` - The name of a ``Geometry``\ -type attribute from the SimpleFeatureType to use. If not specified, the
  default geometry is used.

Attribute Scheme
----------------

The attribute scheme partitions data based on a lexicoded attribute value. The name must be:

* ``attribute``

The following option is required:

* ``attribute`` - The name of the attribute used to partition

The following options are supported:

* ``default`` - A default value to use if the attribute is null
* ``allow`` - An allowed value. ``allow`` may be specified more than once, in order to allow multiple values. If an attribute
  is not in the allowed values, the the ``default`` value will be used instead

The following additional options are supported to bucket the partition values, depending on the type of attribute being used:

* ``width`` - For string type attributes, the value will be truncated to ``width`` max length
* ``divisor`` - For integral type attributes (e.g. ints and longs), the value will be rounded down so that it is divisible by
  ``divisor``. For example, with ``divisor=10``, ``100``, ``109``, etc will all be truncated to ``100``.
* ``scale`` - For fractional type attributes (e.g. floats and doubles), the number of digits to keep to the right of the decimal
  place. For example, with ``scale=2``, ``100.001``, ``100.009``, etc will all be truncated to ``100.00``

The attribute scheme supports the following attribute types: ``String``, ``Integer``, ``Long``, ``Float`` and ``Double``.

Hash Scheme
-----------

The hash scheme partitions data into buckets based on an attribute value. The name must be:

* ``hash``

The following options are required:

* ``attribute`` - The name of the attribute used to partition
* ``buckets`` - The number of buckets used to partition

The hash scheme supports the following attribute types:
``String``, ``Integer``, ``Long``, ``Float``, ``Double``, ``Date``, ``Bytes``, and ``UUID``.
