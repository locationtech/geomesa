.. _fsds_partition_schemes:

Partition Schemes
=================

Partition schemes define how data is stored on the filesystem. The scheme is important because it determines how
the data is queried. When evaluating a query filter, the partition scheme is leveraged to prune data files that
do not match the filter. There are three main types of partition schemes provided: spatial, temporal and attribute.

The partition scheme must be provided when creating a schema. The scheme is defined by a well-known name
and a map of configuration options. See :ref:`partition_scheme_config` for details on how to specify a partition
scheme.

Composite Schemes
-----------------

Composite schemes are hierarchical combinations of other schemes. A composite scheme is named by concatenating
the names of the constituent schemes, separated with commas, e.g. ``hourly,z2-2bits``. The configuration
options for each child scheme should be merged into a single configuration for the composite scheme.

Temporal Schemes
----------------

Temporal schemes lay out data based on a Java
`DateTime format string <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html>`__,
separated by forward slashes, which is used to build a directory structure. All temporal schemes support the
following common configuration option:

* ``dtg-attribute`` - The name of a ``Date``\ -type attribute from the SimpleFeatureType to use for partitioning data.
  If not specified, the default date attribute is used.

Date-Time Scheme
^^^^^^^^^^^^^^^^

**Name:** ``datetime``

**Configuration:**

* ``datetime-format`` - A Java `DateTime format string <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html>`__,
  separated by forward slashes, which will be used to build a directory structure. For example, ``yyyy/MM/dd``.
* ``step-unit`` - A ``java.time.temporal.ChronoUnit`` defining how to increment the leaf of the partition scheme
* ``step`` - The amount to increment the leaf of the partition scheme. If not specified, defaults to ``1``

The date-time scheme provides a fully customizable temporal scheme.

Hourly Scheme
^^^^^^^^^^^^^

**Name:** ``hourly``

The hourly scheme partitions data by the hour, using the layout ``yyyy/MM/dd/HH``.

Minute Scheme
^^^^^^^^^^^^^

**Name:** ``minute``

The minute scheme partitions data by the minute, using the layout ``yyyy/MM/dd/HH/mm``.

Daily Scheme
^^^^^^^^^^^^

**Name:** ``daily``

The daily scheme partitions data by the day, using the layout ``yyyy/MM/dd``.

Weekly Scheme
^^^^^^^^^^^^^

**Name:** ``weekly``

The weekly scheme partitions data by the week, using the layout ``yyyy/ww``.

Monthly Scheme
^^^^^^^^^^^^^^

**Name:** ``monthly``

The monthly scheme partitions data by the month, using the layout ``yyyy/MM``.

Julian Schemes
^^^^^^^^^^^^^^

**Names:** ``julian-minute``, ``julian-hourly``, ``julian-daily``

Julian schemes partition data by Julian day, instead of month/day. They use the patterns ``yyyy/DDD/HH/mm``,
``yyyy/DDD/HH``, and ``yyyy/DDD`` respectively

Spatial Schemes
---------------

Spatial schemes lay out data based on a space-filling curve. All spatial schemes support the following common
configuration option:

* ``geom-attribute`` - The name of a ``Geometry``\ -type attribute from the SimpleFeatureType to use for
  partitioning data. If not specified, the default geometry is used.

Z2 Scheme
^^^^^^^^^

**Name:** ``z2``

**Configuration:**

* ``z2-resolution`` - The number of bits of precision to use for z indexing. Must be a power of 2.

The Z2 scheme uses a Z2 space-filling curve, and can only be used with Point-type geometries. Instead of specifying
the resolution as a configuration option, it may be specified in the name, as ``z2-<n>bits``, where ``<n>`` is
replaced with the Z2 resolution, e.g. ``z2-2bits``.

XZ2 Scheme
^^^^^^^^^^

**Name:** ``xz2``

**Configuration:**

* ``xz2-resolution`` - The number of bits of precision to use for z indexing. Must be a power of 2.

The XZ2 scheme uses an XZ2 space-filling curve, and can be used with any geometry type. Instead of specifying
the resolution as a configuration option, it may be specified in the name, as ``xz2-<n>bits``, where ``<n>`` is
replaced with the XZ2 resolution, e.g. ``xz2-2bits``.

Attribute Schemes
-----------------

Attribute schemes lay out data based on a lexicoded attribute value.

**Name:** ``attribute``

**Configuration:**

* ``partitioned-attribute`` - The name of an attribute from the SimpleFeatureType to use for partitioning data.
