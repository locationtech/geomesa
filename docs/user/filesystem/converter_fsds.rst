Converter Mode
==============

The normal use-case for the FileSystem data store is to ingest data into it in the same way as any other database. However,
the data store also supports reading arbitrary data files that may come from some other process, using :ref:`converters`, as
long as they meet a few criteria. To use this mode, specify ``fs.encoding`` as ``converter`` when creating a data store.

Note that converter mode is read-only.

Configuration
-------------

Converter mode requires several properties to be specified in the data store configuration. These can be set using
the :ref:`fsds_parameters` ``fs.config.properties`` and ``fs.config.file``.

``fs.options.converter.path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property must point to the root path containing the files to read.

``fs.options.sft.name``
^^^^^^^^^^^^^^^^^^^^^^^

This property may contain a well-known feature type name, to be loaded from the classpath.

``fs.options.sft.conf``
^^^^^^^^^^^^^^^^^^^^^^^

This property may contain a full feature type definition.

``fs.options.converter.name``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property may contain a well-known converter name, to be loaded from the classpath.

``fs.options.converter.conf``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property may contain a full converter definition.

``fs.options.leaf-storage``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Leaf storage controls the final layout of files and folders. When using leaf storage, the last component of the partition path
is used as a prefix to the data file name, instead of as a separate folder. This can result in less directory overhead for
filesystems such as S3.

As an example, a partition scheme of ``yyyy/MM/dd`` would produce a partition path like ``2016/01/01``. With
leaf storage, the data files for that partition would be ``2016/01/01_<datafile>.parquet``. If leaf storage is
disabled, the data files would be ``2016/01/01/<datafile>.parquet``, creating an extra level of directories.

``fs.partition-scheme.name``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Comma-delimited list of partition schemes used by the files. Additional partition scheme options can be configured by
prefixing them with ``fs.partition-scheme.opts.``.

Path Filters
------------

The FSDS can filter paths within a partition for more granular control of queries. Path filtering is configured in the feature
type through the user data key ``geomesa.fs.path-filter.name``.

Currently, the only implementation is the ``dtg`` path filter, whose purpose is to parse a datetime from the given
path and compare it to the query filter to include or exclude the file from the query. The following options are
required for the ``dtg`` path filter, configured through the key ``geomesa.fs.path-filter.opts``:

* ``attribute`` - The ``Date`` attribute in the query to compare against.
* ``pattern`` - The regular expression, with a single capturing group, to extract a datetime string from the path.
* ``format`` - The datetime formatting pattern to parse a date from the regex capture.
* ``buffer`` - The duration to buffer the bounds of the parsed datetime by within the current partition. To buffer time
  across partitions, see the ``receipt-time`` partition scheme.

Custom path filters can be loaded via SPI.

Hierarchical Temporal Partitioning
----------------------------------

The standard temporal partition schemes supported by the FileSystem data store are somewhat opaque, as they correspond
to a number of days (or weeks/years/etc) since the Unix epoch (1970/01/01). The converter mode supports an additional
temporal scheme that uses standard date formatting, which may be easier to use with external processes. Note that where
the names overlap, the hierarchical schemes will take precedence over the standard partition schemes (when using the converter
store).

Custom Scheme
^^^^^^^^^^^^^

**Name:** ``datetime``

**Configuration:**

* ``datetime-format`` - A Java `DateTime format string <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html>`__,
  separated by forward slashes, which will be used to build a directory structure. For example, ``yyyy/MM/dd``.
* ``step-unit`` - A ``java.time.temporal.ChronoUnit`` defining how to increment the leaf of the partition scheme
* ``step`` - The amount to increment the leaf of the partition scheme. If not specified, defaults to ``1``

The date-time scheme provides a fully customizable temporal scheme.

Hourly
^^^^^^

**Name:** ``hourly``

The hourly scheme partitions data by the hour, using the layout ``yyyy/MM/dd/HH``.

Minute
^^^^^^

**Name:** ``minute``

The minute scheme partitions data by the minute, using the layout ``yyyy/MM/dd/HH/mm``.

Daily
^^^^^

**Name:** ``daily``

The daily scheme partitions data by the day, using the layout ``yyyy/MM/dd``.

Weekly
^^^^^^

**Name:** ``weekly``

The weekly scheme partitions data by the week, using the layout ``yyyy/ww``.

Monthly
^^^^^^^

**Name:** ``monthly``

The monthly scheme partitions data by the month, using the layout ``yyyy/MM``.

Julian
^^^^^^

**Names:** ``julian-minute``, ``julian-hourly``, ``julian-daily``

Julian schemes partition data by Julian day, instead of month/day. They use the patterns ``yyyy/DDD/HH/mm``,
``yyyy/DDD/HH``, and ``yyyy/DDD`` respectively

Receipt Time
^^^^^^^^^^^^

**Name:** ``receipt-time``

**Configuration:**

* ``datetime-scheme`` - The name of another date-time scheme describing the layout of the data, e.g. ``weekly`` or
  ``hourly``. Additional options may be required to configure the date-time scheme selected.
* ``buffer`` - The amount of time to buffer queries by, expressed as a duration, e.g. ``30 minutes``. This represents
  the latency in the system.

The receipt time scheme partitions data based on when a message is received. Generally this is useful
only for reading existing data that may have been aggregated and stored by an external process.