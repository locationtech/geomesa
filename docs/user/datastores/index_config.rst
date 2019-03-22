.. _index_config:

Index Configuration
===================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.

.. _set_sft_options:

Setting Schema Options
----------------------

Static properties of a ``SimpleFeatureType`` must be set when calling ``createSchema``, and can't be changed
afterwards. Most properties are controlled through user-data values, either on the ``SimpleFeatureType``
or on a particular attribute. Setting the user data can be done in multiple ways.

If you are using a string to indicate your ``SimpleFeatureType`` (e.g. through the command line tools,
or when using ``SimpleFeatureTypes.createType``), you can append the type-level options to the end of
the string, like so:

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    // append the user-data values to the end of the string, separated by a semi-colon
    String spec = "name:String,dtg:Date,*geom:Point:srid=4326;option.one='foo',option.two='bar'";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``,
you may set the values directly in the feature type:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getUserData().put("option.one", "foo");

If you are using TypeSafe configuration files to define your simple feature type, you may include
a 'user-data' key:

.. code-block:: javascript

    geomesa {
      sfts {
        "mySft" = {
          attributes = [
            { name = name, type = String             }
            { name = dtg,  type = Date               }
            { name = geom, type = Point, srid = 4326 }
          ]
          user-data = {
            option.one = "foo"
          }
        }
      }
    }

.. _attribute_options:

Setting Attribute Options
-------------------------

In addition to schema-level user data, each attribute also has user data associated with it. Just like
the schema options, attribute user data can be set in multiple ways.

If you are using a string to indicate your ``SimpleFeatureType`` (e.g. through the command line tools,
or when using ``SimpleFeatureTypes.createType``), you can append the attribute options after the attribute type,
separated with a colon:

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    // append the user-data after the attribute type, separated by a colon
    String spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4326";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``,
you may set the user data directly in the attribute descriptor:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getDescriptor("name").getUserData().put("index", "true");

If you are using TypeSafe configuration files to define your simple feature type, you may add user data keys
to the attribute elements:

.. code-block:: javascript

    geomesa {
      sfts {
        "mySft" = {
          attributes = [
            { name = name, type = String, index = true }
            { name = dtg,  type = Date                 }
            { name = geom, type = Point, srid = 4326   }
          ]
        }
      }
    }

.. _set_date_attribute:

Setting the Indexed Date Attribute
----------------------------------

For schemas that contain a date attribute, GeoMesa will use the attribute as part of the primary Z3/XZ3 index.
If a schema contains more than one date attribute, you may specify which attribute to use through the user-data
key ``geomesa.index.dtg``. If you would prefer to not index any date, you may disable it through the key
``geomesa.ignore.dtg``. If nothing is specified, the first declared date attribute will be used.


.. code-block:: java

    // specify the attribute 'myDate' as the indexed date
    sft1.getUserData().put("geomesa.index.dtg", "myDate");

    // disable indexing by date
    sft2.getUserData().put("geomesa.ignore.dtg", true);

.. _customizing_index_creation:

Customizing Index Creation
--------------------------

Instead of using the default indices, you may specify the exact indices to create. This may be used to create
fewer indices (to speed up ingestion, or because you are only using certain query patterns), or to create additional
indices (for example on non-default geometries or dates).

The indices are created when calling ``createSchema``. If nothing is specified, the Z2, Z3 (or XZ2 and XZ3
depending on geometry type) and ID indices will all be created, as well as any attribute indices you have defined.

.. warning::

    Certain queries may be much slower if you disable an index.

To configure the indices, you may set a user data value in your simple feature type. The user data key is
``geomesa.indices.enabled``, and it should contain a comma-delimited list containing a subset of index
identifiers, as specified in :ref:`index_overview`.

In addition to specifying which types of indices to create, you may optionally specify the exact attributes that will
be used in each index, by appending them with ``:``\ s after the index name. The following example shows two index
configurations. The first configuration has a single Z3 index that includes the default attributes. The second
configuration has two Z3 indices on different geometries, as well as an attribute index on name which includes
a secondary index on dtg.

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    String spec = "name:String,dtg:Date,*start:Point:srid=4326,end:Point:srid=4326";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);
    // enable a default z3 index on start + dtg
    sft.getUserData().put("geomesa.indices.enabled", "z3");
    // alternatively, enable a z3 index on start + dtg, end + dtg, and an attribute index on
    // name with a secondary index on dtg. note that this overrides the previous configuration
    sft.getUserData().put("geomesa.indices.enabled", "z3:start:dtg,z3:end:dtg,attr:name:dtg");

See :ref:`set_sft_options` for details on setting user data. If you are using the GeoMesa ``SchemaBuilder``,
you may instead call the ``indexes`` methods:

.. code-block:: scala

    import org.locationtech.geomesa.utils.geotools.SchemaBuilder

    val sft = SchemaBuilder.builder()
        .addString("name")
        .addDate("dtg")
        .addPoint("geom", default = true)
        .userData
        .indices(List("id", "z3", "attr"))
        .build("mySft")

Configuring Feature ID Encoding
-------------------------------

While feature IDs can be any string, a common use case is to use UUIDs. A UUID is a globally unique, specially
formatted string composed of hex characters in the format ``{8}-{4}-{4}-{4}-{12}``, for example
``28a12c18-e5ae-4c04-ae7b-bf7cdbfaf234``. A UUID can also be considered as a 128 bit number, which can
be serialized in a smaller size.

You can indicate that feature IDs are UUIDs using the user data key ``geomesa.fid.uuid``. If set before
calling ``createSchema``, then feature IDs will be serialized as 16 byte numbers instead of 36 byte strings,
saving some overhead:

.. code-block:: java

    sft.getUserData().put("geomesa.fid.uuid", "true");
    datastore.createSchema(sft);

If the schema is already created, you may still retroactively indicate that feature IDs are UUIDs, but you
**must also indicate** that they are not serialized that way using ``geomesa.fid.uuid-encoded``. This may still
provide some benefit when exporting data in certain formats (e.g. Arrow):

.. code-block:: java

    SimpleFeatureType existing = datastore.getSchema("existing");
    existing.getUserData().put("geomesa.fid.uuid", "true");
    existing.getUserData().put("geomesa.fid.uuid-encoded", "false");
    datastore.updateSchema("existing", existing);

.. warning::

    Ensure that you use valid UUIDs if you indicate that you are using them. Otherwise you will experience
    exceptions writing and/or reading data.

Configuring Geometry Serialization
----------------------------------

By default, geometries are serialized using a modified version of the well-known binary (WKB) format. Alternatively,
geometries may be serialized using the
`tiny well-known binary (TWKB) <https://github.com/TWKB/Specification/blob/master/twkb.md>`__ format. TWKB will be
smaller on disk, but does not allow full double floating point precision. For point geometries, TWKB will take
4-12 bytes (depending on the precision specified), compared to 18 bytes for WKB. For line strings, polygons, or
other geometries with multiple coordinates, the space savings will be greater due to TWKB's delta encoding scheme.

For any geometry type attribute, TWKB serialization can be enabled by setting the floating point precision
through the ``precision`` user-data key. Precision indicates the number of decimal places that will be stored, and
must be between -7 and 7, inclusive. A negative precision can be used to indicate rounding of whole numbers to the
left of the decimal place. For reference, 6 digits of latitude/longitude precision can store a resolution of
approximately 10cm.

For geometries with more than two dimensions, the precision of the Z and M dimensions may be specified separately.
Generally these dimensions do not need to be stored with the same resolution as X/Y. By default, Z will
be stored with precision 1, and M with precision 0. To change this, specify the additional precisions after
the X/Y precision, separated with commas. For example, ``6,1,0`` would set the X/Y precision to 6, the Z
precision to 1 and the M precision to 0. Z and M precisions must be between 0 and 7, inclusive.

TWKB serialization can be set when creating a new schema, but can also be enabled at any time through the
``updateSchema`` method. If modifying an existing schema, any data already written will not be updated.

.. code-block:: java

    SimpleFeatureType sft = ...
    sft.getDescriptor("geom").getUserData().put("precision", "4");

See :ref:`attribute_options` for details on how to set attribute options.

Configuring Column Groups
-------------------------

For back-ends that support it (currently HBase and Accumulo), subsets of attributes may be replicated into
separate column groups. When possible, only the reduced column groups will be scanned for a query, which avoids
having to read unused data from disk. For schemas with a large number of attributes, this can speed up some queries,
at the cost of writing more data to disk.

Column groups are specified per attribute, using attribute-level user data. An attribute may belong to multiple
column groups, in which case it will be replicated multiple times. All attributes will belong to the default
column group without having to specify anything. See :ref:`attribute_options` for details on how to set attribute
options.

Column groups are specified using the user data key ``column-groups``, with the value being a comma-delimited list
of groups for that attribute. It is recommended to keep column group names short (ideally a single character), in
order to minimize disk usage. If a column group conflicts with one of the default groups used by GeoMesa, it
will throw an exception when creating the schema. Currently, the reserved groups are ``d`` for HBase and
``F``, ``A``, ``I``, and ``B`` for Accumulo.

.. tabs::

    .. code-tab:: java

        SimpleFeatureType sft = ...
        sft.getDescriptor("name").getUserData().put("column-groups", "a,b");

    .. code-tab:: scala spec

        import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
        // for java, use org.locationtech.geomesa.utils.interop.SimpleFeatureTypes

        val spec = "name:String:column-groups=a,dtg:Date:column-groups='a,b',*geom:Point:srid=4326:column-groups='a,b'"
        SimpleFeatureTypes.createType("mySft", spec)

    .. code-tab:: javascript config

        geomesa {
          sfts {
            "mySft" = {
              attributes = [
                { name = "name", type = "String", column-groups = "a"                }
                { name = "dtg",  type = "Date",   column-groups = "a,b"              }
                { name = "geom", type = "Point",  column-groups = "a,b", srid = 4326 }
              ]
            }
          }
        }

    .. code-tab:: scala SchemaBuilder

        import org.locationtech.geomesa.utils.geotools.SchemaBuilder

        val sft = SchemaBuilder.builder()
            .addString("name").withColumnGroups("a")
            .addDate("dtg").withColumnGroups("a", "b")
            .addPoint("geom", default = true).withColumnGroups("a", "b")
            .build("mySft")

.. _configuring_z_shards:

Configuring Z-Index Shards
--------------------------

GeoMesa allows configuration of the number of shards (or splits) into which the Z2/Z3/XZ2/XZ3 indices are
divided. This parameter may be changed individually for each ``SimpleFeatureType``. If nothing is specified,
GeoMesa will default to 4 shards. The number of shards must be between 1 and 127.

Shards allow us to pre-split tables, which provides some initial parallelism for reads and writes. As more data is
written, tables will generally split based on size, thus obviating the need for explicit shards. For small data sets,
shards are more important as the tables might never split due to size. Setting the number of shards too high can
reduce performance, as it requires more calculations to be performed per query.

The number of shards is set when calling ``createSchema``. It may be specified through the simple feature type
user data using the key ``geomesa.z.splits``. See :ref:`set_sft_options` for details on setting user data.

.. code-block:: java

    sft.getUserData().put("geomesa.z.splits", "4");

.. _customizing_z_index:

Configuring Z-Index Time Interval
---------------------------------

GeoMesa uses a z-curve index for time-based queries. By default, time is split into week-long chunks and indexed
per week. If your queries are typically much larger or smaller than one week, you may wish to partition at a
different interval. GeoMesa provides four intervals - ``day``, ``week``, ``month`` or ``year``. As the interval
gets larger, fewer partitions must be examined for a query, but the precision of each interval will go down.

If you typically query months of data at a time, then indexing per month may provide better performance.
Alternatively, if you typically query minutes of data at a time, indexing per day may be faster. The default
per week partitioning tends to provides a good balance for most scenarios. Note that the optimal partitioning
depends on query patterns, not the distribution of data.

The time interval is set when calling ``createSchema``. It may be specified through the simple feature type
user data using the key ``geomesa.z3.interval``.  See :ref:`set_sft_options` for details on setting user data.

.. code-block:: java

    sft.getUserData().put("geomesa.z3.interval", "month");

.. _customizing_xz_index:

Configuring XZ-Index Precision
------------------------------

GeoMesa uses an extended z-curve index for storing geometries with extents. The index can be customized
by specifying the resolution level used to store geometries. By default, the resolution level is 12. If
you have very large geometries, you may want to lower this value. Conversely, if you have very small
geometries, you may want to raise it.

The resolution level for an index is set when calling ``createSchema``. It may be specified through
the simple feature type user data using the key ``geomesa.xz.precision``.  See :ref:`set_sft_options` for
details on setting user data.

.. code-block:: java

    sft.getUserData().put("geomesa.xz.precision", 12);

For more information on resolution level (g), see
"XZ-Ordering: A Space-Filling Curve for Objects with Spatial Extension" by Böhm, Klump and Kriegel.

.. _configuring_attr_shards:

Configuring Attribute Index Shards
----------------------------------

GeoMesa allows configuration of the number of shards (or splits) into which the attribute indices are
divided. This parameter may be changed individually for each ``SimpleFeatureType``. If nothing is specified,
GeoMesa will default to 4 shards. The number of shards must be between 1 and 127.

See :ref:`configuring_z_shards` for more background on shards.

The number of shards is set when calling ``createSchema``. It may be specified through the simple feature type
user data using the key ``geomesa.attr.splits``. See :ref:`set_sft_options` for details on setting user data.

.. code-block:: java

    sft.getUserData().put("geomesa.attr.splits", "4");

.. _cardinality_config:

Configuring Attribute Cardinality
---------------------------------

GeoMesa allows attributes to be marked as either high or low cardinality. If set, this hint will be used in
query planning. For more information, see :ref:`attribute_cardinality`.

To set the cardinality of an attribute, use the key ``cardinality`` on the attribute, with a value of
``high`` or ``low``.

.. tabs::

    .. code-tab:: java

        SimpleFeatureType sft = ...
        sft.getDescriptor("name").getUserData().put("index", "true");
        sft.getDescriptor("name").getUserData().put("cardinality", "high");

    .. code-tab:: scala SchemaBuilder

        import org.locationtech.geomesa.utils.geotools.SchemaBuilder
        import org.locationtech.geomesa.utils.stats.Cardinality

        val sft = SchemaBuilder.builder()
            .addString("name").withIndex(Cardinality.HIGH)
            .addDate("dtg")
            .addPoint("geom", default = true)
            .build("mySft")

.. _partitioned_indices:

Configuring Partitioned Indices
-------------------------------

To help with large data sets, GeoMesa can partition each index into separate tables, based on the attributes of
each feature. Having multiple tables for a single index can make it simpler to manage a cluster, for example by
making it trivial to delete old data. This functionality is currently supported in HBase, Accumulo and Cassandra.

Partitioning must be specified through user data when creating a simple feature type, before calling
``createSchema``. To indicate a partitioning scheme, use the key ``geomesa.table.partition``. Currently
the only valid value is ``time``, to indicate time-based partitioning:

.. tabs::

    .. code-tab:: java

        sft.getUserData().put("geomesa.table.partition", "time");

    .. code-tab:: scala SchemaBuilder

        import org.locationtech.geomesa.utils.geotools.SchemaBuilder

        val sft = SchemaBuilder.builder()
            .addString("name")
            .addDate("dtg")
            .addPoint("geom", default = true)
            .userData
            .partitioned()
            .build("mySft")

Note that to enable partitioning the schema must contain a default date field.

When partitioning is enabled, each index will consist of multiple physical tables. The tables are partitioned
based on the Z-interval (see :ref:`customizing_z_index`). Tables are created dynamically when needed.

Partitioned tables can still be pre-split, as described in :ref:`table_split_config`. For Z3 splits, the min/max
date configurations are automatically determined by the partition, and do not need to be specified.

When a query must scan multiple tables, by default the tables will be scanned sequentially. To instead scan
the tables in parallel, set the sytem property ``geomesa.partition.scan.parallel=true``. Note that when enabled,
queries that span many partitions may place a large load on the system.

The GeoMesa command line tools provide functions for managing partitions; see :ref:`manage_partitions_cli`
for details.

.. _table_split_config:

Configuring Index Splits
------------------------

When planning to ingest large amounts of data, if the distribution is known up front, it can be useful to
pre-split tables before writing. This provides parallelism across a cluster from the start, and doesn't
depend on implementation triggers (which typically split tables based on size).

Splits are managed through implementations of the ``org.locationtech.geomesa.index.conf.TableSplitter`` interface.

Specifying a Table Splitter
^^^^^^^^^^^^^^^^^^^^^^^^^^^

A table splitter may be specified through user data when creating a simple feature type, before calling
``createSchema``.

To indicate the table splitter class, use the key ``table.splitter.class``:

.. code-block:: java

    sft.getUserData().put("table.splitter.class", "org.example.CustomSplitter");

To indicate any options for the given table splitter, use the key ``table.splitter.options``:

.. code-block:: java

    sft.getUserData().put("table.splitter.options", "foo,bar,baz");

See :ref:`set_sft_options` for details on setting user data.

The Default Table Splitter
^^^^^^^^^^^^^^^^^^^^^^^^^^

Generally, ``table.splitter.class`` can be omitted. If so, GeoMesa will use a default implementation that allows
for a flexible configuration using ``table.splitter.options``. If no options are specified, then all tables
will generally create 4 split (based on the number of shards). The default ID index splits assume
that feature IDs are randomly distributed UUIDs.

For the default splitter, ``table.splitter.options`` should consist of comma-separated entries, in the form
``key1:value1,key2:value2``. Entries related to a given index should start with the index identifier, e.g. one
of ``id``, ``z3``, ``z2`` or ``attr`` (``xz3`` and ``xz2`` indices use ``z3`` and ``z2``, respectively).

+------------+-------------------------------+----------------------------------------+
| Index      | Option                        | Description                            |
+============+===============================+========================================+
| Z3/XZ3     | ``z3.min``                    | The minimum date for the data          |
+            +-------------------------------+----------------------------------------+
|            | ``z3.max``                    | The maximum date for the data          |
+            +-------------------------------+----------------------------------------+
|            | ``z3.bits``                   | The number of leading bits to split on |
+------------+-------------------------------+----------------------------------------+
| Z2/XZ2     | ``z2.bits``                   | The number of leading bits to split on |
+------------+-------------------------------+----------------------------------------+
| ID         | ``id.pattern``                | Split pattern                          |
+------------+-------------------------------+----------------------------------------+
| Attribute  |  ``attr.<attribute>.pattern`` | Split pattern for a given attribute    |
+------------+-------------------------------+----------------------------------------+

Z3/XZ3 Splits
+++++++++++++

Dates are used to split based on the Z3 time prefix (typically weeks). They are specified in the form
``yyyy-MM-dd``. If the minimum date is specified, but the maximum date is not, it will default to the current date.
After the dates, the Z value can be split based on a number of bits (note that due to the index format, bits can
not be specified without dates). For example, specifying two bits would create splits 00, 01, 10 and 11. The total
number of splits created will be ``<number of z shards> * <number of time periods> * 2 ^ <number of bits>``.

Z2/XZ2 Splits
+++++++++++++

If any options are given, the number of bits must be specified. For example, specifying two bits would create
splits 00, 01, 10 and 11. The total number of splits created will be ``<number of z shards> * 2 ^ <number of bits>``.

ID and Attribute Splits
+++++++++++++++++++++++

Splits are defined by patterns. For an ID index, the pattern(s) are applied to the single feature ID. For an
attribute index, each attribute that is indexed can be configured separately, by specifying the attribute name
as part of the option. For example, given the schema ``name:String:index=true,*geom:Point:srid=4326``, the
``name`` attribute splits can be configured with ``attr.name.pattern``.

Patterns consist of one or more single characters or ranges enclosed in square brackets. Valid characters
can be any of the numbers 0 to 9, or any letter a to z, in upper or lower case. Ranges are two characters
separated by a dash. Each set of brackets corresponds to a single character, allowing for nested splits.

For example, the pattern ``[0-9]`` would create 10 splits, based on the numbers 0 through 9. The
pattern ``[0-9][0-9]`` would create 100 splits. The pattern ``[0-9a-f]`` would create 16 splits based on lower-case
hex characters. The pattern ``[0-9A-F]`` would do the same with upper-case characters.

For data hot-spots, multiple patterns can be specified by adding additional options with a 2, 3, etc appended to
the key. For example, if most of the ``name`` values start with the letter ``f`` and ``t``, splits could be
specified as ``attr.name.pattern:[a-z],attr.name.pattern2:[f][a-z],attr.name.pattern3:[t][a-z]``

For number-type attributes, only numbers are considered valid characters. Due to lexicoding, normal number
prefixing will not work correctly. E.g., if the data has numbers in the range 8000-9000, specifying
``[8-9][0-9]`` will not split the data properly. Instead, trailing zeros should be added to reach the appropriate
length, e.g. ``[8-9][0-9][0][0]``.

Full Example
++++++++++++

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    String spec = "name:String:index=true,age:Int:index=true,dtg:Date,*geom:Point:srid=4326";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("foo", "spec");
    sft.getUserData().put("table.splitter.options",
        "id.pattern:[0-9a-f],attr.name.pattern:[a-z],z3.min:2018-01-01,z3.max:2018-01-31,z3.bits:2,z2.bits:4");

.. _query_interceptors:

Configuring Query Interceptors
------------------------------

GeoMesa provides a chance for custom logic to be applied to a query before executing it. Query interceptors must
be specified through user data in the simple feature type, and may be set before calling ``createSchema``, or
updated by calling ``updateSchema``. To indicate query interceptors, use the key ``geomesa.query.interceptors``:

.. code-block:: java

    sft.getUserData().put("geomesa.query.interceptors", "com.example.MyQueryInterceptor");

The value must be a comma-separated string consisting of the names of one or more classes implementing
the trait ``org.locationtech.geomesa.index.planning.QueryInterceptor``:

.. code-block:: scala

    /**
      * Provides a hook to modify a query before executing it
      */
    trait QueryInterceptor extends Closeable {

      /**
        * Called exactly once after the interceptor is instantiated
        *
        * @param ds data store
        * @param sft simple feature type
        */
      def init(ds: DataStore, sft: SimpleFeatureType): Unit

      /**
        * Modifies the query in place
        *
        * @param query query
        */
      def rewrite(query: Query): Unit
    }

Interceptors must have a default, no-arg constructor. The interceptor lifecycle consists of:

1. The instance is instantiated via reflection, using its default constructor
#. The instance is initialized via the ``init`` method, passing in the data store containing the simple feature type
#. ``rewrite`` is called repeatedly
#. The instance is cleaned up via the ``close`` method

Interceptors will be invoked in the order they are declared in the user data. In order to see detailed information
on the results of query interceptors, you can enable ``TRACE``-level logging on the class
``org.locationtech.geomesa.index.planning.QueryRunner$``.

.. _stat_attribute_config:

Configuring Cached Statistics
-----------------------------

GeoMesa allows for collecting summary statistics for attributes during ingest, which are then stored and
available for instant querying. Hints are set on individual attributes using the key ``keep-stats``, as
described in :ref:`attribute_options`.

.. note::

    Cached statistics are currently only implemented for the Accumulo data store

Stats are always collected for the default geometry, default date and any indexed attributes. See
:ref:`stats_collected` for more details. In addition, any other attribute can be flagged for stats. This
will cause the following stats to be collected for those attributes:

* Min/max (bounds)
* Top-k

Only attributes of the following types can be flagged for stats: ``String``, ``Integer``, ``Long``,
``Float``, ``Double``, ``Date`` and ``Geometry``.

For example:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getDescriptor("name").getUserData().put("keep-stats", "true");

See :ref:`cli_analytic` and :ref:`stats_api` for information on reading cached stats.

Mixed Geometry Types
--------------------

A common pitfall is to unnecessarily specify a generic geometry type when creating a schema.
Because GeoMesa relies on the geometry type for indexing decisions, this can negatively impact performance.

If the default geometry type is ``Geometry`` (i.e. supporting both point and non-point
features), you must explicitly enable "mixed" indexing mode. All other geometry types (``Point``,
``LineString``, ``Polygon``, etc) are not affected.

Mixed geometries must be declared when calling ``createSchema``. It may be specified through
the simple feature type user data using the key ``geomesa.mixed.geometries``.  See :ref:`set_sft_options` for
details on setting user data.

.. code-block:: java

    sft.getUserData().put("geomesa.mixed.geometries", "true");
