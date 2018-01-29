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

Customizing Index Creation
--------------------------

To speed up ingestion, or because you are only using certain query patterns, you may disable some indices.
The indices are created when calling ``createSchema``. If nothing is specified, the Z2/Z3 (or XZ2/XZ3
depending on geometry type) indices and record indices will all be created, as well as any attribute
indices you have defined.

.. warning::

    Certain queries may be much slower if you disable an index.

To enable only certain indices, you may set a user data value in your simple feature type. The user data key is
``geomesa.indices.enabled``, and it should contain a comma-delimited list containing a subset of index
identifiers, as specified in :ref:`index_overview`.

See :ref:`set_sft_options` for details on setting user data. If you are using the GeoMesa ``SftBuilder``,
you may instead call the ``withIndexes`` methods:

.. code-block:: scala

    // scala example
    import org.locationtech.geomesa.utils.geotools.SftBuilder.SftBuilder

    val sft = new SftBuilder()
        .stringType("name")
        .date("dtg")
        .geometry("geom", default = true)
        .withIndexes(List("id", "z3", "attr"))
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

.. code-block:: java

    SimpleFeatureType sft = ...
    sft.getDescriptor("name").getUserData().put("index", "true");
    sft.getDescriptor("name").getUserData().put("cardinality", "high");

If you are using the GeoMesa ``SftBuilder``, you may call the overloaded attribute methods:

.. code-block:: scala

    // scala example
    import org.locationtech.geomesa.utils.geotools.SftBuilder.SftBuilder
    import org.locationtech.geomesa.utils.stats.Cardinality

    val sft = new SftBuilder()
        .stringType("name", Opts(index = true, cardinality = Cardinality.HIGH))
        .date("dtg")
        .geometry("geom", default = true)
        .build("mySft")

.. _table_split_config:

Configuring Index Splits
------------------------

When planning to ingest large amounts of data, if the distribution is known up front, it can be useful to
pre-split tables before writing. This provides parallelism across a cluster from the start, and doesn't
depend on implementation triggers (which typically split tables based on size).

Splits are managed through implementations of the ``org.locationtech.geomesa.index.conf.TableSplitter`` interface.

Specifying a Table Splitter
^^^^^^^^^^^^^^^^^^^^^^^^^^^

A table splitter may be specified by through user data when creating a simple feature type, before calling
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
| Index Type | Option                        | Description                            |
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
