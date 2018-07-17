Kudu Index Configuration
========================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
This section contains Kudu-specific options; general options can be found under :ref:`index_config`.

Default Index Creation
----------------------

By default GeoMesa will only create a single Z3 (or XZ3) index for Kudu. Due to predicate push-downs,
partition pruning and other advanced optimizations, queries on non-indexed fields are still quite fast.
To create additional indices, specify them explicitly. See :ref:`customizing_index_creation` for details.

Note that because they do not have an incrementing time field in the primary key, other indices may eventually
run out of space. Initial partitioning is especially important in this case (see next).

Table Partitioning
------------------

Partitioning is import in Kudu, and GeoMesa supports both Kudu hash and range partitions. See
:ref:`configuring_z_shards` and :ref:`configuring_attr_shards` for details on configuring shards, which will be
translated to Kudu hash partitions. See :ref:`table_split_config` for details on configuring table splits,
which will be translated to Kudu range partitions. By default, the Z3/XZ3 index will create a new range partition
for every time period (week by default).

See the `Kudu documentation <http://kudu.apache.org/docs/schema_design.html#partitioning>`__ for more details on Kudu
partitioning.

Column Encoding
---------------

Kudu has multiple ways to encode data. Each column (attribute) in a schema is encoded separately. The best
encoding will depend on the data being written; for example low-cardinality string columns work will with
dictionary encoding. If nothing is specified, then the Kudu default encoding will be used based on the attribute
type.

Valid encodings depend on the attribute type:

============================== ============================== ===========
Attribute Type                 Encodings                      Default
============================== ============================== ===========
Integer, Long, Date            plain, bit shuffle, run length bit shuffle
Float, Double                  plain, bit shuffle             bit shuffle
Boolean                        plain, run length              run length
String, Bytes, UUID, List, Map plain, prefix, dictionary      dictionary
Point                          plain, bit shuffle             bit shuffle
Non-point geometry             plain, prefix, dictionary      dictionary
============================== ============================== ===========

The encoding must be specified by its enumeration:

========== ====================
Encoding   Keyword
========== ====================
plain      ``PLAIN_ENCODING``
prefix     ``PREFIX_ENCODING``
run length ``RLE``
dictionary ``DICT_ENCODING``
bitshuffle ``BIT_SHUFFLE``
========== ====================

Encodings are set in the attribute user data. See :ref:`attribute_options` for more information on how
to configure attributes.

.. tabs::

    .. group-tab:: Java

        .. code-block:: java

            import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

            SimpleFeatureTypes.createType("example", "name:String:encoding=DICT_ENCODING");

    .. group-tab:: Scala

        .. code-block:: scala

            import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

            SimpleFeatureTypes.createType("example", "name:String:encoding=DICT_ENCODING")

    .. group-tab:: Config

        .. code-block:: javascript

            geomesa = {
              sfts = {
                example = {
                  attributes = [
                    { name = "name", type = "String", encoding = "DICT_ENCODING" }
                  ]
                }
              }
            }

See the `Kudu documentation <http://kudu.apache.org/docs/schema_design.html#encoding>`__ for more information.

.. _kudu_column_compression:

Column Compression
------------------

Kudu also allows compression on a per-column basis. Compression may be one of ``NO_COMPRESSION``, ``SNAPPY``,
``LZ4``, or ``ZLIB``. If not specified, GeoMesa will default to ``LZ4``.

Note that columns that are bit-shuffle encoded are compressed as part of the bit-shuffle algorithm, so it
is not recommended to compress them further. GeoMesa will ignore attempts to do so.

Compressions are set in the attribute user data. See :ref:`attribute_options` for more information on how
to configure attributes.

.. tabs::

    .. group-tab:: Java

        .. code-block:: java

            import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

            SimpleFeatureTypes.createType("example", "name:String:compression=SNAPPY");

    .. group-tab:: Scala

        .. code-block:: scala

            import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

            SimpleFeatureTypes.createType("example", "name:String:compression=SNAPPY")

    .. group-tab:: Config

        .. code-block:: javascript

            geomesa = {
              sfts = {
                example = {
                  attributes = [
                    { name = "name", type = "String", compression = "SNAPPY" }
                  ]
                }
              }
            }

See the `Kudu documentation <http://kudu.apache.org/docs/schema_design.html#compression>`__ for more information.
