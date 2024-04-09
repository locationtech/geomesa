.. _analytic_queries:

Analytic Querying
=================

GeoMesa provides advanced query capabilities through GeoTools query hints. You can use these hints to control
various aspects of query processing or to trigger distributed analytic processing. See :ref:`query_hints`
for details on setting query hints.

.. _feature_sampling:

Feature Sampling
----------------

Instead of returning all features for a query, GeoMesa can use statistical sampling to return a certain
percentage of results. This can be useful when rendering maps, or when there are too many features to
be meaningful.

Features can either be sampled absolutely, or sampled by a certain attribute. For example, given a series of
points in a track, you may wish to sample by the track identifier so that no tracks are completely sampled out.

The sampling value should be a float in the range (0, 1), which represents the fractional value of features that will
be returned. Due to distributed processing, the actual count returned is not guaranteed to equal the requested
percentage - however, there will never be less features than requested. For example, if you sample 5 features
at 10%, you will get back anywhere from 1 to 5 features, depending on how your data is distributed in the cluster.

Sampling can also be combined with the other analytic queries mentioned below.

+----------------------+------------------------------------+----------------------+
| Key                  | Type                               | GeoServer Conversion |
+======================+====================================+======================+
| QueryHints.SAMPLING  | Float                              | any float            |
+----------------------+------------------------------------+----------------------+
| QueryHints.SAMPLE_BY | String - attribute name (optional) | any string           |
+----------------------+------------------------------------+----------------------+

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.index.conf.QueryHints;

        // returns 10% of features, threaded by 'track' attribute
        query.getHints().put(QueryHints.SAMPLING(), new Float(0.1));
        query.getHints().put(QueryHints.SAMPLE_BY(), "track");

    .. code-tab:: scala

        import org.locationtech.geomesa.index.conf.QueryHints

        // returns 10% of features, threaded by 'track' attribute
        query.getHints.put(QueryHints.SAMPLING, 0.1f)
        query.getHints().put(QueryHints.SAMPLE_BY, "track");

    .. code-tab:: none GeoServer

        ...&viewparams=SAMPLING:0.1

Density Queries
---------------

To populate heatmaps or other pre-rendered maps, GeoMesa can use server-side aggregation to map features to
pixels. This results in much less network traffic, and subsequently much faster queries.

The result from a density query is an encoded iterator of ``(x, y, count)``, where ``x`` and ``y`` refer to
the coordinates for the center of a pixel. In GeoServer, you can use the WPS DensityProcess to create a
heatmap from the query result. See :ref:`gdelt_heatmaps` for more information.

+---------------------------+------------------------+----------------------+
| Key                       | Type                   | GeoServer Conversion |
+===========================+========================+======================+
| QueryHints.DENSITY_BBOX   | ``ReferencedEnvelope`` | Use WPS              |
+---------------------------+------------------------+                      +
| QueryHints.DENSITY_GEOM   | String                 |                      |
+---------------------------+------------------------+                      +
| QueryHints.DENSITY_WEIGHT | String                 |                      |
+---------------------------+------------------------+                      +
| QueryHints.DENSITY_WIDTH  | Integer                |                      |
+---------------------------+------------------------+                      +
| QueryHints.DENSITY_HEIGHT | Integer                |                      |
+---------------------------+------------------------+----------------------+

.. tabs::

    .. code-tab:: scala

        import org.geotools.api.data.Transaction
        import org.geotools.geometry.jts.ReferencedEnvelope
        import org.geotools.referencing.CRS
        import org.locationtech.geomesa.index.conf.QueryHints
        import org.locationtech.geomesa.index.iterators.DensityScan

        val bounds = new ReferencedEnvelope(-120.0, -110.0, 45.0, 55.0, CRS.decode("EPSG:4326"))
        query.getHints.put(QueryHints.DENSITY_BBOX, bounds)
        query.getHints.put(QueryHints.DENSITY_WIDTH, 500)
        query.getHints.put(QueryHints.DENSITY_HEIGHT, 500)

        val reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)
        try {
          val decode = DensityScan.decodeResult(bounds, 500, 500)
          while (reader.hasNext) {
            val pts = decode(reader.next())
            while (pts.hasNext) {
              val (x, y, weight) = pts.next()
              // do something with the cell
            }
          }
        } finally {
          reader.close()
        }


.. _statistical_queries:

Statistical Queries
-------------------

GeoMesa supports generating various statistics against a data set. These stats are generated in a distributed
scan, so provide built-in parallelism and require less network traffic. The following stats are supported:

* count
* min/max values (bounds)
* enumeration of values
* top-k values
* frequency of values
* histogram of values
* `descriptive statistics <https://en.wikipedia.org/wiki/Descriptive_statistics>`__

In GeoServer you can use the ``StatsProcess``. Otherwise, the query is controlled through the
following query hints:

+-------------------------------------+--------------------+----------------------+
| Key                                 | Type               | GeoServer Conversion |
+=====================================+====================+======================+
| QueryHints.STATS_STRING             | String             | Use WPS              |
+-------------------------------------+--------------------+                      +
| QueryHints.ENCODE_STATS             | Boolean (optional) |                      |
+-------------------------------------+--------------------+----------------------+

.. tabs::

    .. code-tab:: scala

        import org.geotools.api.data.Transaction
        import org.locationtech.geomesa.index.conf.QueryHints
        import org.locationtech.geomesa.index.iterators.StatsScan
        import org.locationtech.geomesa.utils.stats.Stat

        query.getHints.put(QueryHints.STATS_STRING, "Count()")
        query.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)

        val reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)

        val result: Stat = try {
          // stats should always return exactly one result, even if there are no features in the table
          StatsScan.decodeStat(sft)(reader.next.getAttribute(0).asInstanceOf[String])
        } finally {
          reader.close()
        }

See :ref:`cli_analytic` for information on running statistical queries through the GeoMesa command-line
tools.

Explanation of Hints
++++++++++++++++++++

STATS_STRING
^^^^^^^^^^^^

This hint is a string describing the stats to be collected. Each type of stat has a corresponding string
representation. Multiple stats can be collected at once by delimiting them with a semi-colon. Instead
of constructing stat strings by hand, there are convenience methods in ``org.locationtech.geomesa.utils.stats.Stat``
that will generate valid stat strings. Stat strings can be validated by trying to parse them with
``org.locationtech.geomesa.utils.stats.Stat.apply``. The implementing classes are contained in the package
``org.locationtech.geomesa.utils.stats``.

Stat strings are as follows:

========================== =================== =======================================================
Type                       Implementation      Representation
========================== =================== =======================================================
count                      CountStat           ``Count()``
min/max                    MinMax              ``MinMax("foo")``
enumeration                EnumerationStat     ``Enumeration("foo")``
top-k                      TopK                ``TopK("foo")``
frequency                  Frequency           ``Frequency("foo",<precision>)``
frequency (by time period) Frequency           ``Frequency("foo","dtg",<time period>,<precision>)``
Z3 frequency               Z3Frequency         ``Z3Frequency("geom","dtg",<time period>,<precision>)``
histogram                  Histogram           ``Histogram("foo",<bins>,<min>,<max>)``
Z3 histogram               Z3Histogram         ``Z3Histogram("geom","dtg",<time period>,<bins>)``
descriptive statistics     DescriptiveStats    ``DescriptiveStats("foo","bar")``
multiple stats             SeqStat             ``Count();MinMax("foo")``
grouped stats              GroupBy             ``GroupBy("foo",MinMax("bar"))``
========================== =================== =======================================================

As seen in the table above, multiple stats can be calculated at once through semi-colon delimiting. In addition,
stats can be calculated on grouped values by using ``GroupBy`` on a nested stat expression.

The Z3 frequency and histogram are special stats that will operate on the Z3 value created from the geometry and date.

``<time period>`` can be one of ``day``, ``week``, ``month``, or ``year``, and indicates how data should be grouped.

The ``<precision>`` for frequencies is defined as:

* for geometry and Z3 types, it is the number of bits of z-index to keep (max of 64). Note that the first 2
  bits do not hold any information
* for date types, it is the number of milliseconds to group for binning
* for number types, it is the number of digits that will be grouped together
* for floating point types, it is the number of decimal places that will be considered
* for string types, it is the number of characters that will be considered

The ``<bins>`` for a histogram indicate how many groupings should be made. The ``<min>`` and ``<max>`` values
set the initial sizes of the groupings, but are not hard limits. The histogram will expand if needed as
new values are added, but some precision may be lost.

ENCODE_STATS
^^^^^^^^^^^^

This hint controls whether the stat will be returned as a serialized (encoded) object, or as a JSON string.
Serialized stats can be deserialized using an instance of ``org.locationtech.geomesa.utils.stats.StatSerializer``,
obtained through its factory ``apply`` method.

.. _stats_api:

Accessing Stats through the GeoMesa API
+++++++++++++++++++++++++++++++++++++++

In addition to queries through the GeoTools API, stats can be accessed directly through the GeoMesa API. Most
GeoMesa datastores implement ``org.locationtech.geomesa.index.stats.HasGeoMesaStats``, which defines a single method::

    def stats: org.locationtech.geomesa.index.stats.GeoMesaStats

In addition to running queries, the ``GeoMesaStats`` interface can be used to retrieve cached stats.
See :ref:`stat_config` for details on configuring cached stats.

.. _arrow_encoding:

Arrow Encoding
--------------

GeoMesa supports returning features as `Apache Arrow <https://arrow.apache.org/>`__ encoded vectors. This provides
an optimized columnar memory layout for fast processing and interoperability with other systems.

The result of an Arrow query will be an iterator of SimpleFeatures, where the first attribute of each will be a
byte array. Concatenated together, the byte arrays will form an Arrow file, in the Arrow streaming format
(i.e. no footer).

In GeoServer you can use the ``ArrowConversionProcess``, or through WFS by setting
``outputFormat=application/vnd.arrow`` and controlling the configuration through the ``format_options`` parameter,
e.g. ``format_options=includeFids:true;batchSize:1000``. Otherwise, the encoding is controlled through the
following query hints:

+-------------------------------------+--------------------+------------------------------------+
| Key                                 | Type               | GeoServer Format Option            |
+=====================================+====================+====================================+
| QueryHints.ARROW_ENCODE             | Boolean            | outputFormat=application/vnd.arrow |
+-------------------------------------+--------------------+------------------------------------+
| QueryHints.ARROW_INCLUDE_FID        | Boolean (optional) | includeFids                        |
+-------------------------------------+--------------------+------------------------------------+
| QueryHints.ARROW_PROXY_FID          | Boolean (optional) | proxyFids                          |
+-------------------------------------+--------------------+------------------------------------+
| QueryHints.ARROW_SORT_FIELD         | String (optional)  | sortField                          |
+-------------------------------------+--------------------+------------------------------------+
| QueryHints.ARROW_SORT_REVERSE       | Boolean (optional) | sortReverse                        |
+-------------------------------------+--------------------+------------------------------------+
| QueryHints.ARROW_DICTIONARY_FIELDS  | String (optional)  | dictionaryFields                   |
+-------------------------------------+--------------------+------------------------------------+
| QueryHints.ARROW_BATCH_SIZE         | Integer (optional) | batchSize                          |
+-------------------------------------+--------------------+------------------------------------+
| QueryHints.ARROW_FORMAT_VERSION     | String (optional)  | formatVersion                      |
+-------------------------------------+--------------------+------------------------------------+
| QueryHints.ARROW_PROCESS_DELTAS     | Boolean (optional) | processDeltas                      |
+-------------------------------------+--------------------+------------------------------------+

Explanation of Hints
++++++++++++++++++++

ARROW_ENCODE
^^^^^^^^^^^^

This hint is used to trigger an Arrow query.

ARROW_INCLUDE_FID
^^^^^^^^^^^^^^^^^

This hint controls whether to include the feature ID as an Arrow vector or not. The default is to include it.

ARROW_PROXY_FID
^^^^^^^^^^^^^^^

This hint controls whether to return the full feature ID, or a 4-byte proxy ID. Proxy IDs can be used
for callbacks by using the ``proxyID()`` CQL filter function.

ARROW_SORT_FIELD
^^^^^^^^^^^^^^^^

This hint allows for sorting the results by a particular attribute. Only attribute names are supported, not
arbitrary CQL.

ARROW_SORT_REVERSE
^^^^^^^^^^^^^^^^^^

This hint is used to flip sort order from normal (ascending) to reverse (descending).

ARROW_DICTIONARY_FIELDS
^^^^^^^^^^^^^^^^^^^^^^^

This hint indicates which simple feature attributes should be dictionary encoded. It should be a comma-separated
list of attribute names.

ARROW_BATCH_SIZE
^^^^^^^^^^^^^^^^

This hint will restrict the number of features included in each Arrow record batch. An Arrow file contains
a series of record batches - limiting the max size of each batch can allow memory-constrained systems to
operate more easily.

ARROW_FORMAT_VERSION
^^^^^^^^^^^^^^^^^^^^

This hint controls the IPC format version for Arrow binary encoding. It should be a valid Arrow format version,
i.e. ``0.16`` or ``0.10``. The Arrow IPC format changed slightly starting with version ``0.15``.

ARROW_PROCESS_DELTAS
^^^^^^^^^^^^^^^^^^^^

This is an advanced hint, which can be used to disable normal processing on Arrow queries. When set to false,
data will be returned in a custom binary format, and needs to be processed before it can be read by standard
Arrow libraries. When returned un-processed, data can begin streaming back to the client immediately.

Example Query
+++++++++++++

.. tabs::

    .. code-tab:: scala

        import java.io.ByteArrayOutputStream
        import org.geotools.api.data.Transaction
        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.ARROW_ENCODE, java.lang.Boolean.TRUE)

        val reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)
        val os = new ByteArrayOutputStream()

        while (reader.hasNext) {
          os.write(reader.next().getAttribute(0).asInstanceOf[Array[Byte]])
        }
        reader.close()

        // use ArrowStreamReader or other Arrow libraries to process bytes


Binary Encoding
---------------

GeoMesa supports returning features in a custom binary format (referred to as BIN) that uses 16 or 24 bytes
per feature. This provides an extremely compact representation of a few key attributes.

The 16 byte BIN format is as follows::

    <4 byte int><4 byte int><4 byte floating point><4 byte floating point>

The first integer is referred to as a track ID, and is generally used to group related points. For example,
a line string may be turned into several BIN records with a common track ID. The second integer is a date
represented as the number of seconds since the Java epoch (Jan. 1, 1970). The two floating point numbers
represent the latitude and longitude of the record, respectively.

The 24 byte BIN format is the same as the 16 byte version, but with an additional 8 bytes at the end for
arbitrary data.

The result of a BIN query will be an iterator of SimpleFeatures, where the first attribute of each will be a
byte array containing one or more BIN-encoded features.

In GeoServer you can use the ``BinConversionProcess``. Otherwise, the encoding is controlled through the
following query hints:

+---------------------------+--------------------+----------------------+
| Key                       | Type               | GeoServer Conversion |
+===========================+====================+======================+
| QueryHints.BIN_TRACK      | String             | Use WPS              |
+---------------------------+--------------------+                      +
| QueryHints.BIN_GEOM       | String (optional)  |                      |
+---------------------------+--------------------+                      +
| QueryHints.BIN_DTG        | String (optional)  |                      |
+---------------------------+--------------------+                      +
| QueryHints.BIN_LABEL      | String (optional)  |                      |
+---------------------------+--------------------+                      +
| QueryHints.BIN_SORT       | Boolean (optional) |                      |
+---------------------------+--------------------+                      +
| QueryHints.BIN_BATCH_SIZE | Integer (optional) |                      |
+---------------------------+--------------------+----------------------+

Explanation of Hints
++++++++++++++++++++

BIN_TRACK
^^^^^^^^^

This hint is used to trigger a BIN query. It should be the name of an attribute that will be used to
generate the track ID for each record.

BIN_GEOM
^^^^^^^^

This hint controls the geometry attribute used for each record. If omitted, the default geometry of the
feature type is used.

BIN_DTG
^^^^^^^

This hint controls the date attribute used for each record. If omitted, the default date of the feature type
is used.

BIN_LABEL
^^^^^^^^^

This hint will trigger the creation of 24-byte records, instead of the standard 16. It should be the
name of an attribute that will be used to generate the label for each record.

BIN_SORT
^^^^^^^^

This hint will cause the records to be sorted. It should be the name of an attribute in the feature type.

BIN_BATCH_SIZE
^^^^^^^^^^^^^^

This hint controls the batch size used when generating BIN records.

Example Query
+++++++++++++

.. tabs::

    .. code-tab:: scala

        import java.io.ByteArrayOutputStream
        import org.geotools.api.data.Transaction
        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.BIN_TRACK, "name")

        val reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)
        val os = new ByteArrayOutputStream()

        while (reader.hasNext) {
          os.write(reader.next().getAttribute(0).asInstanceOf[Array[Byte]])
        }
        reader.close()

        // process bytes appropriately
