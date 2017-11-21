.. _analytic_queries:

Analytic Querying
=================

GeoMesa provides advanced query capabilities through GeoTools query hints. You can use these hints to control
various aspects of query processing, or to trigger distributed analytic processing. See :ref:`query_hints`
for details on setting query hints.

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

Density Query
-------------

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
| QueryHints.DENSITY_WEIGHT | String                 |                      |
+---------------------------+------------------------+                      +
| QueryHints.DENSITY_WIDTH  | Integer                |                      |
+---------------------------+------------------------+                      +
| QueryHints.DENSITY_HEIGHT | Integer                |                      |
+---------------------------+------------------------+----------------------+

.. tabs::

    .. code-tab:: scala

        import org.geotools.data.Transaction
        import org.geotools.geometry.jts.ReferencedEnvelope.ReferencedEnvelope
        import org.geotools.referencing.CRS
        import org.locationtech.geomesa.accumulo.iterators.KryoLazyDensityIterator
        import org.locationtech.geomesa.index.conf.QueryHints

        val bounds = new ReferencedEnvelope(-120.0, -110.0, 45.0, 55.0, CRS.decode("EPSG:4326"))
        query.getHints.put(QueryHints.DENSITY_BBOX, bounds)
        query.getHints.put(QueryHints.DENSITY_WIDTH, 500)
        query.getHints.put(QueryHints.DENSITY_HEIGHT, 500)

        val reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)

        val decode = KryoLazyDensityIterator.decodeResult(bounds, 500, 500)

        while (reader.hasNext) {
            val pts = decode(reader.next())
            while (pts.hasNext) {
                val (x, y, weight) = pts.next()
                // do something with the cell
            }
        }
        reader.close()

Arrow Encoding
--------------

GeoMesa supports returning features as `Apache Arrow <https://arrow.apache.org/>`__ encoded vectors. This provides
an optimized columnar memory layout for fast processing and interoperability with other systems.

The result of an Arrow query will be an iterator of SimpleFeatures, where the first attribute of each will be a
byte array. Concatenated together, the byte arrays will form an Arrow file, in the Arrow streaming format
(i.e. no footer).

In GeoServer you can use the ``ArrowConversionProcess``. Otherwise, the encoding is controlled through the
following query hints:

+-------------------------------------+--------------------+----------------------+
| Key                                 | Type               | GeoServer Conversion |
+=====================================+====================+======================+
| QueryHints.ARROW_ENCODE             | Boolean            | Use WPS              |
+-------------------------------------+--------------------+                      +
| QueryHints.ARROW_INCLUDE_FID        | Boolean (optional) |                      |
+-------------------------------------+--------------------+                      +
| QueryHints.ARROW_DICTIONARY_FIELDS  | String (optional)  |                      |
+-------------------------------------+--------------------+                      +
| QueryHints.ARROW_DICTIONARY_VALUES  | String (optional)  |                      |
+-------------------------------------+--------------------+                      +
| QueryHints.ARROW_DICTIONARY_COMPUTE | Boolean (optional) |                      |
+-------------------------------------+--------------------+                      +
| QueryHints.ARROW_BATCH_SIZE         | Integer (optional) |                      |
+-------------------------------------+--------------------+----------------------+

Explanation of Hints
++++++++++++++++++++

ARROW_ENCODE
^^^^^^^^^^^^

This hint is used to trigger an Arrow query.

ARROW_INCLUDE_FID
^^^^^^^^^^^^^^^^^

This hint controls whether to include the feature ID as an Arrow vector or not. The default is to include it.

ARROW_DICTIONARY_FIELDS
^^^^^^^^^^^^^^^^^^^^^^^

This hint indicates which simple feature attributes should be dictionary encoded. It should be a comma-separated
list of attribute names.

ARROW_DICTIONARY_VALUES
^^^^^^^^^^^^^^^^^^^^^^^

This hint indicates known dictionary values to use for encoding each field. This allows for specifying a known
dictionary up front, which means the dictionary doesn't have to be computed. Values which are not indicated
in the dictionary will be grouped under 'other'.

The hint should be an encoded map of attribute names to attribute values. The hint should be encoded in
comma-separated values format, where each line indicates a different attribute. The first item in each line is
the attribute name, and the subsequent items are dictionary values. Standard CSV escaping can be used. The function
``org.locationtech.geomesa.utils.text.StringSerialization.encodeSeqMap`` can be used to encode a map of values.

.. tabs::

    .. code-tab:: scala

        import org.locationtech.geomesa.index.conf.QueryHints
        import org.locationtech.geomesa.utils.text.StringSerialization.encodeSeqMap

        val dictionaries1 =
            """
              |name,Harry,Hermione,Severus
              |age,20,25,30
            """.stripMargin.trim

        // equivalent to dictionaries1
        val dictionaries2 = encodSeqMap(Map("name" -> Seq("Harry", "Hermione", "Severus"), "age" -> Seq(20, 25, 30)))

        query.getHints.put(QueryHints.ARROW_DICTIONARY_VALUES, dictionaries1)

ARROW_DICTIONARY_COMPUTE
^^^^^^^^^^^^^^^^^^^^^^^^

This hint indicates that dictionaries should be computed before running the query. Any provided dictionaries
will not be computed. Dictionary values will use cached statistics (top-k) if available, otherwise will run
a statistical query. Note that this may be slow.

If this hint is false, any dictionary fields will be determined on the fly. However, this means that instead
of a single Arrow file, the result of the query will be multiple separate arrow files, concatenated together.
This is a restriction of the Arrow format, which requires that dictionaries be specified before anything else.

ARROW_BATCH_SIZE
^^^^^^^^^^^^^^^^

This hint will restrict the number of features included in each Arrow record batch. An Arrow file contains
a series of record batches -limiting the max size of each batch can allow memory-constrained systems to
operate more easily.

Example Query
+++++++++++++

.. tabs::

    .. code-tab:: scala

        import java.io.ByteArrayOutputStream
        import org.geotools.data.Transaction
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
name of an attribute that will be used to general the label for each record.

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
        import org.geotools.data.Transaction
        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.BIN_TRACK, "name")

        val reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)
        val os = new ByteArrayOutputStream()

        while (reader.hasNext) {
          os.write(reader.next().getAttribute(0).asInstanceOf[Array[Byte]])
        }
        reader.close()

        // process bytes appropriately
