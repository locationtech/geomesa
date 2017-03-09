Analytic Querying
=================

GeoMesa provides advanced query capabilities through GeoTools query hints. You can use these hints to control
various aspects of query processing, or to trigger distributed analytic processing. See :ref:`query_hints`
for details on setting query hints.

.. _analytic_queries:

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

========================== ================================== ====================
Key                        Type                               GeoServer Conversion
========================== ================================== ====================
QueryHints.SAMPLING        Float                              any float
QueryHints.SAMPLE_BY       String - attribute name (optional) any string
========================== ================================== ====================

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

    .. tab:: GeoServer

        ``...&viewparams=SAMPLING:0.1``

Density Query
-------------

To populate heatmaps or other pre-rendered maps, GeoMesa can use server-side aggregation to map features to
pixels. This results in much less network traffic, and subsequently much faster queries.

The result from a density query is an encoded iterator of ``(x, y, count)``, where ``x`` and ``y`` refer to
the coordinates for the center of a pixel.
In GeoServer, you can use the WPS DensityProcess to create a heatmap from the query result.
See :ref:`gdelt_heatmaps` for more information.

========================= ======================= ==============================
Key                       Type                    GeoServer Conversion
========================= ======================= ==============================
QueryHints.DENSITY_BBOX   ``ReferencedEnvelope``  use WPS
QueryHints.DENSITY_WEIGHT String                  use WPS
QueryHints.DENSITY_WIDTH  Integer                 use WPS
QueryHints.DENSITY_HEIGHT Integer                 use WPS
========================= ======================= ==============================

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
