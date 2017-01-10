Analytic Querying
=================

GeoMesa provides advanced query capabilities through GeoTools query hints. You can use these hints to control
various aspects of query processing, or to trigger distributed analytic processing.

.. _analytic_queries:

Setting Query Hints
-------------------

Query hints can be set in two ways - programmatically or through GeoServer requests.

Programmatic Hints
^^^^^^^^^^^^^^^^^^

To set a hint directly in a query:

.. code-block:: java

    import org.geotools.data.Query;

    Query query = new Query("typeName");
    query.getHints().put(key, value);

Note that query hint values must match the class type of the query hint. See below for available hints and their types.

GeoServer Hints
^^^^^^^^^^^^^^^

To set a hint through GeoServer, modify your query URL to use the ``viewparams`` request parameter:

.. code-block:: none

    ...&viewparams=key1:value1;key2:value2;

Hint values will be converted into the appropriate types. See below for available hints and their accepted values.

Loose Bounding Box
------------------

By default, GeoMesa uses a less precise filter for primary bounding box queries. This is faster, and in most cases
will not change the results returned. However, certain use-cases require an exact result. This can be set
at the data store configuration level, or overridden per query.

===================== =========== =====================
Key                   Type        GeoServer Conversion
===================== =========== =====================
QueryHints.LOOSE_BBOX ``Boolean`` ``true`` or ``false``
===================== =========== =====================

Java
^^^^

.. code-block:: java

    import org.locationtech.geomesa.index.conf.QueryHints;

    query.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);

Scala
^^^^^

.. code-block:: scala

    import org.locationtech.geomesa.index.conf.QueryHints

    query.getHints.put(QueryHints.LOOSE_BBOX, false)

GeoServer
^^^^^^^^^

.. code-block:: none

    ...&viewparams=LOOSE_BBOX:false;

Exact Counts
------------

By default, GeoMesa uses an estimate for counts. In certain cases, users may require exact counts. This may
be set through the system property ``geomesa.force.count`` or per query. Note, however, that exact counts
are expensive to calculate.

====================== =========== =====================
Key                    Type        GeoServer Conversion
====================== =========== =====================
QueryHints.EXACT_COUNT ``Boolean`` ``true`` or ``false``
====================== =========== =====================

Java
^^^^

.. code-block:: java

    import org.locationtech.geomesa.index.conf.QueryHints;

    query.getHints().put(QueryHints.EXACT_COUNT(), Boolean.TRUE);

Scala
^^^^^

.. code-block:: scala

    import org.locationtech.geomesa.index.conf.QueryHints

    query.getHints.put(QueryHints.EXACT_COUNT, true)

GeoServer
^^^^^^^^^

.. code-block:: none

    ...&viewparams=EXACT_COUNT:true;

Query Index
-----------

GeoMesa may be able to use several different indices to satisfy a particular query. For example,
a query with a spatial filter and an attribute filter could potentially use either the primary
spatial index or the attribute index. GeoMesa uses cost-based query planning to pick the best index;
however, the index can be overridden if desired.

====================== ======================= ===========================
Key                    Type                    GeoServer Conversion
====================== ======================= ===========================
QueryHints.QUERY_INDEX ``GeoMesaFeatureIndex`` index name, or name:version
====================== ======================= ===========================

Java
^^^^

.. code-block:: java

    import org.locationtech.geomesa.accumulo.index.z2.Z2Index$;
    import org.locationtech.geomesa.index.conf.QueryHints;

    query.getHints().put(QueryHints.QUERY_INDEX(), Z2Index$.MODULE$);

Scala
^^^^^

.. code-block:: scala

    import org.locationtech.geomesa.accumulo.index.z2.Z2Index
    import org.locationtech.geomesa.index.conf.QueryHints

    query.getHints.put(QueryHints.QUERY_INDEX, Z2Index)

GeoServer
^^^^^^^^^

.. code-block:: none

    ...&viewparams=QUERY_INDEX:z2;

Query Planning
--------------

As explained above, GeoMesa uses cost-based query planning to determine the best index for a given query.
If cost-based query planning is not working as desired, the legacy heuristic-based query
planning can be used as a fall-back. ``Stats`` uses cost-based planning; ``Index`` uses heuristic-based planning.

========================== ================== ======================
Key                        Type               GeoServer Conversion
========================== ================== ======================
QueryHints.COST_EVALUATION ``CostEvaluation`` ``stats`` or ``index``
========================== ================== ======================

Java
^^^^

.. code-block:: java

    import org.locationtech.geomesa.index.api.QueryPlanner.CostEvaluation;
    import org.locationtech.geomesa.index.conf.QueryHints;

    query.getHints().put(QueryHints.COST_EVALUATION(), CostEvaluation.Index());

Scala
^^^^^

.. code-block:: scala

    import org.locationtech.geomesa.index.api.QueryPlanner.CostEvaluation
    import org.locationtech.geomesa.index.conf.QueryHints

    query.getHints.put(QueryHints.COST_EVALUATION, CostEvaluation.Index)

GeoServer
^^^^^^^^^

.. code-block:: none

    ...&viewparams=COST_EVALUATION:index;

Feature Sampling
----------------

Instead of returning all features for a query, GeoMesa can use statistical sampling to return a certain
percentage of results. This can be useful when rendering maps, or when there are too many features to
be meaningful.

.. note::

    Currently this section applies only to the Accumulo Data Store.

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

Java
^^^^

.. code-block:: java

    import org.locationtech.geomesa.index.conf.QueryHints;

    // returns 10% of features, threaded by 'track' attribute
    query.getHints().put(QueryHints.SAMPLING(), new Float(0.1));
    query.getHints().put(QueryHints.SAMPLE_BY(), "track");

Scala
^^^^^

.. code-block:: scala

    import org.locationtech.geomesa.index.conf.QueryHints

    // returns 10% of features, threaded by 'track' attribute
    query.getHints.put(QueryHints.SAMPLING, 0.1f)
    query.getHints().put(QueryHints.SAMPLE_BY, "track");

GeoServer
^^^^^^^^^

.. code-block:: none

    ...&viewparams=SAMPLING:0.1

Density Query
-------------

To populate heatmaps or other pre-rendered maps, GeoMesa can use server-side aggregation to map features to
pixels. This results in much less network traffic, and subsequently much faster queries.

The result from a density query is an encoded iterator of ``(x, y, count)``, where ``x`` and ``y`` refer to
the coordinates for the center of a pixel.
In GeoServer, you can use the WPS DensityProcess to create a heatmap from the query result.
See :ref:`gdelt_heatmaps` for more information.

.. note::

    Currently this section applies only to the Accumulo Data Store.

========================= ======================= ==============================
Key                       Type                    GeoServer Conversion
========================= ======================= ==============================
QueryHints.DENSITY_BBOX   ``ReferencedEnvelope``  use WPS
QueryHints.DENSITY_WEIGHT String                  use WPS
QueryHints.DENSITY_WIDTH  Integer                 use WPS
QueryHints.DENSITY_HEIGHT Integer                 use WPS
========================= ======================= ==============================

Scala
^^^^^

.. code-block:: scala

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
