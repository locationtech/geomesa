Query Properties
================

GeoMesa provides advanced query capabilities through GeoTools query hints. You can use these hints to control
various aspects of query processing, or to trigger distributed analytic processing.

.. _query_hints:

Setting Query Hints
-------------------

Query hints can be set in two ways - programmatically or through GeoServer requests.

Programmatic Hints
^^^^^^^^^^^^^^^^^^

To set a hint directly in a query:

.. code-block:: java

    import org.geotools.api.data.Query;

    Query query = new Query("typeName");
    query.getHints().put(key, value);

Note that query hint values must match the class type of the query hint. See below for available hints and their types.

GeoServer Hints
^^^^^^^^^^^^^^^

To set a hint through GeoServer, modify your query URL to use the ``viewparams`` request parameter:

.. code-block:: none

    ...&viewparams=key1:value1;key2:value2;

Hint values will be converted into the appropriate types. See below for available hints and their accepted values.

Command-Line Tools
^^^^^^^^^^^^^^^^^^

When exporting features through the GeoMesa command line tools, query hints can be set using the ``--hints`` parameter.
Hints should be specified in the form ``key1=value1;key2=value2``. Hints are converted to the appropriate type
according to the conventions for GeoServer hints.

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


.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.index.conf.QueryHints;

        query.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);

    .. code-tab:: scala

        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.LOOSE_BBOX, false)

    .. code-tab:: none GeoServer

        ...&viewparams=LOOSE_BBOX:false

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

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.index.conf.QueryHints;

        query.getHints().put(QueryHints.EXACT_COUNT(), Boolean.TRUE);

    .. code-tab:: scala

        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.EXACT_COUNT, true)

    .. code-tab:: none GeoServer

        ...&viewparams=EXACT_COUNT:true

Filter Compatibility
--------------------

GeoMesa provides a limited compatibility mode, which allows for using a newer client version with
an older distributed-runtime version, for back-ends that have a distributed installation. Currently
this is limited to GeoMesa 1.3.7 and 2.3.x, with the flags ``1.3`` and ``2.3``, respectively. Only certain
basic queries are supported, without advanced options.

======================== ======================= ===========================
Key                      Type                    GeoServer Conversion
======================== ======================= ===========================
QueryHints.FILTER_COMPAT String                  distributed install version
======================== ======================= ===========================

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.index.conf.QueryHints;

        query.getHints().put(QueryHints.FILTER_COMPAT(), "1.3");

    .. code-tab:: scala

        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.FILTER_COMPAT, "1.3")

    .. code-tab:: none GeoServer

        ...&viewparams=FILTER_COMPAT:1.3

.. _query_index_hint:

Query Index
-----------

GeoMesa may be able to use several different indices to satisfy a particular query. For example,
a query with a spatial filter and an attribute filter could potentially use either the primary
spatial index or the attribute index. GeoMesa will try to pick the best index; however, the index
can be specified directly if desired.

====================== ======================= ===========================
Key                    Type                    GeoServer Conversion
====================== ======================= ===========================
QueryHints.QUERY_INDEX String                  index name or identifier
====================== ======================= ===========================

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.index.conf.QueryHints;

        query.getHints().put(QueryHints.QUERY_INDEX(), "z2");
        // or:
        query.getHints().put(QueryHints.QUERY_INDEX(), "z2:5:geom");

    .. code-tab:: scala

        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.QUERY_INDEX, "z2")
        // or:
        query.getHints.put(QueryHints.QUERY_INDEX, "z2:5:geom")

    .. code-tab:: none GeoServer

        ...&viewparams=QUERY_INDEX:z2

For more details, see :ref:`query_planning`.

.. _query_planning_hint:

Query Planning Type
-------------------

GeoMesa uses cost-based query planning to determine the best index for a given query.
By default, heuristics are used to pick the index. This method is quite fast, but may not always account for
unusual data distributions. If heuristic-based query planning is not working as desired, stat-based query
planning can be used, based on data statistics gathered during ingestion. ``Stats`` uses cost-based planning;
``Index`` uses heuristic-based planning. Note that currently, statistics have only been implemented for the
Accumulo and Redis data stores - for other stores, heuristic-based planning will always be used.

Query planning can also be controlled through the system property ``geomesa.query.cost.type``. See
:ref:`geomesa_site_xml` for details. If both a query hint and a system property are set, the query hint will
take precedence.

========================== ================== ======================
Key                        Type               GeoServer Conversion
========================== ================== ======================
QueryHints.COST_EVALUATION ``CostEvaluation`` ``index`` or ``stats``
========================== ================== ======================


.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation;
        import org.locationtech.geomesa.index.conf.QueryHints;

        query.getHints().put(QueryHints.COST_EVALUATION(), CostEvaluation.Index());

    .. code-tab:: scala

        import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.COST_EVALUATION, CostEvaluation.Index)

    .. code-tab:: none GeoServer

        ...&viewparams=COST_EVALUATION:index

See :ref:`query_planning` for more information on query planning strategies.
