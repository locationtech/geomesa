Query Planning
--------------

Query planning in GeoMesa consists of several different steps:

1. The CQL filter (if any) is re-written and optimized for fast evaluation
2. The CQL filter is split apart to match against the available indices
3. One of the available indices is selected to execute the query
4. A logical query plan is created by the core GeoMesa indexing code
5. A physical query plan is created for the particular back-end database

Filter Decomposition
^^^^^^^^^^^^^^^^^^^^

A logical query plan in GeoMesa generally consists of a 'primary' CQL filter, which is used to determine scan
ranges, and a 'secondary' CQL filter which is applied to matching rows. For example, the Z2 index will handle
any spatial predicates as scan ranges, and any additional filters will be applied afterwards.

During step two, the full filter is decomposed and examined with an eye towards the available indices. For
each index, a primary and a secondary filter will be determined (if any).

For example, consider the filter
``BBOX(geom,0,0,10,10) AND dtg DURING 2017-01-01T00:00:00.000Z/2017-01-02T00:00:00.000Z AND name = 'alice'``.
This filter can be decomposed several ways - for the Z2 spatial index, the primary filter is the ``BBOX``, for
the Z3 spatio-temporal index, the primary filter is the ``BBOX`` plus the ``DURING``, and for the attribute index
(assuming 'name' is indexed) the primary filter is ``name = 'alice'``.

Index Selection
^^^^^^^^^^^^^^^

Since skipping rows entirely is much faster than reading and filtering them, the best query plan will generally
be the one that scans the fewest rows. In other words, the best plan is the one that has the most selective
primary filter. GeoMesa has two methods for determining the best index - stat-based, or heuristic-based. Stats
are used when available; however the method used can be configured per-query. See :ref:`query_index_hint` and
:ref:`query_planning_hint` for more information.

Statistical Selection
+++++++++++++++++++++

.. note::

    Cached statistics, and thus stat-based query planning, are currently only implemented for the Accumulo data store

.. _stats_collected:

GeoMesa will collect stats during ingestion, and store them for use in query planning. The stats collected are:

* Total count
* Min/max (bounds) for default geometry, default date and any indexed attributes
* Histogram for default geometry, default date and any indexed attributes
* Frequency for any indexed attributes, split up by week
* Top-k for any indexed attributes
* Z3 histogram based on the default geometry and default date (if both present)

These stats are used to estimate the number of features matching a given primary filter. The primary filter that
matches the fewest features will be selected.

Heuristic Selection
+++++++++++++++++++

Static cost - equals 100, range 250
    *
    * high cardinality: / 10
    * low cardinality: * 10
    *
    * Compare with id at 1, z3 at 200, z2 at 400, date z3 401


.. _attribute_cardinality:

Cardinality Hints
^^^^^^^^^^^^^^^^^

GeoMesa has a query planner that tries to find the best strategy for answering a given query. In
general, this means using the index that will filter the result set the most, before considering
the entire query filter on the reduced data set. For simple queries, there is often only one
suitable index. However, for mixed queries, there can be multiple options.

For example, given the query ``bbox(geom, -120, -60, 120, 60) AND IN('id-01')``, we could try to
execute against the spatial index using the bounding box, or we could try to execute against the
ID index using the feature ID. In this case, we know that the ID filter will match at most one
record, while the bbox filter could match many records, so we will choose the ID index.

Attributes that are know to have many distinct values, i.e. a high cardinality, are likely to filter
out many false positives through the index structure, and thus a query against the attribute index will
touch relatively few records. Conversely, in the worst case, a Boolean attribute (for example), with only
two distinct values, would likely require scanning half of the entire data set.

Cardinality hints may be used to influence the query planner when considering attribute indices.
If an attribute is marked as having a high cardinality, the attribute index will be prioritized.
Conversely, if an attribute is marked with low cardinality, the attribute index will be de-prioritized.


.. _explain_query:

Explaining Query Plans
----------------------

GeoMesa will automatically log explain plans during query execution. This can be useful when debugging
query issues, and can inform decisions to speed up execution time, such as when to add additional indices
or when query hints may be helpful.

In order to show explain logging, configure your logging system to set
``org.locationtech.geomesa.index.utils.Explainer`` to ``trace`` level. For example, in log4j use:

.. code-block:: bash

    log4j.category.org.locationtech.geomesa.index.utils.Explainer=TRACE

Instead of passively logging, you can also generate explain logging explicitly without actually executing a query.
Given a GeoMesa data store and a query, use the following method:

.. code-block:: scala

    import org.locationtech.geomesa.index.utils.ExplainString

    dataStore.getQueryPlan(query, explainer = new ExplainPrintln)

``ExplainPrintln`` will write to ``System.out``. Alternatively, you can use ``ExplainString`` or
``ExplainLogging`` to redirect the output elsewhere.

Using the binary distribution, you can print out an explain plan using the ``explain`` command. See
:ref:`cli_explain` for more details.

GeoServer
^^^^^^^^^

For enabling explain loggingn in GeoServer, see :ref:`geoserver_explain_query`.
