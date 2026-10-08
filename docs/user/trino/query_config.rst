Trino Query Properties
======================

GeoMesa provides advanced query capabilities through GeoTools query hints. You can use these hints to control
various aspects of query processing, or to trigger distributed analytic processing. For an overview of query
hints, see :ref:`query_hints`.

.. _trino_variant_prefilter_hint:

Trino VARIANT Prefilter
-----------------------

This hint enables or disables a necessary SQL prefilter for case-sensitive string equality
on supported object paths into schemaless JSON attributes stored as VARIANT. The original predicate still runs
client-side to preserve GeoTools matching semantics. Other data stores do not use this hint.

The hint overrides ``geomesa.trino.filter.client-variant-pushdown`` for the current query. If omitted, the system
property applies and defaults to ``false``.

================================== =========== =====================
Key                                Type        GeoServer Conversion
================================== =========== =====================
QueryHints.TRINO_VARIANT_PREFILTER ``Boolean`` ``true`` or ``false``
================================== =========== =====================

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.index.conf.QueryHints;

        query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), Boolean.TRUE);

    .. code-tab:: scala

        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.TRINO_VARIANT_PREFILTER, true)

    .. code-tab:: none GeoServer

        ...&viewparams=TRINO_VARIANT_PREFILTER:true

For command-line exports, use ``--hints 'TRINO_VARIANT_PREFILTER=true'``. Use ``false`` to disable the prefilter for
a query when the system property enables it globally.

The client-side filtering mode still applies: ``none`` rejects predicates requiring a residual even when the hint is
``true``. In ``partial`` mode, disabling the prefilter can cause a query to fail if no other predicate can be pushed
down. See :ref:`trino_runtime_configuration` for the supported predicates and filtering modes.
