.. _trino_runtime_configuration:

Trino Runtime Configuration
===========================

The Trino data store supports the following system properties:

``geomesa.trino.filter.client-side``
------------------------------------

``geomesa.trino.filter.client-side`` controls whether client-side filtering is supported or not. Client-side filters are
predicates that can't be evaluated directly by Trino, so have to be evaluated on the query results after they return to the
client. For example, some JSON-path predicates require a final GeoTools check to preserve type conversion and collection
matching semantics.

The possible values are:

* ``partial`` (default) - client-side filters are allowed as long as an exact predicate or necessary prefilter can be pushed down
* ``none`` - no client-side filters are allowed
* ``all`` - all client-side filters are allowed

Note that the max features for a query with client-side filters can't be pushed down, so queries may consume more resources
than expected.

``geomesa.trino.filter.client-variant-pushdown``
------------------------------------------------

Defaults to ``false``. Set to ``true`` to enable a necessary SQL prefilter for case-sensitive string equality on plain
object paths into schemaless ``json=true`` attributes stored as VARIANT. For example,
``"$.payload.category" = 'example'`` can reject nonmatching strings in Trino before returning documents
to the client. Structured JSON attributes with a ``json-schema`` continue to use their existing ROW translation.

The original predicate is always evaluated as a client-side residual to preserve GeoTools type conversion and collection
matching. Unsupported operators and paths retain their existing fallback. The client-side filtering mode still applies;
``none`` rejects these predicates because they require a residual. Exact COUNT and LIMIT pushdown remain unavailable
while a residual exists.

This optimization can reduce result transfer and client work but increase Trino CPU and Parquet reads, because the document
becomes a scan-filter input. Compare representative workloads and concurrency before enabling this option.
The option is independent of ``geomesa.trino.filter.client-side``: ``partial`` and ``all`` permit the residual check,
and ``none`` rejects it regardless of this flag. With the flag disabled, existing residual filtering behavior is preserved.
Deployments can set these properties through JVM options; changing deployment JVM options requires a
client/worker restart, with no archive rewrite.
