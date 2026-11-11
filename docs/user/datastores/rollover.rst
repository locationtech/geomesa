Rollover Data Store View
========================

``RolloverDataStoreView`` presents a read-only logical view over existing source
and destination data stores. It reuses ``MergedDataStoreView`` for query execution
and adds a provider of verified destination coverage. Rollover is a query-routing
operation; it neither moves records nor requires deletion or age-off from a source.

A time-relative merge filter assumes the destination has caught up. Rollover
instead uses a published interval, so delays or failures do not advance routing
just because time passes.

Coverage contract
-----------------

The interfaces in ``org.locationtech.geomesa.index.view`` are:

.. code-block:: scala

    case class CoverageInterval(from: Instant, through: Instant)

    trait RolloverCoverage extends java.io.Closeable {
      def coverage(datasetId: String): Option[CoverageInterval]
    }

Dataset IDs are feature type names, scoped to the selected destination. The
producer certifies that ``[from, through)`` is continuous, complete, queryable,
and final, including reconciled late arrivals and updates. Bounds must be finite,
nonempty and representable at millisecond precision. Producer verification and
publication are independent of the query implementation. A completed export or an
elapsed quiet period alone does not establish finality.

Each reader, count, bounds, statistics, or ``getFeatures`` operation captures one
interval. It reads destination rows inside the interval and source rows outside
it, including rows before ``from`` and rows with null timestamps. Null timestamps
cannot be certified by a temporal interval and remain source-owned. Cached feature
sources resolve fresh coverage
on subsequent operations. A returned collection, its iterators, visitors, sorted
views and subcollections retain the operation's interval. This freezes routing,
not the underlying records or a cross-store database transaction.

Construction
------------

This is a programmatic view over provided data stores- the caller supplies the existing
stores and a coverage provider:

.. code-block:: scala

    import org.locationtech.geomesa.index.view.RolloverDataStoreView
    import org.locationtech.geomesa.index.view.RolloverDataStoreView.Delegate

    val view = new RolloverDataStoreView(
      sources = Seq(Delegate(primaryStore)),
      destination = Delegate(secondaryStore),
      coverage = verifiedCoverage,
      namespace = Some("example-rollover"))

``primaryStore``, ``secondaryStore`` and ``verifiedCoverage`` above are placeholders
for caller-supplied instances. Multiple source stores may be supplied, but each
feature type must occur in at most one selected source. ``Delegate`` accepts an
optional ``String => Boolean`` predicate to select feature types for both discovery
and routing. Discovery is the union of the selected types; incompatible schemas
or ambiguous source placement fail. Destination-only types require published
coverage before they can be queried. Both stores must agree on dataset identity.
The view rejects incompatible resolved temporal attributes before constructing
filters: a configured default date field takes precedence over the first Date
attribute, following the existing merged-store rule.

Set ``parallel = true`` for concurrent scans. For delegates implementing
``HasAuthorizationsProvider``, the view automatically uses their authorization
providers to propagate context to worker threads, following the merged-store
convention. An explicit
``taskDecorator: Option[java.util.function.UnaryOperator[Runnable]]`` overrides
this behavior for all delegates. The caller supplies authorization; the view has
no dependency on any application's identity provider, database, producer, or
workflow system.

The public feature source always returns the rollover view from ``getDataStore``,
including when supplied to an optimized GeoMesa process visitor. It rejects queries
for a different feature type and copies input queries so callers cannot retarget
a captured operation. ``HasGeoMesaFeatureReader`` and ``HasGeoMesaStats`` remain
available; direct stats operations resolve coverage once and expose a no-op writer.
Closing the stats facade does not close borrowed datastore statistics.
Feature sources also implement ``HasGeoMesaStats``; process visitors should prefer
that capability to datastore-wide statistics so operation-bound sources retain
their captured coverage. GeoMesa's min/max process follows this convention.

Resource ownership
------------------

The view borrows its physical stores and never disposes them. The caller remains
responsible for their lifecycle. Upon successful construction, the view owns its
coverage provider; ``dispose()`` closes that provider once. Do not share an owned
provider among independently disposed views. If construction fails, the caller
must close the provider. Private operation plans do not own delegates and are never
returned as public datastores. Avoid disposal or destination rewrites while queries
are in flight. The existing ``MergedDataStoreView`` behavior and ownership rules are
unchanged.
