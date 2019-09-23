.. _geomesa_site_xml:

Runtime Configuration
=====================

GeoMesa uses system properties for various runtime configuration options. As a convenience, properties
can be specified in an XML file instead of the command line. When run, GeoMesa will load
the file ``geomesa-site.xml`` from the classpath (if available), and use any properties configured there.

To configure a GeoMesa binary distribution, place ``geomesa-site.xml`` in the ``conf`` folder.
For GeoServer, place the file under ``geoserver/WEB-INF/classes``. For other environments,
ensure the file is available at the root level of the classpath.

Each tools distribution contains a template file with the default settings at
``conf/geomesa-site.xml.template``. Do not modify this file directly as it is never read;
instead copy the desired configurations into ``geomesa-site.xml``.

By default, system properties set through command line parameters will take precedence over the
configuration file. If you wish a configuration item to always take precedence, even over command
line parameters, change the ``<final>`` tag to true.

Configuration properties with empty values will not be applied, you can change this by marking a
property as final.

Common Properties
-----------------

These properties apply to all GeoMesa implementations. Additional properties for different back-end
databases can be found in the chapters for each one.

geomesa.audit.provider.impl
+++++++++++++++++++++++++++

This property specifies the fully-qualified class name of an audit provider implementation to use.
For more information, see :ref:`audit_provider`.

geomesa.convert.config.urls
+++++++++++++++++++++++++++

This property allows for adding GeoMesa converter configurations to the environment. It can be set to a
comma-separated list of arbitrary URLs. For more information on converters, see :ref:`converters`.

geomesa.convert.scripts.path
++++++++++++++++++++++++++++

This property allows for adding files to the classpath. It should be set to a colon-separated list of file
paths. This is useful for getting scripts onto the classpath for use by map-reduce ingest jobs.

geomesa.density.batch.size
++++++++++++++++++++++++++

This property controls the batch size used for running distributed density (heatmap) queries. It needs to be set on
each region or tablet server. If a query is closed or cancelled before completion, the batch size will determine how
long the distributed scan will keep running before seeing the cancellation.

geomesa.distributed.lock.timeout
++++++++++++++++++++++++++++++++

The property controls the length of time a data store will wait to acquire a distributed lock before performing
schema operations (``createSchema``, ``updateSchema`` and ``removeSchema``). As GeoMesa is often run in parallel,
acquiring a distributed lock among different processes prevents metadata corruption that may result from multiple
threads altering the schema simultaneously. The timeout is specified as a duration, e.g. ``1 minute`` or
``30 seconds``, with a default value of ``2 minutes``.

geomesa.distributed.version.check
+++++++++++++++++++++++++++++++++

This property can be used to check for version mismatches in the distributed classpath. When enabled,
GeoMesa will throw an exception if it detects a major version discrepancy between the local classpath and
the distributed classpath (e.g. HBase region servers or Accumulo tablet servers), as this will generally cause
queries to fail. If not enabled, then classpath errors will not be detected proactively, and will likely result
in runtime exceptions.

.. _id_generator_config:

geomesa.feature.id-generator
++++++++++++++++++++++++++++

This property controls the default implementation used for generating IDs for simple features,
if the ``USE_PROVIDED_FIDS`` or ``PROVIDED_FID`` hint is not set in the feature. It should be set to
the fully-qualified class name for a class implementing ``org.locationtech.geomesa.utils.uuid.FeatureIdGenerator``.

GeoMesa includes an implementing class ``org.locationtech.geomesa.utils.uuid.Z3FeatureIdGenerator``, which creates
a unique feature ID partially based on the default time and geometry for the feature. This class is used by
default if nothing else is specified, and should work well for most situations.

geomesa.filter.hash.threshold
+++++++++++++++++++++++++++++

Evaluating a filter of the form ``name = 'john' OR name = 'jane' OR name = 'doe'`` can be slow if the number
of clauses is high. GeoMesa will optimize such filters by turning them into a hash lookup instead of a sequential
comparison. This property controls the threshold for switching to a hash lookup. By default, the threshold is ``5``.

Note that for datastores with distributed filtering (e.g. HBase and Accumulo), this property needs to be set
on the distributed processing nodes.

geomesa.force.count
+++++++++++++++++++

This property controls how GeoMesa calculates the size of a result set (e.g. ``FeatureSource.getCount``).
By default, GeoMesa will estimate the size of a result set using statistics. This will provide a
rough estimate very quickly. Some applications rely on knowing the exact size of a result set up
front, so estimates will cause problems. To force GeoMesa to calculate the exact size of a result
set, you may set this property to ``true``. You may also override this behavior on a per-query basis
by using the query hint ``org.locationtech.geomesa.accumulo.index.QueryHints.EXACT_COUNT``.

geomesa.geometry.processing
+++++++++++++++++++++++++++

This property controls how query geometries will be handled with respect to the anti-meridian. Acceptable values are
one of ``spatial4j`` or ``none``. ``spatial4j`` (the default) will use the Spatial4J library, which will interpret a
geometry with a segment spanning more than 180 degrees of longitude as being inverted around the anti-meridian. To
prevent a geometry from being inverted, add way-points every 180 degrees. ``none`` will interpret geometries
literally. In this case, to query around the anti-meridian, use an OR filter or a geometry collection.

As an example, the following filters both specify a 2-degree area around the anti-meridian:

.. code-block:: java

  // spatial4j processing
  "intersects(geom, 'POLYGON((-179 90, 179 90, 179 -90, -179 -90, -179 90))')"
  // no processing
  "intersects(geom, 'MULTIPOLYGON(((-179 90, -180 90, -180 -90, -179 -90, -179 90)),((179 90, 180 90, 180 -90, 179 -90, 179 90)))')"

While the following filters both specify a 358-degree globe-spanning polygon:

.. code-block:: java

  // spatial4j processing
  "intersects(geom, 'POLYGON((-179 90, 0 90, 179 90, 179 -90, 0 -90, -179 -90, -179 90))')"
  // no processing
  "intersects(geom, 'POLYGON((-179 90, 179 90, 179 -90, -179 -90, -179 90))')"

geomesa.ingest.local.batch.size
+++++++++++++++++++++++++++++++

Controls the batch size for local ingests via the command-line tools. By default, feature writers will be
flushed every 20,000 features.

geomesa.metadata.expiry
+++++++++++++++++++++++

This property controls how often simple feature type metadata is read from the underlying data store.
Calls to ``updateSchema`` on a data store will not show up in other instances until the metadata
cache has expired. The expiry is specified as a duration, e.g. ``10 minutes`` or ``1 hour``.

geomesa.partition.scan.parallel
+++++++++++++++++++++++++++++++

This property controls how scans against multiple, partitioned tables are executed. By default scans will be
executed sequentially. If set to ``true``, they will be executed in parallel. See :ref:`partitioned_indices`
for details on partitioning.

geomesa.query.cost.type
+++++++++++++++++++++++

This property controls how GeoMesa performs query planning. By default, GeoMesa will perform cost-based
query planning using data statistics to determine the best index for a given query. As a fallback option,
this property may be set to ``index`` to use heuristic-based query planning. This may also be overridden on a
per-query basis using the query hint ``org.locationtech.geomesa.accumulo.index.QueryHints.COST_EVALUATION_KEY``
set to either ``org.locationtech.geomesa.accumulo.index.QueryPlanner.CostEvaluation.Stats``
or ``org.locationtech.geomesa.accumulo.index.QueryPlanner.CostEvaluation.Index``. See :ref:`query_planning`
for more details on query planning strategies.

geomesa.query.decomposition.bits
++++++++++++++++++++++++++++++++

In addition to ``geomesa.query.decomposition.multiplier``, below, ``geomesa.query.decomposition.bits`` sets a
lower threshold on the size of the envelopes. It must be between 1 and 63, inclusive. See the Wikipedia article
on `GeoHashes <https://en.wikipedia.org/wiki/Geohash#Algorithm_and_example>`__ for the approximate spatial extent
of a given number of bits.

geomesa.query.decomposition.multiplier
++++++++++++++++++++++++++++++++++++++

GeoMesa creates scan ranges based on the spatial predicates in a query. For complex spatial predicates,
GeoMesa will decompose the geometry into smaller, rectangular envelopes, which avoids scanning over rows which
don't intersect the geometry. This behavior can be controlled through two properties.

``geomesa.query.decomposition.multiplier`` controls the maximum number of envelopes that a geometry will be
decomposed into. If set below 2, no decomposition will be performed and instead the geometry envelope will be used.
Also see ``geomesa.query.decomposition.bits``, above.

geomesa.query.timeout
+++++++++++++++++++++

This property can be used to prevent long-running queries from overloading the system. When set,
queries will be closed after the timeout, even if not all results have been returned yet. The
timeout is specified as a duration, e.g. ``1 minute`` or ``30 seconds``.

geomesa.scan.block-full-table
+++++++++++++++++++++++++++++

This property will prevent full-table scans from executing. A full-table scan is any query that can't be
constrained down using a search index, and thus requires scanning the entire data set. With large data sets,
such a scan can last a long time and be resource intensive. The property is specified as a Boolean, i.e.
``true`` or ``false``.

For more granularity, it is also possible to specify the full-table scan behavior for individual schemas
(``SimpleFeatureTypes``). Use ``geomesa.scan.<type-name>.block-full-table``, where ``<type-name>`` is
replaced with the schema name (e.g. "gdelt"). Properties set for an individual schema will take precedence
over the globally-defined behavior.

geomesa.scan.block-full-table.threshold
+++++++++++++++++++++++++++++++++++++++

This property works in conjunction with ``geomesa.scan.block-full-table``, above. If a query puts a reasonable limit
on the number of features that are returned (through the use of ``maxFeatures``), then it will not be blocked.
The property is specified as an integer. By default, a limit of 1000 or less is allowed.

geomesa.scan.ranges.target
++++++++++++++++++++++++++

This property provides a rough upper-limit for the number of row ranges that will be scanned for a single
query. It is specified as a number. In general, more ranges will result in fewer false-positive rows being
scanned, which will speed up most queries. However, too many ranges can take a long time to generate, and
overwhelm clients, causing slowdowns. The optimal value depends on the environment.

geomesa.sft.config.urls
+++++++++++++++++++++++

This property allows for adding GeoMesa simple feature type configurations to the environment. It can be set to
a comma-separated list of arbitrary URLs. For more information on defining types, see :ref:`cli_sft_conf`.

geomesa.stats.batch.size
++++++++++++++++++++++++

This property controls the batch size used for running distributed stat queries. It needs to be set on each
region or tablet server. If a query is closed or cancelled before completion, the batch size will determine how
long the distributed scan will keep running before seeing the cancellation.

.. _stats_generate_config:

geomesa.stats.generate
++++++++++++++++++++++

This property controls whether GeoMesa will generate statistics during ingestion. It is specified as a Boolean,
``true`` or ``false``. This property will be used if a data store is not explicitly configured using the
``geomesa.stats.enable`` data store parameter.

geomesa.strategy.decider
++++++++++++++++++++++++

This property allows for overriding strategy selection during query planning. It should specify the
full class name for a class implementing ``org.locationtech.geomesa.index.planning.StrategyDecider``.
The class must have a no-arg constructor.

By default GeoMesa will use cost-based query planning, which should work well for most situations. See
:ref:`query_planning` for more details on query planning strategies.
