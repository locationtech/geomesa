Merged Data Store View
======================

The GeoMesa Merged Data Store View provides a unified way to query multiple data stores concurrently. For example,
you may want to store more recent data in an HBase data store instance, and older data in a FileSystem data
store instance to reduce storage costs, but still provide a single layer to query both.

In comparison to the :doc:`/user/lambda/index`, the Merged Data Store View does not provide any management for
transitioning features between data stores. All writes must be done through the underlying stores directly,
and if the same features exist in multiple stores, they may be returned multiple times in a single query response.

Installation
------------

The GeoMesa data store views are available in the ``geomesa-index-api`` JAR, which is bundled by default with all
GeoMesa data stores. The underlying stores being combined must be installed separately; see the relevant
documentation for each store. Depending on the stores, you may need to resolve classpath conflicts between
their dependencies.

Usage
-----

The Merged Data Store View can be instantiated through the standard GeoTools ``DataStoreFinder`` or the GeoServer
``New Data Source`` page. It only requires a single parameter: ``geomesa.merged.stores``.

``geomesa.merged.stores`` should be a `TypeSafe Config <https://github.com/lightbend/config>`_ string that defines
the parameters for each merged store. The config should have a top-level key of ``stores`` that is a list
of objects, where each object is a set of key-value pairs corresponding to the parameters for a single data store.

For example, to merge a GeoMesa Accumulo data store with a PostGis data store, you could use the following config:

.. code-block:: json

    {
      "stores": [
        {
          "accumulo.zookeepers": "localhost",
          "accumulo.instance.name": "test",
          "accumulo.catalog": "test",
          "accumulo.user": "test",
          "accumulo.password": "test"
        },
        {
          "dbtype": "postgis",
          "host": "localhost",
          "port": "5432",
          "database": "test",
          "user": "test",
          "passwd": "test"
        }
      ]
    }

If the stores may contain duplicate features, these can be filtered out by setting the parameter
``geomesa.merged.deduplicate`` to ``true``. Features are identified by their feature ID. The list of stores should be
defined in priority order, as the first feature with a given feature ID will be returned while any others will be
filtered out. Deduplication will be more efficient if stores with fewer features are defined first in the list.

The parameter ``geomesa.merged.scan.parallel`` can be set to ``true`` to scan all underlying stores in parallel,
instead of sequentially. When parallel scanning is enabled, the parameter ``geomesa.merged.task.decorator`` can
be set to the fully-qualified name of a class implementing  ``java.util.function.UnaryOperator<java.lang.Runnable>``.
The decorator will be invoked on tasks before they are passed off to a new thread. This can be used, for example, to
propagate thread-local user authorizations. If not otherwise specified, any configured :ref:`auth_providers`
will be used to decorate tasks for the associated data store.

Query Filtering
---------------

If the stores being merged have known characteristics, filters can be applied selectively to each store in
order to speed up queries. The most common case is when stores are separated by by time ranges. To support this,
starting and ending dates can be specified along with the other store parameters, using the keys
``geomesa.merged.store.date.start`` (inclusive) and ``geomesa.merged.store.date.end`` (exclusive). The dates
can either be absolute (e.g. ``2026-01-26T00:00:00.000Z``) or relative durations, specified as ISO-8601 (e.g. ``-P1D``).
The default date field in the schema will be used for filtering.

For other use cases, an arbitrary filter can be specified using the key ``geomesa.merged.store.filter``, which should be
an ECQL filter string.

The filters will be used on all layers in the store. For fine-grained control, per-layer filters can be specified
by appending the type name to the filter parameter - for example, ``geomesa.merged.store.date.start.myFeatureType``.

The filters will always be applied in addition to the query filter. This can be used to short-circuit
queries that are not relevant for a particular store. For example, if one store contains features from the past
24 hours, and a second store contains features older than 24 hours, then you could configure them with
time-based filters:

.. code-block:: json

    {
      "stores": [
        {
          "other.store.params": "go here...",
          "geomesa.merged.store.date.start": "-P1D"
        },
        {
          "other.store.params": "go here...",
          "geomesa.merged.store.date.end": "-P1D"
        }
      ]
    }

Custom Query Filtering
^^^^^^^^^^^^^^^^^^^^^^

In addition to the options mentioned above, a custom filter provider can be specified using
``geomesa.merged.store.filter-provider``. The specified class must implement
``org.locationtech.geomesa.index.view.MergedFilterProvider``. Any sub-parameters specified under
``geomesa.merged.store.filter-provider.*`` will be passed to the provider to initialize it.

Rollover Filtering
^^^^^^^^^^^^^^^^^^

One use of a merged view is to manage tiered storage, for example recent features might be stored in PostGIS, while older
features be archived to S3 and accessed through Trino. In such a scenario, data may be migrated from one tier to another
using scheduled jobs. Rollover filtering is designed to address this use case, by bounding the stores based on verified
data migration results.

Rollover filtering requires a JDBC connection used for retrieving rollover results. The migration process must update
the results table, which is not covered here but can be accomplished through
``org.locationtech.geomesa.index.view.filters.RolloverFilterProvider#insertCoverage``. For stores configured with
a ``SourceProvider``, data will be filtered to be outside the published coverage. For stores configured with a
``DestinationProvider``, data will be filtered on be inside the published coverage - if there are no published coverages,
then no data will be returned from the destination store.

In order to configure rollover filtering, specify the rollover filter provider as a custom provider:

.. code-block:: json

    {
      "stores": [
        {
          "other.store.params": "go here... this is the 'source' store, the first tier of storage",
          "geomesa.merged.store.filter-provider": "org.locationtech.geomesa.index.view.filters.RolloverFilterProvider$SourceProvider",
          "geomesa.merged.store.filter-provider.schema": "public",
          "geomesa.merged.store.filter-provider.table": "rollover_status",
          "geomesa.merged.store.filter-provider.url": "jdbc:postgresql://localhost:5432/postgres",
          "geomesa.merged.store.filter-provider.username": "postgres",
          "geomesa.merged.store.filter-provider.password": "postgres",
          "geomesa.merged.store.filter-provider.max-connections": "2"
        },
        {
          "other.store.params": "go here... this is the 'destination' store, the second tier of storage",
          "geomesa.merged.store.filter-provider": "org.locationtech.geomesa.index.view.filters.RolloverFilterProvider$DestinationProvider",
          "geomesa.merged.store.filter-provider.schema": "public",
          "geomesa.merged.store.filter-provider.table": "rollover_status",
          "geomesa.merged.store.filter-provider.url": "jdbc:postgresql://localhost:5432/postgres",
          "geomesa.merged.store.filter-provider.username": "postgres",
          "geomesa.merged.store.filter-provider.password": "postgres",
          "geomesa.merged.store.filter-provider.max-connections": "2"
        }
      ]
    }

Store Configuration Provider
----------------------------

As an alternative to specifying ``geomesa.merged.stores``, config loading can be delegated to a provider
interface: ``org.locationtech.geomesa.index.view.MergedViewConfigLoader``. Implementations of this class
must be made available via Java `SPI loading <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__,
with a special descriptor defined in ``META-INF/services``. To use a config provider, use the parameter
``geomesa.merged.loader`` set to the full class name of the provider class. In GeoServer, available providers
will be displayed in a selection list.

Note that you may combine the explicit configuration of ``geomesa.merged.stores`` with the delegated
configuration of ``geomesa.merged.loader``, in which case the two configurations will be merged.
