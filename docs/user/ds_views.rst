Combined Data Store Views
=========================

GeoMesa provides two generic options for combining multiple data stores into a single view. The first option
is a 'merged' view, where each data store contain a different set of features and they are queried concurrently.
This can be used, for example, to keep a fast, small cache of the most recent data, or to archive off older data
into lower-cost storage. The second option is a 'routed' view, where each data store contains the same set of
features and each query is sent to only one data store. This can be used, for example, to support secondary indexing.

In order to use a layer through the a data store view, the SimpleFeatureType must exist (and match) in all of
the underlying stores. If a schema exists in some stores but not all of them, it will not show up in the
combined view.

Installation
------------

The GeoMesa data store views are available in the ``geomesa-index-api`` JAR, which is bundled by default with all
GeoMesa data stores. The underlying stores being combined must be installed separately; see the relevant
documentation for each store. Depending on the stores, you may need to resolve classpath conflicts between
their dependencies.

Merged Data Store View
----------------------

The GeoMesa Merged Data Store View provides a unified way to query multiple data stores concurrently. For example,
you may want to store more recent data in an HBase data store instance, and older data in a FileSystem data
store instance to reduce storage costs, but still provide a single layer to query both.

In comparison to the :doc:`/user/lambda/index`, the Merged Data Store View does not provide any management for
transitioning features between data stores. All writes must be done through the underlying stores directly,
and if the same features exist in multiple stores, they may be returned multiple times in a single query response.

Usage
^^^^^

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
          "accumulo.instance.id": "test",
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

Query Filtering
^^^^^^^^^^^^^^^

If the stores being merged have known characteristics, filters can be applied selectively to each store in
order to speed up queries. The filter is specified along with the other store parameters, under the key
``geomesa.merged.store.filter``, and should be an ECQL filter string.

The filter will be applied against any query, in addition to the query filter. This can be used to short-circuit
queries that are not relevant for a particular store. For example, if one store contains features from the past
24 hours, and a second store contains features older than 24 hours, then you could configure them with
time-based filters:

.. code-block:: json

    {
      "stores": [
        {
          "other.store.params": "go here...",
          "geomesa.merged.store.filter": "dtg >= currentDate('-P1D')"
        },
        {
          "other.store.params": "go here...",
          "geomesa.merged.store.filter": "dtg < currentDate('-P1D')"
        }
      ]
    }

Store Configuration Provider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an alternative to specifying ``geomesa.merged.stores``, config loading can be delegated to a provider
interface: ``org.locationtech.geomesa.index.view.MergedViewConfigLoader``. Implementations of this class
must be made available via Java `SPI loading <http://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__,
with a special descriptor defined in ``META-INF/services``. To use a config provider, use the parameter
``geomesa.merged.loader`` set to the full class name of the provider class. In GeoServer, available providers
will be displayed in a selection list.

Note that you may combine the explicit configuration of ``geomesa.merged.stores`` with the delegated
configuration of ``geomesa.merged.loader``, in which case the two configurations will be merged.


Routed Data Store View
----------------------

The GeoMesa Routed Data Store View provides a unified way to route queries to multiple data stores. For example,
you may want to store a geospatial index in an HBase data store instance, and attribute indices in a FileSystem data
store instance to reduce storage costs, but still provide a single layer to query both.

All writes must be done through the underlying stores directly. Although not required, generally the same
features should exist in each store. If the features vary from store to store, inconsistencies may occur
when querying.

Usage
^^^^^

The Routed Data Store View can be instantiated through the standard GeoTools ``DataStoreFinder`` or the GeoServer
``New Data Source`` page. It only requires a single parameter: ``geomesa.routed.stores``.

``geomesa.routed.stores`` should be a `TypeSafe Config <https://github.com/lightbend/config>`_ string that defines
the parameters for each underlying store. The config should have a top-level key of ``stores`` that is a list
of objects, where each object is a set of key-value pairs corresponding to the parameters for a single data store.

For example, to combine a GeoMesa Accumulo data store with a PostGis data store, you could use the following config:

.. code-block:: json

    {
      "stores": [
        {
          "accumulo.zookeepers": "localhost",
          "accumulo.instance.id": "test",
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


Query Routing
^^^^^^^^^^^^^

The routed view requires a way to route any incoming query to a particular data store. By default, queries
can be routed based on the query filter attributes. For more advanced use cases, see below.

To route based on attributes, each store can be configured with the attribute combinations that it can handle
efficiently. The attributes are specified under the key ``geomesa.route.attributes``, alongside the other data
store parameters. The attributes should be an array of groups of names that can be queried together.
An empty attribute array corresponds to ``Filter.INCLUDE``, and will be used as a fall-back if nothing else
is matched. The special string ``"id"`` can be used to match a feature ID query. If a query does not match
any of the configured attributes, then it will not return any results.

The following example will route queries such as ``INCLUDE``, ``IN ('1', '2')``, ``bbox(geom,...)`` and
``bbox(geom, ...) and dtg during ...`` to the first store, while queries such as ``name = ...`` and ``age > ...``
will be routed to the second store:

.. code-block:: json

    {
      "stores": [
        {
          "other.store.params": "go here...",
          "geomesa.route.attributes": [ [], [ "id" ], [ "geom" ], [ "dtg", "geom" ] ]
        },
        {
          "other.store.params": "go here...",
          "geomesa.route.attributes": [ [ "name" ], [ "age" ] ]
        }
      ]
    }

Custom Routing
""""""""""""""

As an alternative to routing by attribute, routing can be delegated to a provider interface:
``org.locationtech.geomesa.index.view.RouteSelector``. Implementations of this class must be made available via
Java `SPI loading <http://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__, with a special
descriptor defined in ``META-INF/services``. To use a config provider, use the parameter
``geomesa.route.selector`` set to the full class name of the provider class. In GeoServer, available providers
will be displayed in a selection list.
