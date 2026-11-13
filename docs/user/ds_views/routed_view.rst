Routed Data Store View
======================

The GeoMesa Routed Data Store View provides a unified way to route queries to multiple data stores. For example,
you may want to store a geospatial index in an HBase data store instance, and attribute indices in a FileSystem data
store instance to reduce storage costs, but still provide a single layer to query both.

All writes must be done through the underlying stores directly. Although not required, generally the same
features should exist in each store. If the features vary from store to store, inconsistencies may occur
when querying.

Installation
------------

The GeoMesa data store views are available in the ``geomesa-index-api`` JAR, which is bundled by default with all
GeoMesa data stores. The underlying stores being combined must be installed separately; see the relevant
documentation for each store. Depending on the stores, you may need to resolve classpath conflicts between
their dependencies.

Usage
-----

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


Query Routing
-------------

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
^^^^^^^^^^^^^^

As an alternative to routing by attribute, routing can be delegated to a provider interface:
``org.locationtech.geomesa.index.view.RouteSelector``. Implementations of this class must be made available via
Java `SPI loading <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__, with a special
descriptor defined in ``META-INF/services``. To use a config provider, use the parameter
``geomesa.route.selector`` set to the full class name of the provider class. In GeoServer, available providers
will be displayed in a selection list.
