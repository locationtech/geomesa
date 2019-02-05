Merged Data Store View
======================

The GeoMesa Merged Data Store View provides a unified way to query multiple data stores concurrently. For example,
you may want to store more recent data in an HBase data store instance, and older data in a FileSystem data
store instance to reduce storage costs, but still provide a single layer to query both.

.. warning::

    The Merged Data Store View is an alpha-level feature, and may change without notice

In comparison to the :doc:`/user/lambda/index`, the Merged Data Store View does not provide any management for
transitioning features between data stores. All writes must be done through the underlying stores directly,
and if the same features exist in multiple stores, they may be returned multiple times in a single query response.

In order to use a layer through the Merged Data Store View, the SimpleFeatureType must exist (and match) in all of
the underlying stores. If a schema exists in some stores but not all of them, it will not show up in the
merged view.

Installation
------------

The Merged Data Store View is available in the ``geomesa-index-api`` JAR, which is bundled by default with all
GeoMesa data stores. The underlying stores being merged must be installed separately; see the relevant
documentation for each store. Depending on the stores being merged, you may need to resolve classpath conflicts
between the store dependencies.

Usage
-----

The Merged Data Store View can be instantiated through the standard GeoTools ``DataStoreFinder`` or the GeoServer
``New Data Source`` page. It only requires a single parameter: ``geomesa.merged.stores``.

``geomesa.merged.stores`` should be a `TypeSafe Config <https://github.com/lightbend/config>`_ string the defines
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
---------------

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
          // regular store parameters go here
          "geomesa.merged.store.filter": "dtg >= currentDate('-P1D')"
        },
        {
          // regular store parameters go here
          "geomesa.merged.store.filter": "dtg < currentDate('-P1D')"
        }
      ]
    }

Config Provider
---------------

As an alternative to specifying ``geomesa.merged.stores``, config loading can be delegated to a provider
interface: ``org.locationtech.geomesa.index.view.MergedViewConfigLoader``. Implementations of this class
must be made available via Java `SPI loading <http://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html>`__,
with a special descriptor defined in ``META-INF/services``. To use a config provider, use the parameter
``geomesa.merged.loader`` set to the full class name of the provider class. In GeoServer, available providers
will be displayed in a selection list.

Note that you may combine the explicit configuration of ``geomesa.merged.stores`` with the delegated
configuration of ``geomesa.merged.loader``, in which case the two configurations will be merged.
