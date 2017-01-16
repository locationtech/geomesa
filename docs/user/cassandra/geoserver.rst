.. _create_cassandra_ds_geoserver:

Using the Cassandra Data Store in GeoServer
===========================================

.. note::

    See :doc:`/user/geoserver` for general information for working with GeoMesa in Geoserver.

On the "Add Store" page, select "Cassandra (GeoMesa)" as shown below.

.. image:: /user/_static/img/CassandraNewDataSource.png

In the following page, enter the name and description of the data store, the contact point
(HOST:PORT), the keyspace and the catalog. Click "Save", and GeoServer will search Cassandra for any
GeoMesa-managed feature types in that keyspace.

.. image:: /user/_static/img/CassandraDSParams.png

Note that when you create the data store, you'll be asked
to specify a work space name. Remember this name because you'll need to use it when querying the
data store in GeoServer.

.. note::

    Note that the "Layer Preview" within GeoServer generally will not work with a Cassandra
    data store, because the Cassandra data store expects queries to have both a ``bbox`` and
    date ``between`` components, but the layer previews do not provide these.

Once you have a Cassandra layer set up in GeoServer you can try some queries. One way to test queries against the
GeoServer layer is to submit HTTP requests for the WMS and WFS services. For example, assuming
you have ingested the ``example.csv`` dataset as described above and set it up as a layer in GeoServer, this request should return
a PNG image with a single dot::

    http://localhost:8080/geoserver/wms?styles=&bbox=-180,-90,180,90&layers=myworkspace:example_csv&cql_filter=bbox(geom, -101, 22.0, -100.0, 24.0, 'EPSG:4326') and lastseen between 2015-05-05T00:00:00.000Z and 2015-05-10T00:00:00.000Z&version=1.3&service=WMS&width=100&request=GetMap&height=100&format=image/png&crs=EPSG:4326

and this request should return a JSON dataset with a single feature::

    http://localhost:8080/geoserver/wfs?service=wfs&request=GetFeature&cql_filter=bbox(geom, -101, 22.0, -100.0, 24.0, 'EPSG:4326') and lastseen between 2015-05-05T00:00:00.000Z and 2015-05-10T00:00:00.000Z&outputFormat=application/json&typeNames=myworkspace:example_csv

Note that you should replace ``myworkspace`` in these queries with the name of the work space you're using in GeoServer.
Also remember that all queries to a Cassandra layer must include both a ``bbox`` component and a date/time ``between`` component
as part of the CQL filter.