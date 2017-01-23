Using the Bigtable Data Store Programmatically
==============================================

An instance of a Bigtable data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath. The Bigtable data store also requires that an
``hbase-site.xml`` be located on the classpath; the connection parameters for the Bigtable data store
are obtained from this file. For more information, see `Connecting to Cloud Bigtable
<https://cloud.google.com/bigtable/docs/connecting-hbase>`__.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>;
    parameters.put("bigtable.table.name", "geomesa");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

The data store takes one parameter:

* ``bigtable.table.name`` - the name of the Bigtable table that stores feature type data

More information on using GeoTools can be found in the `GeoTools user guide
<http://docs.geotools.org/stable/userguide/>`__.