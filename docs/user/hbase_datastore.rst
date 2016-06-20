HBase/BigTable Data Stores
==========================

HBase
-----
The code for the HBase support is found in two modules:

* **geomesa-hbase/geomesa-hbase-datastore** - the core GeoMesa HBase datastore
* **geomesa-gs-plugin/geomesa-hbase-gs-plugin** - the GeoServer bundle for GeoMesa HBase

HBase Data Store
~~~~~~~~~~~~~~~~

An instance of an HBase data store can be obtained through the normal GeoTools discovery methods, assuming that the GeoMesa code is on the classpath. The HBase data store also requires that an ``hbase-site.xml`` be located on the classpath; the connection parameters for the HBase data store, including ``hbase.zookeeper.quorum`` and ``hbase.zookeeper.property.clientPort``, are obtained from this file.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>;
    parameters.put("bigtable.table.name", "geomesa");
    parameters.put("namespace", "http://example.com");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

The data store takes two parameters:

* **bigtable.table.name** - the name of the HBase table that stores feature type data
* **namespace** - the namespace URI for the data store (optional)

More information on using GeoTools can be found in the GeoTools [user guide](http://docs.geotools.org/stable/userguide/).

.. _install_hbase_geoserver:

Using the HBase Data Store in GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The HBase GeoServer plugin is a shaded JAR that contains HBase 1.1.5. To change HBase versions,
update ``pom.xml`` and rebuild this module.

To build **geomesa-gs-plugin/geomesa-hbase-gs-plugin**, use the ``hbase`` Maven profile:

.. code-block:: bash

    $ mvn clean install -Phbase

After building, extract ``target/geomesa-hbase-gs-plugin-$VERSION-install.tar.gz`` into GeoServer's ``WEB-INF/lib`` directory.

This distribution does not include the Hadoop or Zookeeper JARs; the following JARs
should be copied from the ``lib`` directory of your HBase or Hadoop installations into
GeoServer's ``WEB-INF/lib`` directory:

 * hadoop-annotations-2.5.1.jar
 * hadoop-auth-2.5.1.jar
 * hadoop-common-2.5.1.jar
 * hadoop-mapreduce-client-core-2.5.1.jar
 * hadoop-yarn-api-2.5.1.jar
 * hadoop-yarn-common-2.5.1.jar
 * zookeeper-3.4.6.jar
 * commons-configuration-1.6.jar

(Note the versions may vary depending on your installation.)

The HBase data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist).

BigTable
--------

Experimental support for GeoMesa implemented on Google BigTable.

The code for BigTable support is found in two modules in the source distribution:

* **geomesa-hbase/geomesa-bigtable-datastore** - contains a stub POM for building a Google BigTable-backed GeoTools data store
* **geomesa-gs-plugin/geomesa-bigtable-gs-plugin** - contains a stub POM for building a bundle containing
all of the JARs to use the Google BigTable data store in GeoServer