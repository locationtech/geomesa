Bigtable Data Store
===================

The GeoMesa Bigtable Data Store is an implementation of the GeoTools
``DataStore`` interface that is backed by `Google Cloud Bigtable <https://cloud.google.com/bigtable/>`__.

The code for Bigtable support is found in two modules in the source distribution:

* ``geomesa-hbase/geomesa-bigtable-datastore`` - contains a stub POM for building a Google Cloud Bigtable-backed GeoTools data store
* ``geomesa-hbase/geomesa-bigtable-gs-plugin`` - contains a stub POM for building a bundle containing all of the JARs to use the Google Cloud Bigtable data store in GeoServer

.. toctree::
    :maxdepth: 1

    install
    usage
    geoserver
    dataproc
