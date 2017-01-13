GeoMesa BlobStore
=================

This tutorial will show you:

1. What the BlobStore is
2. How to extend the BlobStore with FileHandlers
3. How to deploy the GeoMesa BlobStore Servlet
4. How to add FileHandlers to the servlet deploy
5. How to use the RESTful API

Prerequisites
-------------

.. warning::

    For Accumulo deployment, you will need access to an Accumulo |accumulo_version| instance.

Before you begin, you should have these:

-  basic knowledge of `GeoTools <http://www.geotools.org>`__ and
   `GeoServer <http://geoserver.org>`__
-  access to a `GeoServer <http://geoserver.org/>`__ 2.8.x installation
-  an Accumulo user that has both create-table and write permissions

Before you begin, you should have also set up GeoMesa, using the
instructions in :ref:`installation`.

Introduction
------------

The GeoMesa BlobStore serves as a pluggable interface for ingesting Blobs (Binary Large OBject) with a spatio-temporal index.
By separating the spatial index from the data users can utilize GeoMesa's fast indexing and querying capabilities while storing
their Blobs in a persistence service of their choice. By using a pluggable interface for the FileHandlers users can write
implementations for whatever files they wish. Videos, images, raster datasets, and text are all potentially indexable given a
FileHandler for that file format.


FileHandlers
------------

The GeoMesa BlobStore operates by using the FileHandler interface to provide the capability to process different file formats.
The FileHandler interface has only two functions that must be defined by the user:

.. code-block:: java

    /**
     * Indicates whether or not the class can handle the file with the associated parameters.
     * @param file   File to Store
     * @param params Map of parameters indicating or hinting how the processing should work.
     * @return       Whether or not this class can handle the given input
     */
    Boolean canProcess(File file, Map<String, String> params);

    /**
     * This method builds a SimpleFeature given the input file.
     * @param file   File to Store
     * @param params Map of parameters indicating or hinting how to processing should work.
     * @return       SimpleFeature indexing the file.  Must contain a unique ID.
     */
    SimpleFeature buildSimpleFeature(File file, Map<String, String> params);

Classes the implement the FileHandler interface are loaded at runtime by the Java ServiceLoader.
To make a class that implements the interface available at runtime the classname should be registered in *META-INF/services/org.locationtech.geomesa.blob.api.FileHandler*.


GeoMesa BlobStore Servlet Installation Instructions
---------------------------------------------------

The GeoMesa BlobStore Servlet provides a easy to use RESTful api for interaction with multiple BlobStores.
As of now the the BlobStore only provides an interface to an Accumulo backed DataStore and BlobStore.

You should have already followed the instructions in :ref:`installation` on how to setup GeoMesa.
In particular the instructions on how to setup GeoServer with GeoMesa should have been followed before attempting to deploy the blobstore servlet.

To deploy the BlobStore Servlet into your GeoServer, extract the contents of the ``geomesa-blobstore-gs-plugin_2.11-$VERSION.tar.gz`` file in ``dist/geoserver`` in the binary distribution or ``geomesa-$VERSION/geomesa-blobstore/geomesa-blobstore-gs-plugin/target`` in the source distribution.
into your GeoServer's ``lib`` directory (``$VERSION`` = |release|):

If you are using Tomcat:

.. code-block:: bash

    $ tar -xzvf \
      dist/geoserver/geomesa-blobstore-gs-plugin-$VERSION-install.tar.gz \
      -C /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ tar -xzvf \
      dist/geoserver/geomesa-blobstore-gs-plugin-$VERSION-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib/

Adding FileHandlers to the Web Deploy
-------------------------------------

Individual FileHandlers can be made available to the Servlet by similarly placing the jars in the same *geoserver/WEB-INF/lib/*
directory as the rest of the blobstore servlet components.


RESTful API
-----------

Once the servlet is deployed the RESTful api for the Blobstore can be easily utilized via cURL.
DataStores are managed by assigning them to aliases, this allows users to connect to multiple blobstores.
Below is an example cURL request that registers an Accumulo Data Store to the alias *myBlobStore* . Note the
parameter for instanceId, zookeepers, user and password are particular to the user's environment and will
need to be modified.

.. code-block:: bash

    $ curl -d 'instanceId=myCloud' -d 'zookeepers=zoo1,zoo2,zoo3' -d 'tableName=myBlobStore' \
      -d 'user=user' -d 'password=password' http://localhost:8080/geoserver/geomesa/blobstore/ds/myBlobStore

Once a BlobStore has been registered via the servlet the BlobStore can be accessed.
Additionally to manage registered BlobStores the user has the following commands available:

- DELETE /ds/:alias - Delete a previously registered GeoMesa data store

- GET /ds/:alias - Display a registered GeoMesa data store

- GET /ds/ - Display all registered BlobStores

To ingest a file to the BlobStore run:

.. code-block:: bash

    $ curl -X POST -F file=@filename.whatever http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias


To GET a file with the original filename preserved via id, run:

.. code-block:: bash

    $ curl -JO http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id/


The Blobstore servlet also has optional GZip support which can be used by adding the `--compressed` cURL parameter.

.. code-block:: bash

    $ curl --compressed -JO http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id


To DELETE a file from the blobstore, you must do so by id:

.. code-block:: bash

    $ curl -X "DELETE" http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id


