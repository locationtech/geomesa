GeoMesa BlobStore
=================

This tutorial will show you:

1. What the BlobStore is
2. How to extend the BlobStore with FileHandlers
3. How to deploy the GeoMesa BlobStore Servlet
4. How to add FileHandlers to the servlet deploy

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
instructions in :doc:`/user/installation_and_configuration`.

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

. code-block:: java

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
    SimpleFeature buildSF(File file, Map<String, String> params);

Classes the implement the FileHandler interface are loaded at runtime by the Java ServiceLoader.
To make a class that implements the interface available at runtime the classname should be registered in *META-INF/services/org.locationtech.geomesa.blob.api.FileHandler*.


GeoMesa BlobStore Servlet Instructions
--------------------------------------




Adding FileHandlers to the Web Deploy
-------------------------------------
