GeoMesa Blob Store
==================

.. warning::

  The GeoMesa Blob Store is deprecated and will be removed in a future version.

The GeoMesa Blob Store is found in ``geomesa-blobstore`` in the source distribution.

The ``GeoMesaIndexedBlobStore`` is an interface that describes the
function of a GeoMesa BlobStore, which is to store and retrieve files
that have spatio-temporal data associated with them. A GeoMesa Blobstore
is created by combining a geotools data store and a backing blob store
which will contain the blobs separated from the geo index. The
``BlobStore`` interface can be used to implement compatibility with
other databases or distributed file systems.

During ingest of a file into a GeoMesa BlobStore, a pluggable system of
``FileHandler``\ s examines the files (and associated maps of
parameters) and creates ``SimpleFeature``\ s which are the written to a
GeoMesa data store while the blobs are persisted using a class that
implements the ``BlobStore`` interface.

File retrieval is a two-step process. First a query is made to the
GeoMesa ``FeatureStore``. The returned ``SimpleFeature``\ s will contain
a URI field. That URI points to the desired file stored in Accumulo, and
can be retrieved by the ``GeoMesaIndexedBlobStore`` API or by the REST
interface.

FileHandler Interface
---------------------

The FileHandler interface is designed to allow file handling in the
Blobstore to be pluggable. Implementors should implement
``org.locationtech.geomesa.blob.api.handlers.FileHandler`` or extend the
abstract implementation
``org.locationtech.geomesa.blob.api.handlers.AbstractFileHandler``. The
class ``WKTFileHandler`` serves as an example of that latter approach.

``FileHandler`` implementations are loaded at runtime by the Java
ServiceLoader. As such, available implementations should have their
classname registered in
``META-INF/services/org.locationtech.geomesa.blob.api.FileHandler``.

BlobStore Interface
-------------------

The ``BlobStore`` interface is designed to allow the backing blob store
of a ``GeoMesaIndexedBlobStore`` to be pluggable. Implementors should
implement ``org.locationtech.geomesa.blob.api.BlobStore`` interface. The
``AccumuloBlobStoreImpl`` serves as an example implementation of a
``BlobStore`` .

As of this time the ``BlobStore`` interface does not yet use the Service
Provider Interface.

Provided File Handlers
----------------------

The BlobStore API contains by default the ``WKTFileHandler`` and
``ByteArrayHandler`` handlers. The ``WKTFileHandler`` allows users to
ingest any file by associating a WKT geometry to a file. Similarly the
``ByteArrayHandler`` allows users to ingest arbitrary byte arrays
accompanied with spatio-temporal attributes.

Along with these included handlers, the GeoMesa BlobStore has two sub
modules of additional handlers in the geomesa-blobstore-handlers module.

GeoMesa-BlobStore-EXIF
~~~~~~~~~~~~~~~~~~~~~~

The EXIF module contains the ``MetadataFileHandler`` which can parse
EXIF metadata to retrieve geolocation information. This enables ingest
of large collections of geotagged images into a GeoMesa BlobStore.

GeoMesa-BlobStore-GDAL
~~~~~~~~~~~~~~~~~~~~~~

The GDAL module contains the ``GDALFileHandler`` which utilizes GDAL to
extract geolocation information and time from raster/imagery files
supported by GDAL 2.0.0. For a complete list of GDAL supported file
formats please refer to: `GDAL formats list`_.

To utilize this handler the user must have compiled GDAL Java bindings
and have placed relevant JNI libraries in the LD\_LIBRARY\_PATH
environment variable. To build GDAL and the JNI libraries, follow the
instructions here: `GDAL JNI build instructions`_.

GeoMesa-BlobStore-Accumulo
--------------------------

The BlobStore-Accumulo module contains concrete implementations of the
``GeoMesaIndexedBlobStore`` interface for Accumulo as well as an
implementation of the ``BlobStore`` interface for Accumulo, the
``AccumuloBlobStoreImpl`` class. The ``GeoMesaAccumuloBlobStore``
utilizes a GeoMesa Accumulo DataStore as well as the
``AccumuloBlobStoreImpl`` for the Blob persistance layer.

File retrieval is a two-step process. First a query is made to the
GeoMesa ``FeatureStore``. The returned ``SimpleFeature``\ s will contain
a URI field. That URI points to the desired file stored in Accumulo, and
can be retrieved by the ``GeoMesaIndexedBlobStore`` API or by the REST
interface.

.. _GDAL formats list: http://www.gdal.org/formats_list.html
.. _GDAL JNI build instructions: https://trac.osgeo.org/gdal/wiki/GdalOgrInJavaBuildInstructionsUnix

REST API
--------

The GeoMesa BlobStore comes with a RESTful web interface servlet that can be used in place of more direct programmatic access via the Java and Scala classes.
As of now the the BlobStore only provides an interface to an Accumulo backed DataStore and BlobStore. Once the servlet is deployed the RESTful api for the Blobstore can be easily utilized via cURL.

DataStores are managed by assigning them to aliases, this allows users to connect to multiple blobstores.
To Register a GeoMesa Blob Store to an alias (in this case myBlobStore):

.. code-block:: bash

    curl -d 'accumulo.instance.id=myCloud' -d 'accumulo.zookeepers=zoo1,zoo2,zoo3' -d 'accumulo.catalog=myBlobStore' -d 'accumulo.user=user' -d 'accumulo.password=password' http://localhost:8080/geoserver/geomesa/blobstore/ds/myBlobStore

Other Blob Store commands:

-  DELETE /ds/:alias - Delete a previously registered GeoMesa data store

-  GET /ds/:alias - Display a registered GeoMesa data store

-  GET /ds/ - Display all registered BlobStores

To ingest a file:

.. code-block:: bash

    curl -X POST -F file=@filename.whatever http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias

To GET a file with the original filename preserved via id, run:

.. code-block:: bash

    curl -JO http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id/

The Blobstore servlet also has optional GZip support which can be used
by adding the ``--compressed`` cURL parameter.

.. code-block:: bash

    curl --compressed -JO http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id

To DELETE a file from the blobstore, you must do so by id:

.. code-block:: bash

    curl -X "DELETE" http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id   

Servlet Configuration Options
-----------------------------

File upload constraints for the ``BlobstoreServlet`` can be configured with
the following system properties:

.. code-block:: bash

    org.locationtech.geomesa.blob.api.maxFileSizeMB    (defaults to 50MB)

.. code-block:: bash

    org.locationtech.geomesa.blob.api.maxRequestSizeMB (defaults to 100MB)  

The expected unit for these properties is in MB, so setting
``org.locationtech.geomesa.blob.api.maxFileSizeMB=10`` will result in a
10MB maxFileSize.