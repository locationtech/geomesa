GeoMesa Blob Store
==================

The GeoMesa Blob Store is found in ``geomesa-blobstore`` in the source distribution.

The ``AccumuloBlobStore`` is a class designed to store and retrieve
files which have spatio-temporal data associated with them. During
ingest, a pluggable system of ``FileHandler``\ s examines the files (and
associated maps of parameters) and creates ``SimpleFeature``\ s which
are inserted into a GeoMesa feature store.

File retrieval is a two-step process. First a query is made to the
GeoMesa ``FeatureStore``. The returned ``SimpleFeature``\ s will contain
a URI field. That URI points to the desired file stored in Accumulo, and
can be retrieved by the ``AccumuloBlobStore`` API or REST interface.

FileHandler Interface
---------------------

The FileHandler interface is designed to allow file handling in the
Blobstore to be pluggable. Implementors should implement
``org.locationtech.geomesa.blob.core.handlers.FileHandler`` or extend
the abstract implementation
``org.locationtech.geomesa.blob.core.handlers.AbstractFileHandler``. The
class ``WKTFileHandler`` serves as an example of that latter approach.

``FileHandler`` implementations are loaded at runtime by the Java
ServiceLoader. As such, available implementations should have their
classname registered in
``META-INF/services/org.locationtech.geomesa.blob.core.handlers.FileHandler``.

REST API
--------

The RESTful API for the Blobstore can be easily utilized via cURL.

To register a GeoMesa Blob Store to an alias (in this case myBlobStore):

.. code:: bash

    curl -d 'instanceId=myCloud' -d 'zookeepers=zoo1,zoo2,zoo3' -d 'tableName=myBlobStore' -d 'user=user' -d 'password=password' http://localhost:8080/geoserver/geomesa/blobstore/ds/myBlobStore

Other Blob Store commands:

-  DELETE /ds/:alias - Delete a previously registered GeoMesa data store

-  GET /ds/:alias - Display a registered GeoMesa data store

-  GET /ds/ - Display all registered BlobStores

To ingest a file:

.. code:: bash

    curl -X POST -F file=@filename.whatever http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias

To GET a file with the original filename preserved via id, run:

.. code:: bash

    curl -JO http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id/

The Blobstore servlet also has optional GZip support which can be used
by adding the ``--compressed`` cURL parameter.

.. code:: bash

    curl --compressed -JO http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id

To DELETE a file from the blobstore, you must do so by id:

.. code:: bash

    curl -X "DELETE" http://localhost:8080/geoserver/geomesa/blobstore/blob/:alias/some-id   

Servlet Configuration Options
-----------------------------

File upload constraints for the ``BlobstoreServlet`` can be configured with
the following system properties:

.. code:: bash

    org.locationtech.geomesa.blob.api.maxFileSizeMB    (defaults to 50MB)

.. code:: bash

    org.locationtech.geomesa.blob.api.maxRequestSizeMB (defaults to 100MB)  

The expected unit for these properties is in MB, so setting
``org.locationtech.geomesa.blob.api.maxFileSizeMB=10`` will result in a
10MB maxFileSize.