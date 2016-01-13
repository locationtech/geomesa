#GeoMesa Blobstore
The `AccumuloBlobStore` is a class designed to store and retrieve files which have 
spatio-temporal data associated with them.  During ingest, a pluggable system of `FileHandler`s 
examines the files (and associated maps of parameters) and creates `SimpleFeature`s which are 
inserted into a GeoMesa feature store.  

File retrieval is a two-step process.  First a query is made to the GeoMesa `FeatureStore`.  The 
returned `SimpleFeature`s will contain a URI field.  That URI points to the desired file stored 
in Accumulo, and can be retrieved by the `AccumuloBlobStore` API or REST interface.

## FileHandler Interface

The FileHandler interface is designed to allow file handling in the Blobstore to be pluggable.
Implementors should implement `org.locationtech.geomesa.blob.core.handlers.FileHandler` or 
extend the abstract implementation `org.locationtech.geomesa.blob.core.handlers.AbstractFileHandler`. 
The class `WKTFileHandler` serves as an example of that latter approach.

`FileHandler` implementations are loaded at runtime by the Java ServiceLoader.  As such, available 
implementations should have their classname registered in `META-INF/services/org.locationtech.geomesa.blob.core.handlers.FileHandler`.

## REST api

The RESTful api for the Blobstore can be easily utilized via cURL as so:

Before we can do anything we must first register a blob store. This typically only needs to be run once in the lifetime
of the webserver, so if the server is shutdown or restarted the registration call will need to be repeated.
 
To Register a blob store: 

```bash
curl -d 'instanceId=myCloud' -d 'zookeepers=zoo1,zoo2,zoo3' -d 'tableName=myBlobStore' -d 'user=user' -d 'password=password' http://localhost:8080/geoserver/geomesa/blob/ds
```

To ingest a file:

```bash
curl -X POST -F file=@filename.whatever http://localhost:8080/geoserver/geomesa/blob
```

To GET a file with the original filename preserved via id, run:  

```bash  
curl -JO http://localhost:8080/geoserver/geomesa/blob/some-id
```  

The Blobstore servlet also has optional GZip support which can be used by adding the `--compressed` cURL parameter.  

```bash  
curl --compressed -JO http://localhost:8080/geoserver/geomesa/blob/some-id
```  

To DELETE a file from the blobstore, you must do so by id:

```bash
curl -X "DELETE" http://localhost:8080/geoserver/geomesa/blob/some-id   
```