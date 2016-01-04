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
