# GeoMesa HBase Data Store

The data store module contains all of the HBase-related code for GeoMesa.
 
## HBase Data Store

An instance of an HBase data store can be obtained through the normal GeoTools discovery methods, assuming that the GeoMesa code is on the classpath. The HBase data store also requires that an `hbase-site.xml` be located on the classpath; the connection parameters for the HBase data store, including `hbase.zookeeper.quorum` and `hbase.zookeeper.property.clientPort`, are obtained from this file.

```java
Map<String, Serializable> parameters = new HashMap<>;
parameters.put("bigtable.table.name", "geomesa");
parameters.put("namespace", "http://example.com");
org.geotools.data.DataStore dataStore = org.geotools.data.DataStoreFinder.getDataStore(parameters);
```

The data store takes two parameters:

* **bigtable.table.name** - the name of the HBase table that stores feature type data
* **namespace** - the namespace URI for the data store (optional) 

More information on using GeoTools can be found in the GeoTools [user guide](http://docs.geotools.org/stable/userguide/).
