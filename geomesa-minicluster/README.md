# Accumulo MiniCluster

This module is for running a local accumulo MiniCluster for integration testing without needing a
whole cloud.

### Building

This module inherits from the main GeoMesa module, but it not part of it. You need to do a
```mvn install``` on the main project in order to see your changes in this module.

To build this module, run ```mvn clean package``` from the module folder.

#### Running

Once the module is build, you can run it with the following command:

<pre>
java -cp target/geomesa-minicluster-accumulo1.5-1.0.0-SNAPSHOT-shaded.jar:data/ \
  org.locationtech.geomesa.core.integration.MiniCluster -t &lt;data_type&gt;
</pre>

<pre>
Options:
  -t &lt;data_type&gt; - data type to load into the cluster
  -p &lt;port&gt; - port to run the cluster on (default 10099)
</pre>

The cluster works best with smaller amounts of data (less than 100k features). You may need to increase
memory using the standard Java options:

<pre>
java -Xms1024m -Xmx8g -XX:MaxPermSize=2g ...
</pre>

### Data

The cluster will look for a file on the classpath named ```<data_type>.tsv```. This file should contain
the features you want loaded into your cluster, in the following format:

* The header row contains the attribute names
* Each subsequent row is a single feature
* The columns are tab-separated
* The first column must contain the feature ID
* There must be an attribute named ```dtg``` that contains the date field for indexing
* The default geometry must be specified with a ```*``` prefix

Example:

<pre>
mydata.tsv
</pre>
<pre>
featureId:String    attribute1:String   dtg:Date    *geom:Point:srid=4326
1234    value1  2014-01-01 00:00:00	POINT (-80.0 37.0)
</pre>

There is a DataExporter class that can be used to export data from an existing cloud.

### Connecting

Once the cluster is running, you can connect using the standard data store parameter map. The following
values should be used, where <data_type> is the parameter you started the cluster with:

<pre>
"instanceId"   -> "miniInstance"
"zookeepers"   -> "localhost:10099"
"user"         -> "root"
"password"     -> "password"
"tableName"    -> &lt;data_type&gt;
</pre>

Note that the port can also be configured when starting the cluster.
