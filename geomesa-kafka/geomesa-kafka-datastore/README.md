#GeoMesa KafkaDataStore
The `KafkaDataStore` is an implementation of the GeoTools `DataStore` interface that is backed by a Kafka message broker.
The implementation supports the ability for feature producers to persist data into the data store and for consumers to 
pull data from the data store.  The data store supports both live and replay consumer modes.  Consumers operating in 
live mode pull data from the end of the message queue (e.g latest time) while consumers operating in replay mode pull data 
from a specified time interval in the past.  

##Usage/Configuration
Clients of the `KafkaDataStore` select between live mode and replay mode through hints provided by the SimpleFeatureType
object passed to `createSchema(SimpleFeatureType)`.  These hints are added through the use of the following helper
methods:
    
    
    KafkaDataStoreHelper.createStreamingSFT(sft: SimpleFeatureType, zkPath: String) : SimpleFeatureType
    KafkaDataStoreHelper.createReplaySFT(sft: SimpleFeatureType, rc: ReplayConfig) : SimpleFeatureType
    
###Data Producers

Data *producers* should build a SimpleFeatureType object and pass it to `createStreamingSFT` along with a zookeeper path that 
must be unique for a given KafkaDataStore.  The Producer should pass the returned SimpleFeature type to the call to
`Datastore.createSchema`.  For example:

    val ds: DataStore = ...
    val outputSFT: SimpleFeatureType = ...
    
    ...
    
    val zkPath = "/a/unique/zkpath"
    val streamingSFT = KafkaDataStoreHelper.createStreamingSFT(outputSFT, zkPath)
    ds.createSchema(streamingSFT)
    
###Data Consumers (Live Mode)

Data *consumers* that are running in *live* mode access the DataStore through the usual DataStore interface.

###Data Consumers (Replay Mode)

Data *consumers* that are running in *replay* mode will have to first create a new replay schema in the DataStore that 
is derived from the original schema.  The consumer should get the SimpleFeatureType for original schema and use it (
along with a `ReplayConfig`) to create the derived schema via a call to `DataStore.createReplaySFT()`.

    val datastore = ...
    val typename = ...
    val sft = ds.getSchema(typeName)
    val rc = new ReplayConfig(replayStart, replayEnd, readBehind)

    // create the replay schema from the original schema and replay params
    datastore.createSchema(KafkaDataStoreHelper.createReplaySFT(sft, rc))

It should be noted that the new schema will have a different type name and an additional attribute in the schema (time) 
from the original SimpleFeatureType.

### Command Line Tools

Included is a KafkaGeoMessageFormatter that may be used with the `kafka-console-consumer`.  In order to use
this formatter call the kafka-console consumer with these additional arguments:

    --formatter org.locationtech.geomesa.kafka.KafkaGeoMessageFormatter
    --property sft.name={sftName}
    --property sft.spec={sftSpec}
    
In order to pass the spec via a command argument all `%` characters must be replaced by `%37` and all `=`
characters must be replaced by `%61`.

A slightly easier to use but slightly less flexible alternative is to use the `KafkaDataStoreLogViewer` instead
of the `kafka-log-viewer`.  To use the `KafkaDataStoreLogViewer` first copy the
geomesa-kafka-geoserver-plugin.jar to $KAFKA_HOME/libs.  Then create a copy of
$KAFKA_HOME/bin/kafka-console-consumer.sh called "kafka-ds-log-viewer" and in the copy replace the classname
in the exec command at the end of the script with `org.locationtech.geomesa.kafka.KafkaDataStoreLogViewer`.
The `KafkaDataStoreLogViewer` requires three arguments: `--zookeeper`, `--zkPath`, and `--sftName`.  It also
supports an optional argument `--from` which accepts values `oldest` and `newest`.  `oldest` is equivalent
to specifying `--from-beginning` when using the `kafka-console-consumer`'s  option and `newest` is equivalent
 to not specifying `--from-beginning`.