#GeoMesa KafkaDataStore
The `KafkaDataStore` is an implementation of the geotools `DataStore` interface that is backed by a Kafka message broker.
The implementation supports the ability for feature producers to persist data into the data store and for consumers to 
pull data from the datastore.  The data store supports both live and replay consumer modes.  Consumers operating in 
live mode pull data from the end of the message queue (e.g latest time) while consumers operating in replay mode pull data 
from a specified time interval in the past.  

##Usage/Configuration
Clients of the `KafkaDataStore` select between live mode and replay mode through hints provided by the SimpleFeatureType
object passed to `createSchema(SimpleFeatureType)`.  These hints are added through the use of the following helper
methods:
    
    
    KafkaDataStoreHelper.prepareForLive(sft: SimpleFeatureType, zkPath: String) : SimpleFeatureType
    KafkaDataStoreHelper.prepareForReplay(sft: SimpleFeatureType, rc: ReplayConfig) : SimpleFeatureType
    
###Data Producers

Data *producers* should build a SimpleFeatureType object and pass it to `prepareForLive` along with a  zookeeper path that 
must be unique for a given KafkaDataStore.  The Consumer should pass the returned SimpleFeature type to the call to
`Datastore.createSchema`.  For example:

    val ds: DataStore = ...
    val outputSFT: SimpleFeatureType = ...
    
    ...
    
    val zkPath = "/a/unique/zkpath"
    val preppedOutputSft = KafkaDataStoreHelper.prepareForLive(outputSFT, zkPath)
    if(!ds.getTypeNames.contains(outputSFTName)) ds.createSchema(preppedOutputSft)
    
###Data Consumers (Live Mode)

Data *consumers* that are running in *live* mode access the DataStore through the usual DataStore interface.

###Data Consumers (Replay Mode)

Data *consumers* that are running in *replay* mode will have to first create a new replay schema in the DataStore that 
is derived from the original schema.  The consumer should get the SimpleFeatureType for original schema and use it (
along with a `ReplayConfig`) to create the derived schema via a call to `DataStore.prepareForReplay()`.

    val datastore = ...
    val typename = "FrOut"
    val sft = ds.getSchema(typeName)
    val rc = new ReplayConfig(replayStart, replayEnd, readBehind)

    // create the replay schema from the original schema and replay params
    datastore.createSchema(KafkaDataStoreHelper.prepareForReplay(sft,rc))

It should be noted that the new schema will have a different type name and an additional attribute in the schema (time) 
from the original SimpleFeatureType