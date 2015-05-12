# GeoMesa Kafka

### Alternate Kafka Consumer

GeoMesa provides an alternate kafka consumer that allows you fine-grained control over where you start
consuming messages. The high-level consumer provided by kafka only lets you start reading from where your
group left off.

### Configuration

The GeoMesa kafka consumer is ```org.locationtech.geomesa.kafka.consumer.KafkaConsumer```.
Typical use is:

```java
val consumerProps = new Properties
consumerProps.put("group.id", group)
consumerProps.put("metadata.broker.list", brokers)
consumerProps.put("zookeeper.connect", zookeepers)
consumerProps.put("num.consumer.fetchers", "1")
val config = new ConsumerConfig(consumerProps)

val consumer = new KafkaConsumer(topic, config, new StringDecoder, new StringDecoder)
// stream contains your messages
val stream = consumer.createMessageStreams(1, EarliestOffset).head
```

Available offsets are:
*  ```EarliestOffset``` - will start from the first message still available in the kafka log
*  ```LatestOffset``` - will ignore any existing messages and only consume newly written messages
*  ```GroupOffset``` - will start reading messages based on the last message read by the group
*  ```DateOffset(date: Long)``` - will start reading messages based on the date the message was written
    NOTE: date offset is only to the kafka log level resolution, which is fairly coarse
*  ```FindOffset(predicate: MessagePredicate)``` - will do a binary search across all available messages
    using the provided predicate. Note that messages must be in sorted order according to the predicate.

Offsets can also be configured via typesafe config:
```
val conf = ConfigFactory...
RequestedOffset(conf)
```
The configuration is expected under the key 'offset'. Valid values are:
*  earliest 
*  latest
*  group
*  date:1234 - number is Java Date milliseconds
*  com.example.Predicate - a class that implements org.locationtech.geomesa.kafka.consumer.offsets.FindMessage

The offset can also be overwritten using the system property 'geomesa.kafka.offset', set to one of the above.
