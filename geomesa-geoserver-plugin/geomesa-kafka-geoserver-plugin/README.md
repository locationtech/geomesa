###Installation

To install in geoserver, extract and copy all jars from ```target/geomesa-kafka-geoserver-plugin-<version>-geoserver-plugin.zip``` to
geoserver/WEB-INF/lib.

You will also need to install the the following jars into geoserver/WEB-INF/lib for kafka 0.8.2.1:
* kafka-clients-0.8.2.1.jar
* kafka_2.10-0.8.2.1.jar
* metrics-core-2.2.0.jar
* zkclient-0.3.jar
* zookeeper-3.4.5.jar
