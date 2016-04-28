###Installation

To install in geoserver, extract and copy all jars from ```target/geomesa-kafka-09-gs-plugin-<version>-install.tar.gz``` to
geoserver/WEB-INF/lib.

For kafka 0.9.0.1 you will need these instead:
* kafka-clients-0.9.0.1.jar
* kafka_2.11-0.9.0.1.jar
* metrics-core-2.2.0.jar
* zkclient-0.7.jar
* zookeeper-3.4.5.jar