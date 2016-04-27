###Installation

To install in geoserver, extract and copy all jars from ```target/geomesa-kafka-gs-plugin-<version>-install.tar.gz``` to
geoserver/WEB-INF/lib.

You will also need to install the the following jars into geoserver/WEB-INF/lib for kafka 0.8.2.1:
* kafka-clients-0.8.2.1.jar
* kafka_2.11-0.8.2.1.jar
* metrics-core-2.2.0.jar
* zkclient-0.3.jar
* zookeeper-3.4.5.jar

For kafka 0.9.0.1 you will need these instead:
* geomesa-kafka-09-utils-<version>.jar
* kafka-clients-0.9.0.1.jar
* kafka_2.11-0.9.0.1.jar
* metrics-core-2.2.0.jar
* zkclient-0.7.jar
* zookeeper-3.4.5.jar