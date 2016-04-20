###Installation

To install in geoserver, extract and copy all jars from ```target/geomesa-kafka-09-gs-plugin-<version>-install.tar.gz``` to
geoserver/WEB-INF/lib.

You will also need to install the the following jars into geoserver/WEB-INF/lib for kafka 0.9.0.1:
* kafka-clients-0.9.0.1.jar
* kafka_2.11-0.9.0.1.jar
* metrics-core-2.2.0.jar
* zkclient-0.7.jar
* zookeeper-3.4.6.jar
