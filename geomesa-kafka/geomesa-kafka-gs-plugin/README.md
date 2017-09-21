###Installation

To install in geoserver, extract and copy all jars from ```target/geomesa-kafka-gs-plugin-install.tar.gz``` to
geoserver/WEB-INF/lib.

You will also need to install the the following jars into geoserver/WEB-INF/lib for kafka ${kafka.version}:
* kafka-clients-${kafka.version}.jar
* kafka_2.11-${kafka.version}.jar
* metrics-core-2.2.0.jar
* zkclient-0.7.jar
* zookeeper-3.4.5.jar