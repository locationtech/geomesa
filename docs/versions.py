# NB: This file is preprocessed by Maven; the version that is used
# by Sphinx is built in target/versions.py

# release: The full version, including alpha/beta/rc tags
# version: The short X.Y version (we just use the full version)
release = '${project.version}'
version = '${project.version}'
scala_binary_version = '${scala.binary.version}'
maven_min_version = '${maven.min.version}'

copyright_year = '${copyright.year}'

accumulo_version_full = '${accumulo.version.recommended}'
accumulo_version = accumulo_version_full[:accumulo_version_full.rindex('.')]
cassandra_version_full = '${cassandra.server.version.recommended}'
cassandra_version = cassandra_version_full[:cassandra_version_full.rindex('.')]
geoserver_version = '${geoserver.version}'
hadoop_min_version = '${hadoop.min.version}'
hbase_version_full = '${hbase.version.recommended}'
hbase_version = hbase_version_full[:hbase_version_full.rindex('.')]
kafka_min_version = '${kafka.min.version}'
micrometer_version = '${micrometer.version}'
prometheus_version = '${prometheus.version}'
redis_min_version = '${redis.min.version}'
spark_version_full = '${spark.version}'
spark_version = spark_version_full[:spark_version_full.rindex('.')]
