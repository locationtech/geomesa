.. _fsds_sparksql_example:

FileSystem Data Store Spark SQL Example
=======================================

In this example we will ingest and query GDELT data using the GeoMesa FileSystem data store backed by Amazon S3. The
GeoMesa FileSystem data store provides a performant and cost-efficient solution for large SQL queries over datasets
like GDELT using Amazon's Elastic Map Reduce (EMR) framework.

Ingesting GDELT
---------------

To begin, we spin up a few nodes with EMR and then use the AWS CLI to find the public GDELT buckets as instructed on
the GDELT S3 page: https://aws.amazon.com/public-datasets/gdelt/

.. code-block:: bash

    aws s3 ls gdelt-open-data/events/

Files appear to be yearly, monthly, or daily CSV files based on the recency of the data. We'll use the built in GeoMesa
GDELT SimpleFeatureType and converter to ingest a year or so of data:

.. code-block:: bash

    tar xvf geomesa-fs_2.11-$VERSION.tar.gz
    . /etc/hadoop/conf/hadoop-env.sh
    . /etc/hadoop/conf/yarn-env.sh
    export HADOOP_CONF_DIR=/etc/hadoop/conf

.. note::

    We use a temp directory on EMR that utilizes HDFS as intermediate storage to speed up output of parquet files. This
    is recommended by many vendors including HortonWorks Data Platform.

.. code-block:: bash

    bin/geomesa-fs ingest \
    -e parquet --partition-scheme daily,z2-2bit -p s3a://ccri-data/tmp/hulbert/3 \
    --temp-path hdfs:///tmp/geomesa/1 \
    -C gdelt -s gdelt \
    --num-reducers 60 s3a://gdelt-open-data/events/2017*


Querying GDELT with Spark SQL
-----------------------------

The GeoMesa FileSystem distribution includes a shaded Spark jar to be included with the ``--jars`` option of
spark-shell.

.. code-block:: bash

    $ ls dist/spark
    geomesa-fs-spark_2.11-$VERSION.jar  geomesa-fs-spark-runtime_2.11-$VERSION.jar

For example if you are using Amazon EMR with Spark 2.1.1 you can start up a shell with GeoMesa support like this:

.. code-block:: bash

    . /etc/hadoop/conf/hadoop-env.sh
    . /etc/hadoop/conf/yarn-env.sh
    export HADOOP_CONF_DIR=/etc/hadoop/conf
    spark-shell --jars $GEOMESA_FS_HOME/dist/spark/geomesa-fs-spark-runtime_2.11-$VERSION.jar --driver-memory 3g

This will create a new spark shell from which you can load a GeoMesa FileSystem datastore from S3 or HDFS. A common
usage pattern is to keep parquet files in S3 so they can be elastically queried with EMR and Spark. For example if you
have ingested GDELT data into S3 you can query it with SQL in the spark shell::

    val dataFrame = spark.read
      .format("geomesa")
      .option("fs.path","s3a://mybucket/geomesa/datastore")
      .option("geomesa.feature", "gdelt")
      .load()
    dataFrame.createOrReplaceTempView("gdelt")

    // Select the top event codes
    spark.sql("SELECT eventCode, count(*) as count FROM gdelt " +
              "WHERE dtg >= '2017-06-01T00:00:00Z' AND dtg <= '2017-06-30T00:00:00Z' " +
              "GROUP BY eventcode ORDER by count DESC").show()

