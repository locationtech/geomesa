GeoMesa FileSystem Data Store with Spark SQL
============================================

In this example we will ingest and query GDELT data using the GeoMesa FileSystem datastore backed by Amazon S3. The
GeoMesa FileSystem Datastore provides an performant and cost-efficient solution for large SQL queries over datasets
like GDELT using Amazon's Elastic Map Reduce (EMR) framework.

Ingesting GDELT
---------------

To begin, we spin up a few nodes with EMR and then use the AWS CLI to few the public GDELT buckets as instructed on
the GDELT S3 page: https://aws.amazon.com/public-datasets/gdelt/

.. code-block:: bash

    aws s3 ls gdelt-open-data/events/

Files appear to be yearly, monthly, or daily CSV files based on the recency of the data. We'll use the built in GeoMesa
GDELT SimpleFeatureType and converter to ingest a year or so of data:

.. code-block:: bash

    tar xvf geomesa-fs-dist_2.11-$VERSION.tar.gz
    . /etc/hadoop/conf/hadoop-env.sh
    . /etc/hadoop/conf/yarn-env.sh
    export HADOOP_CONF_DIR=/etc/hadoop/conf

    bin/geomesa-fs ingest -e parquet --partition-scheme daily,z2-2bit -p s3a://ccri-data/tmp/hulbert/3 \
    -C gdelt -s gdelt \
    --num-reducers 60 s3a://gdelt-open-data/events/2017*


Querying GDELT with Spark SQL
-----------------------------

The GeoMesa FileSystem distribution includes a shaded Spark jar to be included with the ``--jars`` option of
spark-shell.

.. code-block:: bash

    $ ls dist/spark
    geomesa-fs-spark_2.11-$VERSION.jar  geomesa-fs-spark-runtime_2.11-$VERSION.jar

.. note::

    The spark runtime for GeoMesa FileSystem Datastore shaded parquet 1.9.0 in order to provide dictionary
    query support. Spark 2.1.1 currently ships with the 1.8.x series of parquet.

For example if you are using Amazon EMR with Spark 2.1.1 you can start up a shell with GeoMesa support like this:

.. code-block:: bash

    . /etc/hadoop/conf/hadoop-env.sh
    . /etc/hadoop/conf/yarn-env.sh
    export HADOOP_CONF_DIR=/etc/hadoop/conf
    spark-shell --jars $GEOMESA_FS_HOME/dist/spark/geomesa-fs-spark-runtime_2.11-$VERSION.jar

This will create a new spark shell from which you can load a GeoMesa FileSystem datastore from S3 or HDFS. A common
usage pattern is to keep parquet files in S3 so they can be elastically queried with EMR and Spark. For example if you
have ingested Twitter data into S3 you can query it with SQL in the spark shell::

    val dataFrame = spark.read
      .format("geomesa")
      .option("fs.encoding","parquet")
      .option("fs.path","s3a://mybucket/geomesa/datastore")
      .option("geomesa.feature", "gdelt")
      .load()
    dataFrame.createOrReplaceTempView("gdelt")

    // Select
    spark.sql("select eventCode, count(eventCode) from gdelt where dtg >= cast('2017-06-01' as timestamp)" +
              "and dtg <= cast('2017-06-31' as timestamp) group by eventcode").show()

