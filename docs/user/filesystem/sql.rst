GeoMesa FileSystem Data Store with Spark SQL
============================================

The GeoMesa FileSystem Data Store serves as an effective source of data for large SQL queries in Spark. The distribution
includes a shaded Spark jar to be included with the ``--jars`` option of spark-shell. For example if you are using
Amazon EMR with Spark 2.1.1 you can start up a shell with GeoMesa support like this:

.. code-block:: bash

    $ tar xvf geomesa-fs-dist_2.11-$VERSION.tar.gz
    $ ls dist/spark
    geomesa-fs-spark_2.11-$VERSION.jar  geomesa-fs-spark-runtime_2.11-$VERSION.jar

    $ . /etc/hadoop/conf/hadoop-env.sh
    $ . /etc/hadoop/conf/yarn-env.sh
    $ export HADOOP_CONF_DIR=/etc/hadoop/conf

    $ spark-shell --jars $GEOMESA_FS_HOME/dist/spark/geomesa-fs-spark-runtime_2.11-$VERSION.jar

This will create a new spark shell from which you can load a GeoMesa FileSystem datastore from S3 or HDFS. A common
usage pattern is to keep parquet files in S3 so they can be elastically queried with EMR and Spark. For example if you
have ingested Twitter data into S3 you can query it with SQL in the spark shell::

    val dataFrame = spark.read
      .format("geomesa")
      .option("fs.encoding","parquet")
      .option("fs.path","s3a://mybucket/datastore")
      .option("geomesa.feature", "twitter")
      .load()
    dataFrame.createOrReplaceTempView("twitter")

    // Select all tweets from a month of twitter
    spark.sql("select user_name from twitter where Time > cast('2016-09-01' as timestamp) and Time < cast('2016-10-01' as timestamp)").show()

