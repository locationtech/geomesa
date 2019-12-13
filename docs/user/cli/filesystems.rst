Remote File System Support
==========================

Through Hadoop's file system support, GeoMesa supports ingesting files directly from remote file systems, including
Amazon's S3 and Microsoft's Azure.

Note: the examples below use the Accumulo tools, but should work with any other distribution as well.

Enabling S3 Ingest
------------------

Hadoop ships with implementations of S3-based filesystems, which can be enabled in the Hadoop configuration used with
GeoMesa tools. Specifically, GeoMesa tools can perform ingests using both the second-generation (`s3n`) and
third-generation (`s3a`) filesystems. Edit the ``$HADOOP_CONF_DIR/core-site.xml`` file in your Hadoop installation,
as shown below (these instructions apply to Hadoop 2.5.0 and higher). Note that you must have the environment variable
``$HADOOP_MAPRED_HOME`` set properly in your environment. Some configurations
can substitute ``$HADOOP_PREFIX`` in the classpath values below.

.. warning::

    AWS credentials are valuable! They pay for services and control read and write protection for data. If you are
    running GeoMesa on AWS EC2 instances, it is recommended to use the ``s3a`` filesystem. With ``s3a``, you can omit the
    Access Key Id and Secret Access keys from `core-site.xml` and rely on IAM roles.

Configuration
^^^^^^^^^^^^^

For ``s3a``:

.. code-block:: xml

    <!-- core-site.xml -->
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*</value>
        <description>The classpath specifically for Map-Reduce jobs. This override is needed so that s3 URLs work on Hadoop 2.6.0+</description>
    </property>

    <!-- OMIT these keys if running on AWS EC2; use IAM roles instead -->
    <property>
        <name>fs.s3a.access.key</name>
        <value>XXXX YOURS HERE</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>XXXX YOURS HERE</value>
        <description>Valuable credential - do not commit to CM</description>
    </property>

After you have enabled S3 in your Hadoop configuration you can ingest with GeoMesa tools. Note that you can still
use the Kleene star (*) with S3.:

.. code-block:: bash

    $ geomesa-accumulo ingest -u username -p password -c geomesa_catalog -i instance -s yourspec -C convert s3a://bucket/path/file*

For ``s3n``:

.. code-block:: xml

    <!-- core-site.xml -->
    <!-- Note that you need to make sure HADOOP_MAPRED_HOME is set or some other way of getting this on the classpath -->
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*</value>
        <description>The classpath specifically for map-reduce jobs. This override is needed so that s3 URLs work on hadoop 2.6.0+</description>
    </property>
    <property>
        <name>fs.s3n.impl</name>
        <value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value>
        <description>Tell hadoop which class to use to access s3 URLs. This change became necessary in hadoop 2.6.0</description>
    </property>
    <property>
        <name>fs.s3n.awsAccessKeyId</name>
        <value>XXXX YOURS HERE</value>
    </property>
    <property>
        <name>fs.s3n.awsSecretAccessKey</name>
        <value>XXXX YOURS HERE</value>
    </property>

S3n paths are prefixed in hadoop with ``s3n://`` as shown below::

    $ geomesa-accumulo ingest -u username -p password \
      -c geomesa_catalog -i instance -s yourspec \
      -C convert s3n://bucket/path/file s3n://bucket/path/*

Enabling Azure Ingest
---------------------

Hadoop ships with implementations of Azure-based filesystems, which can be enabled in the Hadoop configuration used with
GeoMesa tools. Specifically, GeoMesa tools can perform ingests using the ``wasb`` and ``wasbs`` filesystems.
Edit the ``$HADOOP_CONF_DIR/core-site.xml`` file in your Hadoop installation as shown below
(these instructions apply to Hadoop 2.5.0 and higher). In addition, the hadoop-azure and azure-storage JARs need to be
available.

.. warning::

    Azure credentials are valuable! They pay for services and control read and write protection for data. Be sure to keep
    your core-site.xml configuration file safe. It is recommended that you use Azure's SSL enable file protocol
    variant ``wasbs`` where possible.

Configuration
^^^^^^^^^^^^^

To enable, place the following in your Hadoop Installation's core-site.xml.

.. code-block:: xml

    <!-- core-site.xml -->
    <property>
      <name>fs.azure.account.key.ACCOUNTNAME.blob.core.windows.net</name>
      <value>XXXX YOUR ACCOUNT KEY</value>
    </property>

After you have enabled Azure in your Hadoop configuration you can ingest with GeoMesa tools. Note that you can still
use the Kleene star (*) with Azure.:

.. code-block:: bash

    $ geomesa-accumulo ingest -u username -p password \
      -c geomesa_catalog -i instance -s yourspec \
      -C convert wasb://CONTAINER@ACCOUNTNAME.blob.core.windows.net/files/*
