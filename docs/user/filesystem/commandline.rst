GeoMesa FileSystem CommandLine Tools
====================================

Building from Source
--------------------

To build from source make sure you have Maven, Git, and Java SDK 1.8 installed. To build just the GeoMesa FileSystem
distribution do this:

.. code-block:: bash

    git clone git@github.com:locationtech/geomesa.git
    cd geomesa
    build/mvn clean install -pl geomesa-fs/geomesa-fs-dist -am -T4

The tarball can also be downloaded from geomesa.org.

Installation
------------

To install the command line tools simply untar the distribution tarball:

.. code-block:: bash

    # If building from source
    tar xvf geomesa-fs/geomesa-fs-dist/target/geomesa-fs_2.11-$VERSION-dist.tar.gz
    export GEOMESA_FS_HOME=/path/to/geomesa-fs_2.11-$VERSION

After untaring the archive you'll need to either define the standard Hadoop environmental variables or install hadoop
using the ``bin/install-hadoop.sh`` script provided in the tarball. Note that you will need the proper Yarn/Hadoop
environment configured if you would like to run a distributed ingest job to create files.

If you are using a service such as Amazon Elastic MapReduce (EMR) or have a distribution of Apache Hadoop, Cloudera, or
Hortonworks installed you can likely run something like this to configure hadoop for the tools:

.. code-block:: bash

    # These will be specific to your Hadoop environment
    . /etc/hadoop/conf/hadoop-env.sh
    . /etc/hadoop/conf/yarn-env.sh
    export HADOOP_CONF_DIR=/etc/hadoop/conf

After installing the tarball you should be able to run the ``geomesa-fs`` command like this:

.. code-block:: bash

    $ cd $GEOMESA_FS_HOME
    $ bin/geomesa-fs

The output should look like this::

    INFO  Usage: geomesa-fs [command] [command options]
      Commands:
        ...

Command Overview
----------------

configure
~~~~~~~~~

Used to configure the current environment for using the commandline tools. This is frequently run after the tools are
first installed to ensure the environment is configured correctly::

    $ geomesa-fs configure

classpath
~~~~~~~~~

Prints out the current classpath configuration::

    $ geomesa-fs classpath

.. _fsds_ingest_command:

ingest
~~~~~~

Ingests files into a GeoMesa FS Datastore. Note that a "datastore" is simply a path in the filesystem. All data and
metadata will be stored in the filesystem under the hierarchy of the root path. Before ingesting data you will need
to define a SimpleFeatureType schema and a GeoMesa converter for your data. Many default type schemas and converters
for formats such as gdelt, twitter, etc. are provided in the conf directory of GeoMesa.

If you are not using one of these data types you can provide a file containing your schema and converter or configure
the GeoMesa tools to reference the schema configuration by name (see :ref:`fsds_partition_schemes`). Schemas files
can also be stored in a remote filesystem such as HDFS, S3, GCS, or WASB.

Commonly used arguments for ingest are:

* ``-p`` - Root path for storage (e.g. s3a://bucket/datastores/myds)
* ``-e`` - Encoding to use for file storage (e.g. parquet)
* ``--partition-scheme`` - Common partition scheme name (e.g. daily, z2) or path to a file containing a scheme config
* ``--temp-dir`` - A temp dir in HDFS to use when doing S3 ingest (can speed up writes for parquet)
* ``--num-reducers`` - The number of reducers to use when performing distributed ingest (try to set to num-partitions / 2)
* ``-C`` - Path to a converter or named convert available in the environment
* ``-s`` - Path to a SimpleFeatureType config or named type available in the environment

For example lets say we have all our data for 2016 stored in an S3 bucket::

    geomesa-fs ingest \
      -p 's3a://mybucket/datastores/test' \
      -e parquet \
      --partition-scheme daily,z2-2bits \
      -s s3a://mybucket/schemas/my-config.conf \
      -C s3a://mybucket/schemas/my-config.conf \
      --temp-dir hdfs://namenode:port/tmp/gm/1 \
      --num-reducers 20 \
      's3a://mybucket/data/2016/*'


After ingest we expect to see a file structure with metadata and parquet files in S3 for our type named "myfeature"::

    aws s3 ls --recursive s3://mybucket/datastores/test

    datastores/test/myfeature/schema.sft
    datastores/test/myfeature/metadata
    datastores/test/myfeature/2016/01/01/0/0000.parquet
    datastores/test/myfeature/2016/01/01/2/0000.parquet
    datastores/test/myfeature/2016/01/01/3/0000.parquet
    datastores/test/myfeature/2016/01/02/0/0000.parquet
    datastores/test/myfeature/2016/01/02/1/0000.parquet
    datastores/test/myfeature/2016/01/02/3/0000.parquet

Two metadata files (``schema.sft`` and ``metadata``) store information about the schema, partition scheme, and list of
files that have been created. Note that the list of created files allows the datastore to quickly compute available
files to avoid possibly expensive directly listings against the filesystem. You may need to run update-metadata if you
decide to insert new files.

Notice that the bucket "directory structure" includes year, month, day and then a 0,1,2,3 representing a quadrant of the
Z2 Space Filling Curve with 2bit resolution (i.e. 0 = lower left, 1 = lower right, 2 = upper left, 3 = upper right).
Note that in our example January 1st and 2nd both do not have all four quadrants represented. This means that the input
dataset for that day didn't have any data in that region of the world. If additional data were ingested, the directory
and a corresponding file would be created.

update-metadata
~~~~~~~~~~~~~~~

Recompute the list of partitions stored within the metadata file in a filesystem datastore. This metadata file
is used at query time in lieu of performing repeated directory listings.

export
~~~~~~

Export GeoMesa features. Commonly used arguments to control export are:

* ``-a`` - A comma-separated list of attributes and/or filter functions to export (e.g. geom,dtg,user_name)
* ``-q``` - a GeoTools CQL query to select rows of data from the datastore
* ``-F`` - an output format (e.g. csv, tsv, avro)
* ``--query-threads`` - The number of threads to read files from the datastore
* ``-m`` - Maxiumum number of features to export

Example export commands::

    $ geomesa-fs export  \
      -p 's3a://mybucket/datastores/myds' \
      -e parquet \
      -f test_feature

    $ geomesa-fs \
      -p 's3a://mybucket/datastores/myds' \
      -e parquet \
      -f test_feature \
      -F TSV \
      -q "dtg >= '2016-01-02' and dtg < '2016-01-10' and bbox(geom, -5, -5, 50, 50)"

version
~~~~~~~

Prints out the version, git branch, and commit ID that the tools were built with::

    $ geomesa-fs version


