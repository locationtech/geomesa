.. _hbase_tools:

HBase Command-Line Tools
========================

The GeoMesa HBase distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`setting_up_hbase_commandline`.

Once installed, the tools should be available through the command ``geomesa-hbase``::

    $ geomesa-hbase
    INFO  Usage: geomesa-hbase [command] [command options]
      Commands:
        ...

Commands that are common to multiple back ends are described in :doc:`/user/cli/index`. The commands
here are HBase-specific.

General Arguments
-----------------

The HBase tools commands do not require connection arguments; instead they rely on an appropriate
``hbase-site.xml`` to be available on the classpath, as described in :ref:`setting_up_hbase_commandline`.

Commands
--------

``bulk-ingest``
^^^^^^^^^^^^^^^

Ingest data and write out HFiles, suitable for bulk loading into a cluster. Writing to offline HFiles instead
of directly to a running cluster can reduce the load on your cluster, and avoid costly data compactions.
See `Bulk Loading <http://hbase.apache.org/book.html#arch.bulk.load>`_ in the HBase documentation for more details
on the general concept.

A bulk ingest must be run as a map/reduce job. As such, ensure that your input files are staged in HDFS. Currently
only the GeoMesa converter framework is supported for bulk ingestion.

When running a bulk ingest, you should ensure that the data tables have appropriate splits, based on
your input. This will avoid creating extremely large files during the ingest, and will also prevent the cluster
from having to subsequently split the HFiles. See :ref:`table_split_config` for more information.

Currently HBase only supports writing out to a single table at one time. Because of this, a complete bulk load
will consist of running this command multiple times, once for each index table (e.g. ``z3``, ``id``, etc).

Once the files have been generated, use the ``bulk-load`` command (described below) to load them into the cluster.

``bulk-load``
^^^^^^^^^^^^^

Load HFiles into an HBase cluster. This command uses the HBase ``LoadIncrementalHFiles`` class to load the
data into the region servers. See the ``bulk-ingest`` command, above, for details on creating HFiles.

.. warning::

  This command may corrupt your cluster data. If possible, you should always back up your cluster before
  attempting a bulk load. If there are any errors or timeouts during the bulk load, you may need to use
  the HBase ``hbck`` command to repair the cluster.

Depending on the size of your data, you may need to modify the default HBase configuration settings
in order to successfully bulk load the files. This is done by modifying the ``hbase-site.xml`` file on the
GeoMesa tools classpath. The following properties are particularly relevant:

* ``hbase.rpc.timeout`` - may need to increase, especially if dealing with large HFiles or if using HBase on S3
* ``hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily`` - may need to increase, if ingesting a large
  number of HFiles
* ``hbase.loadincremental.threads.max`` - can increase to speed up the bulk load. Increasing to match the number of
  region servers may be appropriate.

``aws``
^^^^^^^

Bootstrap a AWS EMR cluster with GeoMesa on HBase installed and configured with optional notebook
servers Jupyter or Zeppelin. If the GeoMesa-HBase tools distribution is built with the python maven
profile then geomesa_pyspark will additionally be available in the notebook servers.

.. warning::

  This command is not intended for production use. It is intended to act as a quickstart and analyst tool, providing quick setup for ephemeral clusters.

In order to you this command you must have access to and the ability to use:

* AWS CLI Tools provided by Amazon and configured with your AWS credentials.
* Read and Write access to a S3 Bucket. Additionally, the EC2 VPC that will contain your EMR cluster must have read and write access to the same S3 Bucket.

.. note::

  If you wish to use GeoMesa's PySpark support you will need to rebuild the `geomesa-hbase-dist_$VERSION` with the `python` maven profile.

The complete list of parameters are available by running the command ``bin/geomesa-hbase aws``. Some of the key parameters are described here:

* ``--container`` - S3 Bucket URL to use as the HBase root and bootstrap working directory
* ``--read-only`` - Start HBase in read only mode. This will not permit ingest of data but allows multiple HBase clusters to read from the same S3 root directory.
* ``--jupyter`` - Specifying this parameter will install and configure a Jupyter notebook on the cluster master. Additionally, you can specify a password to use for the Jupyter server by providing a value to this parameter. (e.g. --jupyter=password) Default password: geomesa
* ``--zeppelin`` - Specifying this parameter will install and configure a Zeppelin notebook on the cluster master.
* ``--ec2-attributes`` - Additional EC2 attributes to pass to the 'ec2-attributes' parameter. Comma separated list or can be provided multiple times.
* ``--WorkerType`` - EC2 Instance type designation for the cluster. Default: 'm1.large'
* ``--WorkerCount`` - Number of workers to provision. Default: 1

This bootstrap process works by uploading the tools that invoke it to an S3 staging area. It then uses the AWS CLI to start up an EMR cluster and instructs it to run a GeoMesa bootstrap script. This script handles spawning the appropriate child scripts which configure the cluster.

Any of the ``aws-boostrap-geomesa-*`` scripts can be used on an existing cluster to bootstrap the respective functionality. Simply copy the ``geomesa-hbase-dist_$VERSION`` tarball to the master and run the desired script as root.
