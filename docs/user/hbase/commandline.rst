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
