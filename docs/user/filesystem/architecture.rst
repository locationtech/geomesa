Architecture
============

The GeoMesa FileSystem Datastore (GeoMesa FSDS) takes advantage of the performance characteristics of modern
cloud-native and distributed filesystems to scale bulk analytic queries. The FSDS is a good choice for doing bulk egress
queries or large analytic jobs using frameworks such as Spark SQL and MapReduce. The FSDS differs from other datastores
in that ingest and point query latencies are traded for high-throughput query performance. The FSDS pairs well with
low-latency ingest and cache-based datastores systems such as HBase or Kafka to provide an optimal pairing of "hot" and
"warm" storage options. This pairing is commonly known as a Lambda Architecture.

The GeoMesa FSDS consists of a few primary components:

* **FileSystem** - A separately managed storage system that implements the GeoMesa FileSystem API
* **Partition Scheme** - A strategy for laying out data on the filesystem
* **Storage Format** - A defined format or encoding to store data in files
* **Query Engine** - A query engine or client to fulfill queries and run analytic jobs

FileSystem
----------

GeoMesa FSDS can utilize any filesystem that implements the Hadoop FileSystem API. The most common filesystems used
with GeoMesa FSDS are:

* **HDFS** - Hadoop Distributed File System
* **S3** - Amazon Simple Storage
* **GCS** - Google Cloud Storage
* **WASB** - Windows Azure Blob Storage
* **Local** - Locally mounted file system (e.g. local disk or NFS)

Choosing a filesystem depends generally on cost and performance requirements. One thing to note is that S3, GCS, and
WASB are all "cloud-native" storage meaning that they are built into Amazon, Google, and Microsoft Azure cloud
platforms. These cloud-native filesystems are scaled separately from the compute nodes which generally provides a more
cost efficient storage solution. Compared to HDFS, their price per GB of storage is lower but their latency is
higher. They also have the ability to persist data after you turn off all your compute nodes.

Any of the filesystems mentioned about are good choices for the FSDS. If you have more questions about making a choice
contact the `GeoMesa team <http://www.geomesa.org/community/>`__

Partition Schemes
-----------------

Partition schemes define how data is stored on the filesystem. The scheme is important because it determines how
the data is queried. When evaluating a query filter, the partition scheme is leveraged to prune data files that
do not match the filter. For more information, see :ref:`fsds_partition_schemes`.

Metadata
--------

The FSDS stores metadata about partitions and data files, to avoid having to repeatedly interrogate the filesystem.
By default, metadata information is stored as a change log in the file system. For more advanced use-cases, the
FSDS also supports using a relational database. For more information, see :ref:`fsds_metadata`.

Storage Formats
---------------

* **Apache Parquet** - Apache Parquet is the leading interoperable columnar format in the Hadoop ecosystem. It
  provides efficient compression, storage, and query of structured data.

* **Apache ORC** - Apache ORC is a self-describing type-aware columnar file format designed for Hadoop workloads. It
  is optimized for large streaming reads, but with integrated support for finding required rows quickly.

* **Converter Storage** - The converter storage format is a synthetic format which allows you to overlay a GeoMesa
  converter on top of a filesystem using a defined partition scheme. This allows you to utilize existing data stored
  in JSON, CSV, TSV, Avro, or other formats. Converters are extensible, allowing users to expose their own custom
  storage formats if desired. For more details on converters, see :ref:`converters`. Converter storage is a
  read-only format.
