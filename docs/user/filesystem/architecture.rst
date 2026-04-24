Architecture
============

The GeoMesa FileSystem Datastore (GeoMesa FSDS) takes advantage of the performance characteristics of modern
cloud-native object stores to scale bulk analytic queries. The FSDS is a good choice for doing bulk egress
queries or large analytic jobs using frameworks such as Spark SQL and MapReduce. The FSDS differs from other datastores
in that ingest and point query latencies are traded for high-throughput query performance. The FSDS pairs well with
low-latency ingest and cache-based datastores systems such as HBase or Kafka to provide an optimal pairing of "hot" and
"warm" storage options. This pairing is commonly known as a Lambda Architecture.

The GeoMesa FSDS consists of a few primary components:

* **FileSystem** - A separately managed storage system that implements the GeoMesa FileSystem API
* **Partition Scheme** - A strategy for grouping individual records into files in order to accelerate queries
* **Query Engine** - A query engine or client to fulfill queries and run analytic jobs

FileSystem
----------

GeoMesa FSDS currently supports the following filesystems:

* **S3** - Amazon Simple Storage
* **Local** - Locally mounted file system (e.g. local disk or NFS)

Support for additional object stores may be added in the future.

Partition Schemes
-----------------

Partition schemes define how data is stored on the filesystem. The scheme is important because it determines how
the data is queried. When evaluating a query filter, the partition scheme is leveraged to prune data files that
do not match the filter. For more information, see :ref:`fsds_partition_schemes`.

Metadata
--------

The FSDS stores metadata about partitions and data files, to avoid having to repeatedly interrogate the filesystem.
For ease of use, metadata information can be stored as a change log in the file system, which does not require any additional
infrastructure. For improved performance, metadata can instead be stored in a relational database. For more information,
see :ref:`fsds_metadata`.

Storage Formats
---------------

* **Apache Parquet** - Apache Parquet is the leading interoperable columnar format. It provides efficient compression,
  storage, and query of structured data.

* **Converter Storage** - The converter storage format is a synthetic format which allows you to overlay a GeoMesa
  converter on top of a filesystem using a defined partition scheme. This allows you to utilize existing data stored
  in JSON, CSV, TSV, Avro, or other formats. Converters are extensible, allowing users to expose their own custom
  storage formats if desired. For more details on converters, see :ref:`converters`. Converter storage is a
  read-only format.
