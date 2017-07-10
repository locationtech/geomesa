FileSystem Datastore Architecture
=================================

The GeoMesa FileSystem Datastore (GeoMesa FSDS) takes advantage of the performance characteristics of modern
cloud-native and distributed filesystems to scale bulk analytic queries. The FSDS is a good choice for doing bulk egress
queries or large analytic jobs using frameworks such as Spark SQL and MapReduce. The FSDS differs from other queries
in that ingest latency is traded for high-throughput query. The FSDS pairs well with low-latency ingest systems such as
HBase or Kafka via the GeoMesa Lambda datastore to provide an optimal pairing of "hot", "warm" storage options.


The GeoMesa FSDS consists of a few primary components:

* **FileSystem** - A separately managed storage system that implements the GeoMesa FileSystem API
* **Partition Scheme** - A stategy for laying out data on within the filesystem
* **Storage Format** - A defined format or encoding to store data in files
* **Query Engine** - A query engine or client to fulfill queries and run analytic jobs


FileSystem
----------

GeoMesa FSDS can utilize any filesystem that implements the Hadoop FileSystem API. The most common filesystems used
with GeoMesa FSDS are:

* HDFS - Hadoop Distributed File System
* S3 - Amazon Simple Storage
* GCS - Google Cloud Storage
* WASB - Windows Azure Blob Store

Choosing a filesystem depends generally on cost and performance requirements. One thing to note is that S3, GCS, and
WASB are all "cloud-native" storage meaning that they are built into Amazon, Google, and Microsoft Azure cloud
platforms. These cloud-native filesystems are scaled separately from the compute nodes which generally provides a more
cost efficient storage solution. In general their price per GigaByte of storage is lower then using HDFS though they
may be slightly more latent. They also have the ability to persist data when after you turn off all your compute nodes.

Any of the filesystems mentioned about are good choices for the FSDS. If you have more questions about making a choice
contact the GeoMesa team.

Partition Schemes
-----------------

The partition scheme defines how data is stored within the filesystem. The scheme is important because it defines how
the data is queried. Most OLAP queries in GeoMesa contain a date range and geometric predicate and the partition scheme
can aide in finding the files satisfy the query. There are two main partition schmes used with GeoMesa FSDS:

* **Date** - partition data by a Date attribute
* **Geometry (Z2)** - partition data by its geometric coordinates using a Z2 space filling curve
* **Combined Date and Geometry** - partition data using a combined data-time scheme

The partition scheme must be provided at ingest time. Examples of common schemes are:

* **Hourly** - Store a file for each hour of each day
* **Daily** - Store a single file for each day of the year
* **Day with Z2** - Store a file each day for each region of the world (at some precision of geometry)

More information on defining partition schemes can be found in the link (commandline tools) and
partition schemes documentation.


Storage Formats
---------------

* **Apache Parquet** - Apache Parquet is the leading interoperable columnar format in the Hadoop ecosystem. It provides
efficient compression, storage, and query of structured data. Apache Parquet is currently the only format that can be
used for writing data into the FileSystem datastore.

* **Converter Storage** - The converter storage format is a synthetic format which allows you to overlay a GeoMesa converter
on top of a filesystem using a defined partition scheme. This allows you to utilize existing data storage layouts of
data stored in JSON, CSV, TSV, Avro, or other formats. Converters are pluggable allowing users to expose their own
custom storage formats if desired.
