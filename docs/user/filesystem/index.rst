FileSystem Data Store
=====================

The GeoMesa FileSystem data store (FSDS) provides a cost-effective, performant solution for massive OLAP-style spatial
data analytics using frameworks such as Apache Spark. It utilizes modern columnar formats that enable data compression
and column-based encodings for efficient OLAP queries. GeoMesa FSDS can run on top of any distributed or local
filesystem including Amazon S3, Hadoop HDFS, Google FileStorage, and Azure BlobStore. This enables users to realize
cost savings through the utilization of elastic compute resources in lieu of dedicated servers.

The FileSystem datastore can be used to ETL new data into a filesystem or  wrap existing file system storage directories
using a GeoMesa converter. In both modes the user must provide a defined partition scheme describing the layout of the
directory structures.

.. toctree::
    :maxdepth: 1

    architecture
    install
    index_config
    partition_schemes
    metadata
    usage
    geoserver
    commandline
    configuration
    example
    spark_example
