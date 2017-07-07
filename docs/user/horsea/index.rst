FileSystem Datastore
====================

The GeoMesa FileSystem Datastore provides a cost-effective, performant solution for massive OLAP-style spatial
data analytics using frameworks such as Apache Spark. It utilizes modern columnar formats that enable data compression
and column-based encodings for efficient OLAP queries. GeoMesa DFDS can run on top of any distributed or local
filesystem including Amazon S3, Hadoop HDFS, Google FileStorage, and Azure BlobStore. This enables users to realize
cost savings through the utilization of elastic compute resources in lieu of dedicated servers

The FileSystem datastore can be used to ETL new data into a filesystem or  wrap existing file system storage directories
using a GeoMesa converter. In both modes the user must provide a defined partition scheme describing the layout of the
directory structures.

Partition Schemes
-----------------

GeoMesa FS ships with datetime and z-order partition schemes that organize data on disk to facilitate selection of
files for query. These schemes are configured via typesafe/json config and are used when a new datastore is created. The
datetime scheme creates folders or bucket structures based on the a date format. The Z2 storage scheme creates buckets
based on the geographic coordinates in the data.The datetime and z2 schemes can be combined using the datetime-z2 scheme
which uses a primary date layout with secondary z-order layout.

Common Options
``````````````

* ``name`` - The name of the partition scheme
* ``leaf-storage`` - Defines if the partition scheme can store 1 file or multiple files per partition

  * ``true`` - if the leaves of the storage scheme are files and contain data
  * ``false`` - if the scheme defines a leaf "bucket" or directory containing multiple files.
             This should be used if you want to append to the datastore.


Datetime Scheme
```````````````

The datetime scheme is indicated by the key ``name = "datetime"`` and uses the following options:

* ``datetime-format`` - A Java DateTime format string separated by forward slashes which will be used to build a
  directory structure. Examples are:

  * ``yyyy/DDD/HH`` - year, JDate, hour layout
  * ``yyyy/MM/dd`` - year, month, day layout
  * ``yyyy/MM/dd/HH`` - year, month, day, hour layout

* ``step-unit`` - A ``java.time.temporal.ChronoUnit`` defining how to increment the leaf of the partition scheme
* ``step`` - The step amount to increment the leaf of the partition scheme
* ``dtg-attribute`` - The datetime attribute from the SimpleFeature to use for partitioning data

.. code-block:: json

    {
      name = "datetime"
      opts = {
        datetime-format = "yyyy/DDD/HH"
        step-unit = HOURS
        step = 1
        dtg-attribute = dtg
        leaf-storage = false
      }
    }

Z2 Scheme
`````````

The z2 scheme is indicate by the key ``name = "z2"`` and uses the following options:


* ``geom-attribute`` - The geometry attribute from the SimpleFeature to use for partitioning data
* ``z2-resolution`` - The number of bits to use for z indexing.


.. code-block:: json

    {
      name = "z2"
      opts = {
        geom-attribute = geom
        z2-resolution = 2
        leaf-storage = false
      }
    }

Datetime-Z2 Scheme
``````````````````

The datetime-z2 scheme is indicated by the key ``name = "datetime-z2"`` and uses the combined options
from the datetime and z2 schemes.

.. code-block:: json

    {
      name = "datetime-z2"
      opts = {
        datetime-format = "yyyy/dd/HH"
        step-unit = HOURS
        step = 1
        dtg-attribute = dtg
        geom-attribute = geom
        z2-resolution = 2
        leaf-storage = true
      }
    }

Supported File Formats
----------------------

* **Apache Parquet** - Apache Parquet is the leading interoperable columnar format in the Hadoop ecosystem. It provides
efficient compression, storage, and query of structured data. Apache Parquet is currently the only format that can be
used for writing data into the FileSystem datastore.

* **Converter Storage** - The converter storage format is a synthetic format which allows you to overlay a GeoMesa converter
on top of a filesystem using a defined partition scheme. This allows you to utilize existing data storage layouts of
data stored in JSON, CSV, TSV, Avro, or other formats. Converters are pluggable allowing users to expose their own
custom storage formats if desired.

Creating a Writable FileSystemDatastore
---------------------------------------


Overlaying a Converter
----------------------




