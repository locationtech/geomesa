.. _fsds_partition_schemes:

Partition Schemes
=================

Partition Schemes can be defined using a JSON/typesafe configuration or by a common name. By default these schemes all
utilize "leaf storage" with a one up sequence number. This allows for appending data when it arrives out of order.

Common Date schemes are:

* minute
* hourly
* daily
* monthly
* julian-minute
* julian-hourly
* julian-daily

Common geometry schemes are:

* z2-2bit
* z2-4bit

Common combined schemes are created by using a comma. For example:

* hourly,z2-2bit
* julian-daily,z2-2bit

Leaf vs Bucket Storage
----------------------

As previously mentioned, the default storage mode is leaf storage. Leaf storage means that the last component of the
partition scheme is the prefix of the parquet file name. A sequence number is then appended to the filename before the
file extension. For example, a partition scheme of ``yyyy/MM/dd`` would produce the storage path
``2016/01/01_0000.parquet`` for the first file created for the partition ``2016/01/01``. The next file (created by a
new ingest process) for the same partition would result in a file named ``2016/01/01_0001.parquet`` and then
``2016/01/01_0002.parquet`` and so on.

By contrast, bucket storage uses a directory for the final component of the scheme. For our partition example above
the storage paths would be ``2016/01/01/0001.parquet``, ``2016/01/01/0002.parquet``, and ``2016/01/01/0003.parquet``.

Leaf storage results in less directory overhead for filesystems such as S3.

JSON Configuration
------------------

You can manually configure the datetime and z2 geometry schemes using typesafe and json configuration.

Common Options
``````````````

* ``scheme`` - The names of the partition schemes separated by commas
* ``leaf-storage`` - Defines if the partition scheme are files or directories

  * ``true`` - Partitions map to file names and will be overwritten with subsequent ingest clients
  * ``false`` - Partitions map to directories allowing multiple files to be ingested per partition


DateTime Configuration
``````````````````````

The datetime scheme is indicated by the key ``name = "datetime"`` and uses the following options:

* ``datetime-format`` - A Java DateTime format string separated by forward slashes which will be used to build a
  directory structure. Examples are:

  * ``yyyy/DDD/HH`` - year, Julian Date, hour layout
  * ``yyyy/MM/dd`` - year, month, day layout
  * ``yyyy/MM/dd/HH`` - year, month, day, hour layout

* ``step-unit`` - A ``java.time.temporal.ChronoUnit`` defining how to increment the leaf of the partition scheme
* ``step`` - The step amount to increment the leaf of the partition scheme
* ``dtg-attribute`` - The datetime attribute from the SimpleFeature to use for partitioning data

.. code-block:: json

    {
      "scheme" : "datetime,z2",
      "options" : {
        "datetime-format" : "yyyy/MM/dd/HH",
        "step-unit" : "HOURS",
        "step" : "1",
        "dtg-attribute" : "dtg",
        "leaf-storage" : true
      }
    }

Z2 Configuration
````````````````

The z2 scheme is indicate by the key ``name = "z2"`` and uses the following options:


* ``geom-attribute`` - The geometry attribute from the SimpleFeature to use for partitioning data
* ``z2-resolution`` - The number of bits to use for z indexing.


.. code-block:: json

    {
      "scheme" : "z2",
      "options" : {
        "geom-attribute" : "geom",
        "z2-resolution" : 2,
        "leaf-storage" : false
      }
    }

Combined DateTime and Z2 Configuration
``````````````````````````````````````

The datetime-z2 scheme is indicated by the key ``name = "datetime,z2"`` and uses the combined options
from the datetime and z2 schemes.

.. code-block:: json

    {
      "scheme" : "datetime,z2",
      "options" : {
        "datetime-format" : "yyyy/MM/dd/HH",
        "step-unit" : "HOURS",
        "step" : 1,
        "dtg-attribute" : "dtg",
        "geom-attribute" : "geom",
        "z2-resolution" : 2,
        "leaf-storage" : true
      }
    }
