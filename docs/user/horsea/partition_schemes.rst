Partition Schemes
=================

Partition Schemes can be defined using a JSON/typesafe configuration or by a common name. By default these schemes all
utilize "bucket storage" meaning that multiple files for each partition (e.g. hour of the day) can be created and stored
in the filesystem. This allows for appending data when it arrives out of order.

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
      "scheme" : "datetime,z2"
      "options" : {
        "datetime-format" : "yyyy/dd/HH",
        "step-unit" : "HOURS",
        "step" : "1",
        "dtg-attribute" = "dtg",
        "leaf-storage" = true,
      }
    }

Z2 Configuration
````````````````

The z2 scheme is indicate by the key ``name = "z2"`` and uses the following options:


* ``geom-attribute`` - The geometry attribute from the SimpleFeature to use for partitioning data
* ``z2-resolution`` - The number of bits to use for z indexing.


.. code-block:: json

    {
      "scheme" : "z2"
      "options" : {
        "geom-attribute" = "geom",
        "z2-resolution" = 2,
        "leaf-storage" = false,
      }
    }

Combined DateTime and Z2 Configuration
``````````````````````````````````````

The datetime-z2 scheme is indicated by the key ``name = "datetime,z2"`` and uses the combined options
from the datetime and z2 schemes.

.. code-block:: json

    {
      "scheme" : "datetime,z2"
      "options" : {
        "datetime-format" : "yyyy/dd/HH",
        "step-unit" : "HOURS",
        "step" : "1",
        "dtg-attribute" = "dtg",
        "geom-attribute" = "geom",
        "z2-resolution" = 2,
        "leaf-storage" = true,
      }
    }