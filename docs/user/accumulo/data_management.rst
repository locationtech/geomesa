Data Management
===============

GeoMesa provides many ways to optimize your data storage. You can add additional indices to speed up
certain queries, disable indices to speed up ingestion, pre-split tables for optimal data
distribution and migrate data between tables or environments.

.. _accumulo_attribute_indices:

Accumulo Attribute Indices
--------------------------

See :ref:`attribute_indices` for an overview of attribute indices. The Accumulo data store extends the
normal attribute indices with an additional 'join' format that stores less data.

.. warning::

    Accumulo data stores use the reduced 'join' format by default. In order to use 'full' indices,
    the keyword ``full`` must be used in the ``SimpleFeatureType`` as described below.

Join Indices
^^^^^^^^^^^^

Join indices store a reduced subset of data in the index - just the feature ID, the default date
and the default geometry. To answer most queries, a join against the record index is required
to retrieve the full simple features - hence the name join index. Joining against the record
table is slow when returning many results, and should generally be avoided except for small queries.

GeoMesa will avoid joining against the record table if it is possible to answer
a query with only the data in the join index. In general, this means that the query is only
returning the properties for the default date, default geometry and the attribute being queried.
In addition, any CQL filters must only operate on those three attributes as well.

The keyword ``join`` may be used in place of ``true`` when specifying an attribute index in the
``SimpleFeatureType``. Note that join indices are the default in the Accumulo data store.

Full Indices
^^^^^^^^^^^^

Full indices store the full simple feature. This takes up the most space on disk, but allows for any query to
be answered without joining against the record table. This is the only option for non-Accumulo data stores.
To use a full index, the keyword ``full`` must be used in place of ``true`` when specifying an attribute
index in the ``SimpleFeatureType``.  Note that join indices are the default in the Accumulo data store.

.. _logical_timestamps:

Accumulo Logical Timestamps
---------------------------

By default, GeoMesa index tables are created using Accumulo's logical time. This ensures that updates to a given
simple feature will be ordered correctly, however it obscures the actual insert time for the underlying data
row. For advanced use cases, standard system time can be used instead of logical time. To disble logical
time, add the following user data hint to the simple feature type before calling ``createSchema``:

.. code-block:: java

    // append the hints to the end of the string, separated by a semi-colon
    String spec = "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.logical.time='false'";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

.. warning::

    If table sharing is enabled, then the index tables may have already been created with logical
    time enabled. If this is the case, then disabling it will have no effect. To ensure the
    tables are created correctly, you may wish to disable table sharing by adding the user data string
    ``geomesa.table.sharing='false'``

Splitting the Record Index
--------------------------

By default, GeoMesa assumes that feature IDs are UUIDs, and have an even distribution. If your
feature IDs do not follow this pattern, you may define a custom table splitter for the record index.
This will ensure that your features are spread across several different tablet servers, speeding
up ingestion and queries.

GeoMesa supplies three different table splitter options:

- ``org.locationtech.geomesa.index.conf.HexSplitter`` (used by default)

  Assumes an even distribution of IDs starting with 0-9, a-f, A-F

- ``org.locationtech.geomesa.index.conf.AlphaNumericSplitter``

  Assumes an even distribution of IDs starting with 0-9, a-z, A-Z

- ``org.locationtech.geomesa.index.conf.DigitSplitter``

  Assumes an even distribution of IDs starting with numeric values, which are specified as options

Custom splitters may also be used - any class that extends ``org.locationtech.geomesa.index.conf.TableSplitter``.

Specifying a Table Splitter
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Table splitter may be specified by setting a hint when creating a simple feature type,
similar to enabling indices (above).

Setting the hint can be done in three ways. If you are using a string to indicate your simple feature type
(e.g. through the command line tools, or when using ``SimpleFeatureTypes.createType``), you can append
the hint to the end of the string, like so:

.. code-block:: java

    // append the hints to the end of the string, separated by a semi-colon
    String spec = "name:String,dtg:Date,*geom:Point:srid=4326;" +
        "table.splitter.class=org.locationtech.geomesa.index.conf.AlphaNumericSplitter";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``,
you may set the hint directly in the feature type:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getUserData().put("table.splitter.class",
        "org.locationtech.geomesa.index.conf.DigitSplitter");
    sft.getUserData().put("table.splitter.options", "fmt:%02d,min:0,max:99");

If you are using TypeSafe configuration files to define your simple feature type, you may include
a 'user-data' key:

.. code-block:: javascript

    geomesa {
      sfts {
        "mySft" = {
          attributes = [
            { name = name, type = String             }
            { name = dtg,  type = Date               }
            { name = geom, type = Point, srid = 4326 }
          ]
          user-data = {
            table.splitter.class = "org.locationtech.geomesa.index.conf.DigitSplitter"
            table.splitter.options = "fmt:%01d,min:0,max:9"
          }
        }
      }
    }


Moving and Migrating Data
-------------------------

If you want an offline copy of your data, or you want to move data between networks, you can
export compressed Avro files containing your simple features. To do this using the command line
tools, use the export command with the ``format`` and ``gzip`` options:

.. code-block:: bash

    $ geomesa-accumulo export -c myTable -f mySft --format avro --gzip 6 -o myFeatures.avro

To re-import the data into another environment, you may use the ingest command. Because the Avro file
is self-describing, you do not need to specify any converter config or simple feature type definition:

.. code-block:: bash

    $ geomesa-accumulo ingest -c myTable -f mySft myFeatures.avro

If your data is too large for a single file, you may run multiple exports and use CQL
filters to separate your data.

If you prefer to not use Avro files, you may do the same process with delimited text files:

.. code-block:: bash

    $ geomesa-accumulo export -c myTable -f mySft --format tsv --gzip 6 -o myFeatures.tsv.gz
    $ geomesa-accumulo ingest -c myTable -f mySft myFeatures.tsv.gz

.. _index_upgrades:

Upgrading Existing Indices
--------------------------

GeoMesa often makes updates to indexing formats to improve query and write performance. However,
the index format for a given schema is fixed when it is first created. Updating GeoMesa versions
will provide bug fixes and new features, but will not update existing data to new index formats.

The following tables show the different indices available for different versions of GeoMesa. If not
known, the schema version for a feature type can be checked by examining the user data value
``geomesa.version``, or by scanning the Accumulo catalog table for ``version``. For GeoMesa schemas
created with 1.2.7 or later, the version of each index is tracked separately and the overall schema
version is no longer maintained.

Schema version 4 - 1.0.0-rc.7
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------+---------+----------------------------------------------------------------------------+
| Index     | Version | Notes                                                                      |
+===========+=========+============================================================================+
| GeoHash   | 1       | Used for most queries                                                      |
+-----------+---------+----------------------------------------------------------------------------+
| Record    | 1       | Used for queries on feature ID                                             |
+-----------+---------+----------------------------------------------------------------------------+
| Attribute | 1       | Used for attribute queries (when configured)                               |
+-----------+---------+----------------------------------------------------------------------------+

Schema version 5 - 1.1.0-rc.1 through 1.1.0-rc.2
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------+---------+----------------------------------------------------------------------------+
| Index     | Version | Notes                                                                      |
+===========+=========+============================================================================+
| Z3        | 1       | Replaces GeoHash index for spatio-temporal queries on point geometries     |
+-----------+---------+----------------------------------------------------------------------------+
| GeoHash   | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| Record    | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| Attribute | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+

Schema version 6 - 1.1.0-rc.3 through 1.2.0
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------+---------+----------------------------------------------------------------------------+
| Index     | Version | Notes                                                                      |
+===========+=========+============================================================================+
| Z3        | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| GeoHash   | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| Record    | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| Attribute | 2       | Added a composite date index and improved row-key collisions               |
+-----------+---------+----------------------------------------------------------------------------+


Schema version 7 - 1.2.1
^^^^^^^^^^^^^^^^^^^^^^^^

+-----------+---------+----------------------------------------------------------------------------+
| Index     | Version | Notes                                                                      |
+===========+=========+============================================================================+
| Z3        | 2       | Added support for non-point geometries and sharding for improved ingestion |
+-----------+---------+----------------------------------------------------------------------------+
| GeoHash   | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| Record    | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| Attribute | 2       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+


Schema version 8 - 1.2.2 through 1.2.4
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------+---------+----------------------------------------------------------------------------+
| Index     | Version | Notes                                                                      |
+===========+=========+============================================================================+
| Z3        | 2       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| Z2        | 1       | Spatial only index to replace GeoHash index                                |
+-----------+---------+----------------------------------------------------------------------------+
| Record    | 1       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+
| Attribute | 2       |                                                                            |
+-----------+---------+----------------------------------------------------------------------------+

Schema version 10 - 1.2.5+
^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------+---------+----------------------------------------------------------------------------+
| Index     | Version | Notes                                                                      |
+===========+=========+============================================================================+
| Z3        | 3       | Support for attribute-level visibilities and improved feature ID encoding  |
+-----------+---------+----------------------------------------------------------------------------+
| Z2        | 2       | Support for attribute-level visibilities and improved feature ID encoding  |
+-----------+---------+----------------------------------------------------------------------------+
| XZ3       | 1       | Spatio-temporal index with improved support for non-point geometries       |
+-----------+---------+----------------------------------------------------------------------------+
| XZ2       | 1       | Spatial index with improved support for non-point geometries               |
+-----------+---------+----------------------------------------------------------------------------+
| Record    | 2       | Support for attribute-level visibilities and improved feature ID encoding  |
+-----------+---------+----------------------------------------------------------------------------+
| Attribute | 3       | Support for attribute-level visibilities and improved feature ID encoding  |
+-----------+---------+----------------------------------------------------------------------------+

Using the GeoMesa command line tools, you can add or update an index to a newer version using ``add-index``.
For example, you could add the XZ3 index to replace the Z3 index for a feature type with non-point geometries.
The command will populate the new index using a distributed job. For large data sets, you can choose to
only populate features matching a CQL filter (e.g. the last month), or choose to not populate any
data. The update is seamless, and clients can continue to query and ingest while it runs.

See :ref:`add_index_command` for more details on the command line tools.
