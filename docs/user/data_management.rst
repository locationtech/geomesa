Data Management
===============

GeoMesa provides many ways to optimize your data storage. You can add additional indices to speed up
certain queries, disable indices to speed up ingestion, pre-split tables for optimal data
distribution and migrate data between tables or environments.

Index Structure
---------------

By default, GeoMesa creates three indices:

- Z2 - the Z2 index uses a z-order curve to index latitude and longitude. This is the primary index used
  to answer queries with a spatial component but no temporal component.
- Z3 - the Z3 index uses a z-order curve to index latitude, longitude and time. This is the primary index
  used to answer queries with both a spatial and temporal component.
- Record - the record index stores features by feature ID. It is used for any query by ID. Additionally,
  certain attribute queries may end up retrieving data from the record index. This is explained below.

If specified, GeoMesa will also create the following indices:

- Attribute - the attribute index uses attribute values as the primary index key. This allows for
  fast retrieval of queries without a spatio-temporal component. Attribute indices can be created
  in a 'reduced' format, that takes up less disk space at the cost of performance for certain queries,
  or a 'full' format that takes up more space but can answer certain queries faster.

Attribute Indices
-----------------

Some queries are slow to answer using the default indices. For example, with twitter data you
might want to return all tweets for a given user. To speed up this type of query, any
attribute in your simple feature type may be indexed individually.

Attribute indices may be one of two types: join or full.


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


Full Indices
^^^^^^^^^^^^

Full indices store the full simple feature. This takes up the most space, but allows for any
query to be answered without joining against the record table.


Cardinality Hints
^^^^^^^^^^^^^^^^^

GeoMesa has a query planner that tries to find the best strategy for answering a given query. In
general, this means using the index that will filter the result set the most, before considering
the entire query filter on the reduced data set. For simple queries, there is often only one
suitable index. However, for mixed queries, there can be multiple options.

For example, given the query ``bbox(geom, -120, -60, 120, 60) AND IN('id-01')``, we could try to
execute against the geohash index using the bounding box, or we could try to execute against the
record index using the feature ID. In this case, we know that the ID filter will match at most one
record, while the bbox filter could match many records, so we will choose the record index.

In order to force GeoMesa to always use the attribute index when available, you may specify
an attribute as having a high cardinality - i.e. having many distinct values. This implies
that a query against that attribute will return relatively few records. If a query contains
a filter against a high-cardinality attribute, the attribute index will always be used first.

Note that technically you may also specify attributes as low-cardinality - but in that case
it is better to just not index the attribute at all.


Adding Attribute Indices
^^^^^^^^^^^^^^^^^^^^^^^^

To index an attribute, add an ``index`` hint to the attribute descriptor with a value of ``join`` or
``full``. The string ``true`` is also allowed for legacy reasons, and is equivalent to join. To set
the cardinality of an attribute, use the hint ``cardinality`` with a value of ``high`` or ``low``.

Setting the hint can be done in multiple ways. If you are using a string to indicate your simple feature type
(e.g. through the command line tools, or when using ``SimpleFeatureTypes.createType``), you can append
the hint to the attribute to be indexed, like so:

.. code-block:: java

    // append the hint after the attribute type, separated by a colon
    String spec = "name:String:index=full:cardinality=high,age:Int:index=join," +
        "dtg:Date,*geom:Point:srid=4326"
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``,
you may set the hint directly in the feature type:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getDescriptor("name").getUserData().put("index", "join");
    sft.getDescriptor("name").getUserData().put("cardinality", "high");

If you are using TypeSafe configuration files to define your simple feature type, you may include the hint in
the attribute field:

.. code-block:: json

    geomesa {
      sfts {
        "mySft" = {
          attributes = [
            { name = name, type = String, index = full, cardinality = high }
            { name = age,  type = Int,    index = join                     }
            { name = dtg,  type = Date                                     }
            { name = geom, type = Point,  srid = 4326                      }
          ]
        }
      }
    }

If you are using the GeoMesa ``SftBuilder``, you may call the overloaded attribute methods:

.. code-block:: scala

    // scala example
    import org.locationtech.geomesa.utils.geotools.SftBuilder.SftBuilder
    import org.locationtech.geomesa.utils.stats.Cardinality

    val sft = new SftBuilder()
        .stringType("name", Opts(index = true, cardinality = Cardinality.HIGH))
        .intType("age", Opts(index = true))
        .date("dtg")
        .geometry("geom", default = true)
        .build("mySft")


Customizing Index Creation
--------------------------

To speed up ingestion, or because you are only using certain query patterns, you may disable some indices.
The indices are created when calling ``createSchema``. If nothing is specified, the Z2, Z3 and record
indices will all be created, as well as any attribute indices you have defined.

.. warning::

    Certain queries may be much slower if you disable any indices.

.. warning::

    It is not currently possible to add core indices after schema creation. However, attribute
    indices may be added at any time through map/reduce jobs - see :ref:`attribute_indexing_job`.

To enable only certain indices, you may set a hint in your simple feature type. The hint key is
``geomesa.indexes.enabled``, and it should contain a comma-delimited list containing a subset of:

- ``z2`` - corresponds to the Z2 index
- ``z3`` - corresponds to the Z3 index
- ``records`` - corresponds to the record index
- ``attr_idx`` - corresponds to the attribute index


Setting the hint can be done in multiple ways. If you are using a string to indicate your simple feature type
(e.g. through the command line tools, or when using ``SimpleFeatureTypes.createType``), you can append
the hint to the end of the string, like so:

.. code-block:: java

    // append the hints to the end of the string, separated by a semi-colon
    String spec = "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.indexes.enabled='records,z3'";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``,
you may set the hint directly in the feature type:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getUserData().put("geomesa.indexes.enabled", "records,z3");

If you are using TypeSafe configuration files to define your simple feature type, you may include
a 'user-data' key:

.. code-block:: json

    geomesa {
      sfts {
        "mySft" = {
          attributes = [
            { name = name, type = String             }
            { name = dtg,  type = Date               }
            { name = geom, type = Point, srid = 4326 }
          ]
          user-data = {
            geomesa.indexes.enabled = "records,z3,attr_idx"
          }
        }
      }
    }

If you are using the GeoMesa ``SftBuilder``, you may call the ``withIndexes`` methods:

.. code-block:: scala

    // scala example
    import org.locationtech.geomesa.utils.geotools.SftBuilder.SftBuilder

    val sft = new SftBuilder()
        .stringType("name")
        .date("dtg")
        .geometry("geom", default = true)
        .withIndexes(List("records", "z3", "attr_idx"))
        .build("mySft")


Splitting the Record Index
--------------------------

By default, GeoMesa assumes that feature IDs are UUIDs, and have an even distribution. If your
feature IDs do not follow this pattern, you may define a custom table splitter for the record index.
This will ensure that your features are spread across several different tablet servers, speeding
up ingestion and queries.

GeoMesa supplies three different table splitter options:

- ``org.locationtech.geomesa.accumulo.data.HexSplitter`` (used by default)

  Assumes an even distribution of IDs starting with 0-9, a-f, A-F

- ``org.locationtech.geomesa.accumulo.data.AlphaNumericSplitter``

  Assumes an even distribution of IDs starting with 0-9, a-z, A-Z

- ``org.locationtech.geomesa.accumulo.data.DigitSplitter``

  Assumes an even distribution of IDs starting with numeric values, which are specified as options

Custom splitters may also be used - any class that extends ``org.locationtech.geomesa.accumulo.data.TableSplitter``.

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
        "table.splitter.class=org.locationtech.geomesa.accumulo.data.AlphaNumericSplitter";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

If you have an existing simple feature type, or you are not using ``SimpleFeatureTypes.createType``,
you may set the hint directly in the feature type:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getUserData().put("table.splitter.class",
        "org.locationtech.geomesa.accumulo.data.DigitSplitter");
    sft.getUserData().put("table.splitter.options", "fmt:%02d,min:0,max:99");

If you are using TypeSafe configuration files to define your simple feature type, you may include
a 'user-data' key:

.. code-block:: json

    geomesa {
      sfts {
        "mySft" = {
          attributes = [
            { name = name, type = String             }
            { name = dtg,  type = Date               }
            { name = geom, type = Point, srid = 4326 }
          ]
          user-data = {
            table.splitter.class = "org.locationtech.geomesa.accumulo.data.DigitSplitter"
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

    $ geomesa export -c myTable -f mySft --format avro --gzip 6 -o myFeatures.avro

To re-import the data into another environment, you may use the import command. Because the Avro file
is self-describing, you do not need to specify any converter config or simple feature type definition:

.. code-block:: bash

    $ geomesa import -c myTable -f mySft myFeatures.avro

If your data is too large for a single file, you may run multiple exports and use CQL
filters to separate your data.

If you prefer to not use Avro files, you may do the same process with delimited text files:

.. code-block:: bash

    $ geomesa export -c myTable -f mySft --format tsv --gzip 6 -o myFeatures.tsv.gz
    $ geomesa import -c myTable -f mySft myFeatures.tsv.gz
