Data Management
===============

GeoMesa provides many ways to optimize your data storage. You can add additional indices to speed up
certain queries, disable indices to speed up ingestion, pre-split tables for optimal data
distribution and migrate data between tables or environments.

.. note::

    Currently this chapter applies only to the Accumulo Data Store.

.. _index_structure:

Index Structure
---------------

By default, GeoMesa creates a number of indices:

- **Z2** - the Z2 index uses a two-dimensional Z-order curve to index latitude and longitude
  for point data. This index will be created if the feature type has the geometry type
  ``Point``. This is used to efficiently answer queries of
  features with point geometry with a spatial component but no temporal component.
- **Z3** - the Z3 index uses a three-dimensional Z-order curve to index latitude, longitude,
  and time for point data. This index will be created if the feature type has the geometry
  type ``Point`` and has a time attribute. This is used to efficiently answer queries of
  features with point geometry with both spatial and temporal components.
- **XZ2** - the XZ2 index uses a two-dimensional implementation of XZ-ordering [#ref1]_ to index
  latitude and longitude for non-point data. XZ-ordering is an extension of Z-ordering
  designed for spatially extended objects (i.e. non-point geometries such as line strings or
  polygons). This index will be created if the feature type has a non-\ ``Point`` geometry. This
  is used to efficiently answer queries of features with non-point geometry with a spatial
  component but no temporal component.
- **XZ3** - the XZ3 index uses a three-dimensional implementation of XZ-ordering [#ref1]_ to index
  latitude, longitude, and time for non-point data. This index will be created if the feature
  type has a non-\ ``Point`` geometry and has a time attribute. This is used to efficiently
  answer queries of features with non-point geometry with both spatial and temporal components.
- **Record** - the record index stores features by feature ID. It is used for any query by ID. Additionally,
  certain attribute queries may end up retrieving data from the record index. This is explained below.

If specified, GeoMesa will also create the following indices:

- **Attribute** - the attribute index uses attribute values as the primary index key. This allows for
  fast retrieval of queries without a spatio-temporal component. Attribute indices can be created
  in a 'reduced' format, that takes up less disk space at the cost of performance for certain queries,
  or a 'full' format that takes up more space but can answer certain queries faster.

.. _attribute_indices:

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

.. code-block:: javascript

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

.. _customizing_z_index:

Customizing the Z-Index
-----------------------

GeoMesa uses a z-curve index for time-based queries. By default, time is split into week-long chunks and indexed
per-week. If your queries are typically much larger or smaller than one week, you may wish to partition at a
different interval. GeoMesa provides four intervals - ``day``, ``week``, ``month`` or ``year``. As the interval
gets larger, fewer partitions must be examined for a query, but the precision of each interval will go down.

If you typically query months of data at a time, then indexing per-month may provide better performance.
Alternatively, if you typically query minutes of data at a time, indexing per-day may be faster. The default
per-week partitioning tends to provides a good balance for most scenarios. Note that the optimal partitioning
depends on query patterns, not the distribution of data.

The time partitioning is set when calling ``createSchema``. It may be specified through the simple feature type
user data using the hint ``geomesa.z3.interval``:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getUserData().put("geomesa.z3.interval", "month");

See below for alternate ways to set the user data.

.. _customizing_xz_index:

Customizing the XZ-Index
------------------------

GeoMesa uses an extended z-curve index for storing geometries with extents. The index can be customized
by specifying the resolution level used to store geometries. By default, the resolution level is 12. If
you have very large geometries, you may want to lower this value. Conversely, if you have very small
geometries, you may want to raise it.

The resolution level for an index is set when calling ``createSchema``. It may be specified through
the simple feature type user data using the hint ``geomesa.xz.precision``:

.. code-block:: java

    // set the hint directly
    SimpleFeatureType sft = ...
    sft.getUserData().put("geomesa.xz.precision", 12);

See below for alternate ways to set the user data.

For more information on resolution level (g), see
"XZ-Ordering: A Space-Filling Curve for Objects with Spatial Extension" by Böhm, Klump and Kriegel.

.. _customizing_index_creation:

Customizing Index Creation
--------------------------

To speed up ingestion, or because you are only using certain query patterns, you may disable some indices.
The indices are created when calling ``createSchema``. If nothing is specified, the Z2/Z3 (or XZ2/XZ3
depending on geometry type) indices and record indices will all be created, as well as any attribute
indices you have defined.

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

If the default geometry type is ``Geometry`` (i.e. supporting both point and non-point
features), you must explicitly enable "mixed" indexing mode with ``geomesa.mixed.geometries``:

.. code-block:: java

    // append the hints to the end of the string, separated by a semi-colon
    String spec = "name:String,dtg:Date,*geom:Geometry:srid=4326;geomesa.mixed.geometries='true'";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

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

.. _accumulo_visibilities:

Accumulo Visibilities
---------------------

GeoMesa support Accumulo visibilities for securing data. Visibilities can be set at data store level,
feature level or individual attribute level.

See :ref:`accumulo_authorizations` for details on querying data with visibilities.

Data Store Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When creating your data store, a default visibility can be configured for all features:

.. code-block:: java

    Map<String, String> parameters = ...
    parameters.put("visibilities", "admin&user");
    DataStore ds = DataStoreFinder.getDataStore(parameters);

If present, visibilities set at the feature or attribute level will take priority over the data store configuration.


Feature Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^

Visibilities can be set on individual features using the simple feature user data:

.. code-block:: java

    import org.locationtech.geomesa.security.SecurityUtils;

    SecurityUtils.setFeatureVisibility(feature, "admin&user")

or

.. code-block:: java

    feature.getUserData().put("geomesa.feature.visibility", "admin&user");


Attribute-Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For more advanced use cases, visibilities can be set at the attribute level.
Attribute-level visibilities must be enabled when creating your simple feature type by setting
the appropriate user data value:

.. code-block:: java

    sft.getUserData().put("geomesa.visibility.level", "attribute");
    dataStore.createSchema(sft);

When writing each feature, the per-attribute visibilities must be set in a comma-delimited string in the user data.
Each attribute must have a corresponding value in the delimited string, otherwise an error will be thrown.

For example, if your feature type has four attributes:

.. code-block:: java

    import org.locationtech.geomesa.security.SecurityUtils;

    SecurityUtils.setFeatureVisibility(feature, "admin,user,admin,user")

or

.. code-block:: java

    feature.getUserData().put("geomesa.feature.visibility", "admin,user,admin,user");

.. _accumulo_authorizations:

Accumulo Authorizations
-----------------------

When performing a query, GeoMesa delegates the retrieval of authorizations to ``service providers`` that
implement the following interface:

.. code-block:: java

    package org.locationtech.geomesa.security;

    public interface AuthorizationsProvider {

        public static final String AUTH_PROVIDER_SYS_PROPERTY = "geomesa.auth.provider.impl";

        /**
         * Gets the authorizations for the current context. This may change over time
         * (e.g. in a multi-user environment), so the result should not be cached.
         *
         * @return
         */
        public Authorizations getAuthorizations();

        /**
         * Configures this instance with parameters passed into the DataStoreFinder
         *
         * @param params
         */
        public void configure(Map<String, Serializable> params);
    }

When a GeoMesa data store is instantiated, it will scan for available service providers.
Third-party implementations can be enabled by placing them on the classpath and including
a special service descriptor file. See the
`Oracle Javadoc <http://docs.oracle.com/javase/7/docs/api/javax/imageio/spi/ServiceRegistry.html>`__
for details on implementing a service provider.

The GeoMesa data store will call ``configure()`` on the ``AuthorizationsProvider``
implementation, passing in the parameter map from the call to ``DataStoreFinder.getDataStore(Map params)``.
This allows the ``AuthorizationsProvider`` to configure itself based on the environment.

To ensure that the correct ``AuthorizationsProvider`` is used, GeoMesa will throw an exception if multiple
third-party service providers are found on the classpath. In this scenario, the particular service
provider class to use can be specified by the following system property:

.. code-block:: java

    // equivalent to "geomesa.auth.provider.impl"
    org.locationtech.geomesa.security.AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY

For simple scenarios, the set of authorizations to apply to all queries can be specified when creating
the GeoMesa data store by using the ``auths`` configuration parameter. This will use a
default ``AuthorizationsProvider`` implementation provided by GeoMesa.

.. code-block:: java

    // create a map containing initialization data for the GeoMesa data store
    Map<String, String> configuration = ...
    configuration.put("auths", "user,admin");
    DataStore dataStore = DataStoreFinder.getDataStore(configuration);

If there are no ``AuthorizationsProvider`` implementations found on the classpath, and the ``auths`` parameter is
not set, GeoMesa will default to using the authorizations associated with the underlying Accumulo
connection (i.e. the ``user`` configuration value).

.. warning::

    This is not a recommended approach for a production system.

In addition, please note that the authorizations used in any scenario cannot exceed
the authorizations of the underlying Accumulo connection.

For examples on implementing an ``AuthorizationsProvider`` see the :ref:`accumulo_tutorials_security` tutorials.

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

.. rubric:: Footnotes

.. [#ref1] Böhm, Klump, and Kriegel. "XZ-ordering: a space-filling curve for objects with spatial extension." 6th. Int. Symposium on Large Spatial Databases (SSD), 1999, Hong Kong, China. (http://www.dbs.ifi.lmu.de/Publikationen/Boehm/Ordering_99.pdf)
