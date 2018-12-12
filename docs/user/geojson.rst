GeoMesa GeoJSON
===============

GeoMesa provides built-in integration with GeoJSON. GeoMesa provides a GeoJSON API that allows for the
indexing and querying of GeoJSON data. The GeoJSON API does not use GeoTools - all data and operations
are pure JSON. The API also includes a REST endpoint for web integration.

In addition, when working with GeoTools, JSON can be stored as an attribute in a simple feature
and queried using CQL.

GeoMesa's JSON processing uses `JSONPath <http://goessner.net/articles/JsonPath/>`__ for selecting JSON elements.

GeoJSON API
-----------

The GeoJSON API provides a simplified interface for spatially indexing GeoJSON. GeoJSON is ingested
by indexing the embedded geometry. Additionally, a date field can be indexed by specifying a custom
field in the ``properties`` element, which allows arbitrary JSON extensions.

Data may be accessed programmatically or through a REST endpoint.

Adding and Updating Features
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Data is added to the index as GeoJSON strings. When creating the index, you may optionally specify an ID field
and a date field using JSONPath expressions. The ID field is required in order to update or modify features.
The date field will allow for optimized spatio-temporal queries.

Features can be added to the index by passing in GeoJSON objects of type ``Feature`` or ``FeatureCollection``.
Updating or deleting features requires the corresponding feature IDs.

Querying Features
^^^^^^^^^^^^^^^^^

Features can be queried using a MongoDB-like JSON syntax. Results will be the original GeoJSON, or may be
transformed into arbitrary JSON py passing in a projection.

Predicates
::::::::::

+--------------------+-----------------------------------------------------------------------------------+
| Predicate          | Syntax                                                                            |
+====================+===================================================================================+
| include            | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     {}                                                                            |
+--------------------+-----------------------------------------------------------------------------------+
| equals             | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     { "foo" : "bar" }                                                             |
+--------------------+-----------------------------------------------------------------------------------+
| greater-than       | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     { "foo" : { "$gt" : 10 } }                                                    |
|                    |     { "foo" : { "$gte" : 10 } }                                                   |
+--------------------+-----------------------------------------------------------------------------------+
| less-than          | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     { "foo" : { "$lt" : 10 } }                                                    |
|                    |     { "foo" : { "$lte" : 10 } }                                                   |
+--------------------+-----------------------------------------------------------------------------------+
| bounding box       | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     { "geometry" : { "$bbox" : [-180, -90, 180, 90] }}                            |
+--------------------+-----------------------------------------------------------------------------------+
| spatial intersects | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     {                                                                             |
|                    |       "geometry" : {                                                              |
|                    |         "$intersects" : {                                                         |
|                    |           "$geometry" : { "type" : "Point", "coordinates" : [30, 10] }            |
|                    |         }                                                                         |
|                    |       }                                                                           |
|                    |     }                                                                             |
+--------------------+-----------------------------------------------------------------------------------+
| spatial within     | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     {                                                                             |
|                    |       "geometry" : {                                                              |
|                    |         "$within" : {                                                             |
|                    |           "$geometry" : {                                                         |
|                    |             "type" : "Polygon",                                                   |
|                    |             "coordinates": [ [ [0,0], [3,6], [6,1], [0,0] ] ]                     |
|                    |           }                                                                       |
|                    |         }                                                                         |
|                    |       }                                                                           |
|                    |     }                                                                             |
+--------------------+-----------------------------------------------------------------------------------+
| spatial contains   | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     {                                                                             |
|                    |       "geometry" : {                                                              |
|                    |         "$contains" : {                                                           |
|                    |           "$geometry" : {  "type" : "Point", "coordinates" : [30, 10] }           |
|                    |         }                                                                         |
|                    |       }                                                                           |
|                    |     }                                                                             |
+--------------------+-----------------------------------------------------------------------------------+
| and                | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     { "foo" : "bar", "baz" : 10 }                                                 |
+--------------------+-----------------------------------------------------------------------------------+
| or                 | .. code-block:: json                                                              |
|                    |                                                                                   |
|                    |     { "$or" : [ { "foo" : "bar" }, { "baz" : 10 } ] }                             |
+--------------------+-----------------------------------------------------------------------------------+

Transformations
:::::::::::::::

The JSON being returned can be transformed by specifying element mappings. The transform is defined by a
map where the keys define the element in the returned JSON using dot notation, and the values specify the
JSONPath expression used to extract the value from the original GeoJSON.

For example, to return just the geometry you could use a mapping of ``"geom" -> "geometry"``:

.. code-block:: json

    {"geom":{"type":"Point","coordinates":[30,10]}}

Programmatic Access
^^^^^^^^^^^^^^^^^^^

The main interface for programmatic access is the ``GeoJsonIndex``. It can be instantiated by wrapping a
GeoMesa ``DataStore``. The module is available through maven:

.. code-block:: xml

        <dependency>
            <groupId>org.locationtech.geomesa</groupId>
            <artifactId>geomesa-geojson-api_2.11</artifactId>
            <version>1.3.0</version>
        </dependency>

Example code in scala:

.. code-block:: scala

    import org.locationtech.geomesa.geojson.{GeoJsonGtIndex, GeoJsonIndex}

    val ds = DataStoreFinder.getDataStore(...) // ensure this is a GeoMesa data store
    val index: GeoJsonIndex = new GeoJsonGtIndex(ds)
    index.createIndex("test", Some("$.properties.id"), points = true)

    val features =
        s"""{ "type": "FeatureCollection",
           |  "features": [
           |    {"type":"Feature","geometry":{"type":"Point","coordinates":[30,10]},"properties":{"id":"0","name":"n0"}},
           |    {"type":"Feature","geometry":{"type":"Point","coordinates":[31,10]},"properties":{"id":"1","name":"n1"}},
           |    {"type":"Feature","geometry":{"type":"Point","coordinates":[32,10]},"properties":{"id":"2","name":"n2"}}
           |  ]
           |}""".stripMargin

    index.add(name, features)

    // query by bounding box
    index.query(name, """{ "geometry" : { "$bbox" : [29, 9, 31.5, 11] }}""").toList
    // result:
    // {"type":"Feature","geometry":{"type":"Point","coordinates":[30,10]},"properties":{"id":"0","name":"n0"}}
    // {"type":"Feature","geometry":{"type":"Point","coordinates":[31,10]},"properties":{"id":"1","name":"n1"}}

    // query for all, transform JSON coming back
    index.query(name, "{}", Map("foo.bar" -> "geometry", "foo.baz" -> "properties.name")).toList
    // result:
    // {"foo":{"bar":{"type":"Point","coordinates":[30,10]},"baz":"n0"}}
    // {"foo":{"bar":{"type":"Point","coordinates":[31,10]},"baz":"n1"}}
    // {"foo":{"bar":{"type":"Point","coordinates":[32,10]},"baz":"n2"}}

REST Access
^^^^^^^^^^^

The ``GeoJsonIndex`` is also exposed through a REST endpoint. Currently, the REST endpoint does not support
updates or deletes of existing features, or transformation of resposes. Furthermore, it requires Accumulo as
the backing data store. It may be installed in GeoServer by extracting the install file into
``geoserver/WEB-INF/lib``:

.. code-block:: bash

    $ tar -xf geomesa-geojson/geomesa-geojson-gs-plugin/target/geomesa-geojson-gs-plugin_2.11-$VERSION-install.tar.gz -C <dest>

Note that this also requires the AccumuloDataStore to be installed. See :ref:`install_accumulo_geoserver`.

The REST endpoint will be available at ``<host>:<port>/geoserver/geomesa/geojson/``.

Methods
^^^^^^^

Get Registered DataStores
:::::::::::::::::::::::::

Returns a list of data stores available for querying.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/ds``                                                                              |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``GET``                                                                              |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | None                                                                                 |
+-----------------+--------------------------------------------------------------------------------------+
| **Data Params** | None                                                                                 |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 200                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:**                                                                         |
|                 |                                                                                      |
|                 | .. code-block:: json                                                                 |
|                 |                                                                                      |
|                 |     {                                                                                |
|                 |       "mycloud": {                                                                   |
|                 |         "accumulo.instance.id":"foo",                                                |
|                 |         "accumulo.zookeepers":"foo1,foo2,foo3",                                      |
|                 |         "accumulo.catalog":"foo.bar",                                                |
|                 |         "accumulo.user":"foo",                                                       |
|                 |         "accumulo.password":"***"                                                    |
|                 |       }                                                                              |
|                 |     }                                                                                |
|                 |                                                                                      |
|                 |                                                                                      |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | N/A                                                                                  |
| **Response**    |                                                                                      |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     curl 'localhost:8080/geoserver/geomesa/geojson/ds'                               |
+-----------------+--------------------------------------------------------------------------------------+
| **Notes**       | An entry will be returned for each registered data store                             |
+-----------------+--------------------------------------------------------------------------------------+

Register a DataStore
::::::::::::::::::::

Registers a data store to make it available for querying.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/ds/:alias``                                                                       |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``POST``                                                                             |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Alias used to reference the data store in future requests |
+-----------------+--------------------------------------------------------------------------------------+
| **Data Params** | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``accumulo.instance.id=[alphanumeric]``                                            |
|                 | * ``accumulo.zookeepers=[alphanumeric]``                                             |
|                 | * ``accumulo.user=[alphanumeric]``                                                   |
|                 | * ``accumulo.catalog=[alphanumeric]``                                                |
|                 |                                                                                      |
|                 | **Optional**                                                                         |
|                 |                                                                                      |
|                 | * ``geomesa.security.auths=[alphanumeric]``                                          |
|                 | * ``geomesa.security.visibilities=[alphanumeric]``                                   |
|                 | * ``geomesa.query.timeout=[alphanumeric]``                                           |
|                 | * ``geomesa.query.threads=[integer]``                                                |
|                 | * ``accumulo.query.record-threads=[integer]``                                        |
|                 | * ``accumulo.write.threads=[integer]``                                               |
|                 | * ``geomesa.query.loose-bounding-box=[Boolean]``                                     |
|                 | * ``geomesa.stats.generate=[Boolean]``                                               |
|                 | * ``geomesa.query.audit=[Boolean]``                                                  |
|                 | * ``geomesa.query.caching=[Boolean]``                                                |
|                 | * ``geomesa.security.force-empty-auths=[Boolean]``                                   |
|                 | * ``accumulo.mock=[Boolean]``                                                        |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 200                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if data store can not be created with the provided parameters        |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/ds/myds' \                           |
|                 |       -d accumulo.user=foo -d accumulo.password=foo -d accumulo.catalog=foo.bar \    |
|                 |       -d accumulo.zookeepers=foo1,foo2,foo3 -d accumulo.instance.id=foo              |
|                 |                                                                                      |
+-----------------+--------------------------------------------------------------------------------------+
| **Notes**       | Parameters correspond to the ``AccumuloDataStore`` connection parameters used        |
|                 | by DataStoreFinder                                                                   |
+-----------------+--------------------------------------------------------------------------------------+

Create GeoJSON Index
::::::::::::::::::::

Creates a new index under an existing data store.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/index/:alias/:index``                                                             |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``POST``                                                                             |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Reference to a previously registered data store           |
|                 | * ``index=[alphanumeric]`` Unique name of the GeoJSON index to create                |
+-----------------+--------------------------------------------------------------------------------------+
| **Data Params** | **Optional**                                                                         |
|                 |                                                                                      |
|                 | * ``points=[Boolean]`` Optimization hint if all geometries will be points            |
|                 | * ``date=[alphanumeric]`` JSONPath expression to a date field for temporal indexing  |
|                 | * ``id=[alphanumeric]`` JSONPath expression to an ID field to uniquely identify each |
|                 |   record                                                                             |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 201                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if a required parameter is not specified                             |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test' \                   |
|                 |       -d id=properties.id                                                            |
|                 |                                                                                      |
+-----------------+--------------------------------------------------------------------------------------+

Delete GeoJSON Index
::::::::::::::::::::

Deletes an existing index and all features it contains.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/index/:alias/:index``                                                             |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``DELETE``                                                                           |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Reference to a previously registered data store           |
|                 | * ``index=[alphanumeric]`` Unique name of the GeoJSON index to create                |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 204                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if a required parameter is not specified                             |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test' \                   |
|                 |       -X DELETE                                                                      |
|                 |                                                                                      |
+-----------------+--------------------------------------------------------------------------------------+

Add Features
::::::::::::

Add features to the index with GeoJSON.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/index/:alias/:index/features``                                                    |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``POST``                                                                             |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Reference to a previously registered data store           |
|                 | * ``index=[alphanumeric]`` Unique name of a GeoJSON index                            |
+-----------------+--------------------------------------------------------------------------------------+
| **Body**        | * ``[alphanumeric]`` GeoJSON ``Feature`` or ``FeatureCollection``                    |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 200                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** ``["1","2"]`` List of ids for the added features                        |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if a required parameter is not specified                             |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     echo '{"type":"Feature","geometry":{"type":"Point",' \                           |
|                 |       '"coordinates":[30,10]},"properties":{"id":"0","name":"n0"}}' \                |
|                 |       > feature.json                                                                 |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features' \          |
|                 |       -H 'Content-type: application/json' \                                          |
|                 |       -d @feature.json                                                               |
|                 |                                                                                      |
|                 |     echo '{"type":"FeatureCollection","features":[' \                                |
|                 |       '{"type":"Feature","geometry":{"type":"Point",' \                              |
|                 |       '"coordinates":[32,10]},"properties":{"id":"1","name":"n1"}},' \               |
|                 |       '{"type":"Feature","geometry":{"type":"Point",' \                              |
|                 |       '"coordinates":[34,10]},"properties":{"id":"2","name":"n2"}}]}' \              |
|                 |       > features.json                                                                |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features' \          |
|                 |       -H 'Content-type: application/json' \                                          |
|                 |       -d @features.json                                                              |
+-----------------+--------------------------------------------------------------------------------------+

Update Features
:::::::::::::::

Update existing features in the index. Feature IDs will be extracted from the GeoJSON submitted.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/index/:alias/:index/features``                                                    |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``PUT``                                                                              |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Reference to a previously registered data store           |
|                 | * ``index=[alphanumeric]`` Unique name of a GeoJSON index                            |
+-----------------+--------------------------------------------------------------------------------------+
| **Body**        | * ``[alphanumeric]`` GeoJSON ``Feature`` or ``FeatureCollection``                    |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 200                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if a required parameter is not specified                             |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
|                 |                                                                                      |
|                 | **Code:** 400 - if ID field was not specified when creating the index                |
|                 |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     echo '{"type":"Feature","geometry":{"type":"Point",' \                           |
|                 |       '"coordinates":[30,10]},"properties":{"id":"0","name":"n0-updated"}}' \        |
|                 |       > feature.json                                                                 |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features' \          |
|                 |       -H 'Content-type: application/json' \                                          |
|                 |       --upload-file feature.json                                                     |
|                 |                                                                                      |
|                 |     echo '{"type":"FeatureCollection","features":[' \                                |
|                 |       '{"type":"Feature","geometry":{"type":"Point",' \                              |
|                 |       '"coordinates":[32,10]},"properties":{"id":"1","name":"n1-updated"}},' \       |
|                 |       '{"type":"Feature","geometry":{"type":"Point",' \                              |
|                 |       '"coordinates":[34,10]},"properties":{"id":"2","name":"n2-updated"}}]}' \      |
|                 |       > features.json                                                                |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features' \          |
|                 |       -H 'Content-type: application/json' \                                          |
|                 |       --upload-file features.json                                                    |
+-----------------+--------------------------------------------------------------------------------------+

Update Features by ID
:::::::::::::::::::::

Update existing features in the index, explicitly specifying the feature IDs.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/index/:alias/:index/features/:ids``                                               |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``PUT``                                                                              |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Reference to a previously registered data store           |
|                 | * ``index=[alphanumeric]`` Unique name of a GeoJSON index                            |
|                 | * ``id=[alphanumeric]`` Feature IDs to update, comma-separated                       |
+-----------------+--------------------------------------------------------------------------------------+
| **Body**        | * ``[alphanumeric]`` GeoJSON ``Feature`` or ``FeatureCollection``                    |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 200                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if a required parameter is not specified                             |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     echo '{"type":"Feature","geometry":{"type":"Point",' \                           |
|                 |       '"coordinates":[30,10]},"properties":{"id":"0","name":"n0-updated"}}' \        |
|                 |       > feature.json                                                                 |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features/0' \        |
|                 |       -H 'Content-type: application/json' \                                          |
|                 |       --upload-file feature.json                                                     |
|                 |                                                                                      |
|                 |     echo '{"type":"FeatureCollection","features":[' \                                |
|                 |       '{"type":"Feature","geometry":{"type":"Point",' \                              |
|                 |       '"coordinates":[32,10]},"properties":{"id":"1","name":"n1-updated"}},' \       |
|                 |       '{"type":"Feature","geometry":{"type":"Point",' \                              |
|                 |       '"coordinates":[34,10]},"properties":{"id":"2","name":"n2-updated"}}]}' \      |
|                 |       > features.json                                                                |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features/1,2' \      |
|                 |       -H 'Content-type: application/json' \                                          |
|                 |       --upload-file features.json                                                    |
+-----------------+--------------------------------------------------------------------------------------+

Delete Features by ID
:::::::::::::::::::::

Delete existing features in the index by feature IDs.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/index/:alias/:index/features/:ids``                                               |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``DELETE``                                                                           |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Reference to a previously registered data store           |
|                 | * ``index=[alphanumeric]`` Unique name of a GeoJSON index                            |
|                 | * ``id=[alphanumeric]`` Feature IDs to delete, comma-separated                       |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 200                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if a required parameter is not specified                             |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features/1,2' \      |
|                 |       -X DELETE                                                                      |
+-----------------+--------------------------------------------------------------------------------------+

Query Features by ID
::::::::::::::::::::

Query features in the index by feature IDs.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/index/:alias/:index/features/:ids``                                               |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``GET``                                                                              |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Reference to a previously registered data store           |
|                 | * ``index=[alphanumeric]`` Unique name of a GeoJSON index                            |
|                 | * ``id=[alphanumeric]`` Feature IDs to query, comma-separated                        |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 200                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** GeoJSON feature collection                                              |
|                 |                                                                                      |
|                 | **Example:**                                                                         |
|                 |                                                                                      |
|                 | .. code-block:: json                                                                 |
|                 |                                                                                      |
|                 |     {                                                                                |
|                 |       "type":"FeatureCollection",                                                    |
|                 |       "features":[                                                                   |
|                 |         {                                                                            |
|                 |           "type":"Feature",                                                          |
|                 |           "geometry":{"type":"Point","coordinates":[32,10]},                         |
|                 |           "properties":{"id":"1","name":"n1"}                                        |
|                 |         }                                                                            |
|                 |       ]                                                                              |
|                 |     }                                                                                |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if a required parameter is not specified                             |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features/1,2'        |
+-----------------+--------------------------------------------------------------------------------------+

Query Features
::::::::::::::

Query features with a predicate.

+-----------------+--------------------------------------------------------------------------------------+
| **URL**         | ``/index/:alias/:index/features``                                                    |
+-----------------+--------------------------------------------------------------------------------------+
| **Method**      | ``GET``                                                                              |
+-----------------+--------------------------------------------------------------------------------------+
| **URL Params**  | **Required**                                                                         |
|                 |                                                                                      |
|                 | * ``alias=[alphanumeric]`` Reference to a previously registered data store           |
|                 | * ``index=[alphanumeric]`` Unique name of a GeoJSON index                            |
|                 |                                                                                      |
|                 | **Optional**                                                                         |
|                 |                                                                                      |
|                 | * ``q=[alphanumeric]`` JSON query predicate                                          |
+-----------------+--------------------------------------------------------------------------------------+
| **Success**     | **Code:** 200                                                                        |
| **Response**    |                                                                                      |
|                 | **Content:** GeoJSON feature collection                                              |
|                 |                                                                                      |
|                 | **Example:**                                                                         |
|                 |                                                                                      |
|                 | .. code-block:: json                                                                 |
|                 |                                                                                      |
|                 |     {                                                                                |
|                 |       "type":"FeatureCollection",                                                    |
|                 |       "features":[                                                                   |
|                 |         {                                                                            |
|                 |           "type":"Feature",                                                          |
|                 |           "geometry":{"type":"Point","coordinates":[32,10]},                         |
|                 |           "properties":{"id":"1","name":"n1"}                                        |
|                 |         }                                                                            |
|                 |       ]                                                                              |
|                 |     }                                                                                |
+-----------------+--------------------------------------------------------------------------------------+
| **Error**       | **Code:** 400 - if a required parameter is not specified                             |
| **Response**    |                                                                                      |
|                 | **Content:** empty                                                                   |
+-----------------+--------------------------------------------------------------------------------------+
| **Sample Call** | .. code-block:: bash                                                                 |
|                 |                                                                                      |
|                 |     # return all features in the index 'test'                                        |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features'            |
|                 |                                                                                      |
|                 |     # query by feature id                                                            |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features' \          |
|                 |       --get --data-urlencode 'q={"properties.id":"0"}'                               |
|                 |                                                                                      |
|                 |     # query by bounding box                                                          |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features' \          |
|                 |       --get --data-urlencode 'q={"geometry":{"$bbox":[33,9,35,11]}}'                 |
|                 |                                                                                      |
|                 |     # query by property                                                              |
|                 |     curl \                                                                           |
|                 |       'localhost:8080/geoserver/geomesa/geojson/index/myds/test/features' \          |
|                 |       --get --data-urlencode 'q={"properties.name":"n1"}'                            |
+-----------------+--------------------------------------------------------------------------------------+
| **Notes**       | See `Querying Features`_ for full query syntax                                       |
+-----------------+--------------------------------------------------------------------------------------+

.. _json_attributes:

JSON Attributes
---------------

In addition to the GeoJSON API, GeoMesa allows for JSON integration with GeoTools data stores. Simple
feature ``String``-type attributes can be marked as JSON and then queried using CQL. JSON attributes
must be specified when creating a simple feature type:

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    // append the json hint after the attribute type, separated by a colon
    String spec = "json:String:json=true,dtg:Date,*geom:Point:srid=4326"
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);
    dataStore.createSchema(sft);

JSON attributes are still strings, and are set as any other strings:

.. code-block:: java

    String json = "{ \"foo\" : \"bar\" }";
    SimpleFeature sf = ...
    sf.setAttribute("json", json);

JSON attributes can be queried using JSONPath expressions. The first part of the path refers to the simple
feature attribute name, and the rest of the path is applied to the JSON attribute. Note that in ECQL, path
expressions must be enclosed in double quotes.

.. code-block:: java

    Filter filter = ECQL.toFilter("\"$.json.foo\" = 'bar'")
    SimpleFeature sf = ...
    sf.setAttribute("json", "{ \"foo\" : \"bar\" }");
    filter.evaluate(sf); // returns true
    sf.getAttribute("\"$.json.foo\""); // returns "bar"
    sf.setAttribute("json", "{ \"foo\" : \"baz\" }");
    filter.evaluate(sf); // returns false
    sf.getAttribute("\"$.json.foo\""); // returns "baz"
    sf.getAttribute("\"$.json.bar\""); // returns null

.. _json_path_filter_function:

JSONPath CQL Filter Function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

JSON attributes can contain periods and spaces. In order to query these attributes through an ECQL filter
use the jsonPath CQL filter function. This passes the path to an internal interpreter function that understands
how to handle these attribute names.

.. code-block:: java

    Filter filter = ECQL.toFilter("jsonPath('$.json.foo') = 'bar'")
    SimpleFeature sf = ...
    sf.setAttribute("json", "{ \"foo\" : \"bar\" }");
    filter.evaluate(sf); // returns true

To handle periods and spaces in attribute names, enclose the attribute in the standard bracket notation. However,
since the path is being passed to the jsonPath function as a string literal parameter, the single quotes need to be
escaped with an additional single quote.

.. code-block:: java

    Filter filter = ECQL.toFilter("jsonPath('$.json.[''foo.bar'']') = 'bar'")
    SimpleFeature sf = ...
    sf.setAttribute("json", "{ \"foo.bar\" : \"bar\" }");
    filter.evaluate(sf); // returns true

Similarly for spaces:

.. code-block:: java

    Filter filter = ECQL.toFilter("jsonPath('$.json.[''foo bar'']') = 'bar'")
    SimpleFeature sf = ...
    sf.setAttribute("json", "{ \"foo bar\" : \"bar\" }");
    filter.evaluate(sf); // returns true