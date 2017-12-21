.. _json_converter:

Parsing JSON
------------

The JSON converter defines the path to a list of features as well as
json-types of each field:

::

    {
      type         = "json"
      id-field     = "$id"
      feature-path = "$.Features[*]"
      fields = [
        { name = "id",     json-type = "integer",  path = "$.id",               transform = "toString($0)" }
        { name = "number", json-type = "integer",  path = "$.number",                                      }
        { name = "color",  json-type = "string",   path = "$.color",            transform = "trim($0)"     }
        { name = "weight", json-type = "double",   path = "$.physical.weight",                             }
        { name = "geom",   json-type = "geometry", path = "$.geometry",                                    }
      ]
    }

JSON Geometries
~~~~~~~~~~~~~~~

Geometry objects can be represented as either WKT or GeoJSON and parsed
with the same config:

Config:

::

     { name = "geom", json-type = "geometry", path = "$.geometry", transform = "point($0)" }

Data:

::

    {
       DataSource: { name: "myjson" },
       Features: [
         {
           id: 1,
           number: 123,
           color: "red",
           geometry: { "type": "Point", "coordinates": [55, 56] }
         },
         {
           id: 2,
           number: 456,
           color: "blue",
           geometry: "Point (101 102)"
         }
       ]
    }

Remember to use the most general Geometry type as your ``json-type`` or
SimpleFeatureType field type. Defining a type ``Geometry`` allows for
polygons, points, and linestrings, but specifying a specific geometry
like point will only allow for parsing of points.
