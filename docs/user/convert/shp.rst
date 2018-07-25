.. _shp_converter:

Shapefile Converter
===================

The Shapefile converter handles shapefiles. To use the Shapefile converter, specify ``type = "shp"`` in your converter
definition.

Configuration
-------------

The Shapefile converter does not have any configuration beyond the basic converter defaults. However,
since a shapefile is a collection of files, and not a single input, there are a few constraints that must be
observed.

Shapefile converters are not usable in distributed converter jobs, as the map/reduce paradigm does not work
well with the collection of related files that comprise a shapefile. In addition, when using a Shapefile converter
it is important to set the input file path in the evaluation context. This is handled automatically by the GeoMesa
CLI tools, but if used programmatically ``"inputFilePath"`` must be set in the evaluation context global parameters.

.. _shp_converter_functions:

Shapefile Transform Functions
-----------------------------

The ``transform`` element supports referencing each attribute in the input shapefile by its column number using
``$``. ``$0`` refers to the feature ID, then the first attribute is ``$1``, etc. Each attribute will be typed
according to the schema of the shapefile.

Additionally, the attributes of the input shapefile may be referenced by name using the ``shp`` function.
The feature ID can be referenced by using the ``shpFeatureId`` function.

The standard functions in :ref:`converter_functions` can be used as normal.

shp
~~~

For ease of use, this will access an attribute in the shapefile by name, e.g. ``shp('my_attribute')``. Note
that attributes can also be referenced by number, as described above.

shpFeatureId
~~~~~~~~~~~~

For ease of use, this will access the feature ID of the current shapefile feature, e.g. ``shpFeatureId``. Note
that the feature ID can also be referenced by ``$0``, as described above.

Example Usage
-------------

This example is for the Tiger US States boundary file available from the
`US Census Bureau <https://www.census.gov/geo/maps-data/data/cbf/cbf_state.html>`__.

The shapefile has the following columns::

  *the_geom:MultiPolygon,STATEFP:String,STATENS:String,AFFGEOID:String,GEOID:String,STUSPS:String,NAME:String,LSAD:String,ALAND:Long,AWATER:Long

From these, we will only consider a subset, using the following SimpleFeatureType::

  geomesa.sfts.cb_2017_us_state_20m = {
    attributes = [
      { name = "name",    type = "String"       }
      { name = "usps",    type = "String"       }
      { name = "area",    type = "Long"         }
      { name = "geom",    type = "MultiPolygon" }
    ]
  }

You could ingest with the following converter::

  geomesa.converters.cb_2017_us_state_20m = {
    type     = "shp"
    id-field = "$0"
    fields = [
      { name = "name", transform = "$7" }, // example of lookup by field number
      { name = "usps", transform = "shp('STUSPS')" }, // example of lookup by name
      { name = "area", transform = "add(shp('ALAND'),shp('AWATER'))" },
      { name = "geom", transform = "shp('the_geom')" },
    ]
  }
