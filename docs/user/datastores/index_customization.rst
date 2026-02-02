.. _index_customization:

Index Customization
-------------------

Instead of using the default indices, you may specify the exact indices to create. This may be used to create
fewer indices (to speed up ingestion, or because you are only using certain query patterns), or to create additional
indices (for example on non-default geometries or dates).

The indices are created when calling ``createSchema``. If nothing is specified, the Z2, Z3 (or XZ2 and XZ3
depending on geometry type) and ID indices will all be created, as well as any attribute indices you have defined.

.. warning::

    Certain queries may be much slower if you disable an index.

Indices are configured using the attribute-level user-data key ``index``. An index may be identified in several ways:

* The string ``true``, which will use the default index for the attribute type
* Name of an index, e.g. ``z3`` or ``attr``, which will use the default secondary attributes for the index type
* Name of an index, plus any secondary attributes, e.g. ``z3:dtg`` or ``attr:geom:dtg``
* Name of an index, version of the index, plus any secondary attributes, e.g. ``z3:7:dtg`` or ``attr:8:geom:dtg``
* The string ``none`` or ``false``, which will disable any default index for the attribute type

Multiple indices on a single attribute may be separated with a comma (``,``).

For attribute indices, if secondary geometry and date attributes are specified, the secondary index will be Z3 of XZ3, as
appropriate. If just a geometry is specified, the secondary index will be Z2 of XZ2, as appropriate. If just a date
is specified, the secondary index will be an ordered temporal index.

The ID index does not correspond to any attribute, so it can be disabled through the feature-level user-data key
``id.index.enabled=false`.`

Examples
--------

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

        // creates a default attribute index on name and an implicit default z3 and z2 index on geom
        String spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4325";
        // creates an attribute index on name (with a secondary date index), and a z3 index on geom and dtg
        spec = "name:String:index='attr:dtg',dtg:Date,*geom:Point:srid=4325:index='z3:dtg'";
        // creates an attribute index on name (with a secondary date index), and a z3 index on geom and dtg and disables the ID index
        spec = "name:String:index='attr:dtg',dtg:Date,*geom:Point:srid=4325:index='z3:dtg';id.index.enabled=false";

        SimpleFeatureType sft = SimpleFeatureTypes.createType("myType", spec);
        // alternatively, set user data after parsing the type string (but before calling "createSchema")
        sft.getDescriptor("name").getUserData().put("index", "true");

    .. code-tab:: scala SchemaBuilder

        import org.locationtech.geomesa.utils.geotools.SchemaBuilder

        val sft =
          SchemaBuilder.builder()
            .addString("name").withIndex("attr:dtg") // creates an attribute index on name, with a secondary date index
            .addInt("age").withIndex() // creates an attribute index on age, with a default secondary index
            .addDate("dtg") // not a primary index
            .addPoint("geom", default = true).withIndices("z3:dtg", "z2") // creates a z3 index with dtg, and a z2 index
            .disableIdIndex() // disables the ID index
            .build("mySft")

    .. code-tab:: hocon Config

        {
          type-name = myType
          attributes = [
            { name = "name", type = "String", index = "attr:dtg" } // creates an attribute index on name, with a secondary date index
            { name = "age", type = "Int", index = "true" } // creates an attribute index on age, with a default secondary index
            { name = "dtg", type = "Date" } // not a primary index
            { name = "geom", type = "Point", srid = "4326", index = "z3:dtg,z2" } // creates a z3 index with dtg, and a z2 index
          ]
          user-data = {
            "id.index.enabled" = "false" // disables the default ID index
          }
        }

Feature-Level Configuration
===========================

Instead of configuring individual attributes, you may set a top-level user data value in your simple feature type using the key
``geomesa.indices.enabled``. The value should contain a comma-delimited list containing a subset of index identifiers, as
specified in :ref:`index_overview` (and optionally an index version and/or list of attributes to include in the index, as
detailed above). When setting indices this way, any attribute-level configuration will be ignored.

.. code-block:: java

    import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;

    String spec = "name:String,dtg:Date,*start:Point:srid=4326,end:Point:srid=4326";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);
    // enable a default z3 index on start + dtg
    sft.getUserData().put("geomesa.indices.enabled", "z3");
    // alternatively, enable a z3 index on start + dtg, end + dtg, and an attribute index on
    // name with a secondary index on dtg. note that this overrides the previous configuration
    sft.getUserData().put("geomesa.indices.enabled", "z3:start:dtg,z3:end:dtg,attr:name:dtg");

See :ref:`set_sft_options` for details on setting user data. If you are using the GeoMesa ``SchemaBuilder``,
you may instead call the ``indices`` method:

.. code-block:: scala

    import org.locationtech.geomesa.utils.geotools.SchemaBuilder

    val sft = SchemaBuilder.builder()
        .addString("name")
        .addDate("dtg")
        .addPoint("geom", default = true)
        .userData
        .indices(List("id", "z3", "attr"))
        .build("mySft")
