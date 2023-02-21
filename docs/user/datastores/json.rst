.. _json_attributes:

JSON Attributes
===============

GeoMesa supports native JSON integration with GeoTools data stores. Simple feature ``String``-type attributes
can be marked as JSON and then queried using CQL. JSON attributes must be specified when creating a simple
feature type:

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
----------------------------

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

JSONPath With GeoServer Styles
------------------------------

When using JSON path in GeoServer styles (SLD or CSS), the attribute and path must be separated out in order for
the GeoTools renderer to work correctly. In this case, pass in two arguments, the first being a property expression
in double quotes of the JSON-type attribute name, and the second being the path:

.. code-block:: none

    * {
      mark: symbol(arrow);
      mark-size: 12px;
      mark-rotation: [ jsonPath("json", 'foo') ];
      :mark {
        fill: #009900;
      }
    }
