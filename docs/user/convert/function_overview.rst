Transformation Function Overview
--------------------------------

The provided transformation functions are listed below.

Control Functions
~~~~~~~~~~~~~~~~~

-  ``try``

String Functions
~~~~~~~~~~~~~~~~

-  ``stripQuotes``
-  ``length``
-  ``trim``
-  ``capitalize``
-  ``lowercase``
-  ``regexReplace``
-  ``concatenate``
-  ``substring``
-  ``toString``

Date Functions
~~~~~~~~~~~~~~

-  ``now``
-  ``date``
-  ``dateTime``
-  ``basicDate``
-  ``basicDateTime``
-  ``basicDateTimeNoMillis``
-  ``dateHourMinuteSecondMillis``
-  ``millisToDate``
-  ``secsToDate``

Geometry Functions
~~~~~~~~~~~~~~~~~~

-  ``point``
-  ``linestring``
-  ``polygon``
-  ``geometry``

ID Functions
~~~~~~~~~~~~

-  ``stringToBytes``
-  ``md5``
-  ``uuid``
-  ``base64``

Type Conversions
~~~~~~~~~~~~~~~~

-  ``::int`` or ``::integer``
-  ``::long``
-  ``::float``
-  ``::double``
-  ``::boolean``
-  ``::r``
-  ``stringToInt`` or ``stringToInteger``
-  ``stringToLong``
-  ``stringToFloat``
-  ``stringToDouble``
-  ``stringToBoolean``

List and Map Parsing
~~~~~~~~~~~~~~~~~~~~

-  ``parseList``
-  ``parseMap``

Functions defined using scripting languages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can define functions using scripting languages that support JSR-223.
This is currently tested with JavaScript only as it is natively
supported in all JREs via the Nashorn extension. To define a JavaScript
function for use in the converter framework, either put the file in
``geomesa-convert-scripts`` on the classpath or set the system property
``geomesa.convert.scripts.path`` to be a comma-separated list of paths
to load functions from. Then, any function you define in a file in one
of those paths will be available in a convert definition with a
namespace prefix. For instance, if you have defined a function such as

.. code-block:: javascript

    function hello(s) {
       return "hello: " + s;
    }

you can reference that function in a transform expression as
``js:hello($2)``

CQL Functions
~~~~~~~~~~~~~

Most of the basic CQL functions are available as transformations. To use
one, invoke it like a regular function, prefixed with the ``cql``
namespace. For example, you can use the CQL buffer function to turn a
point into a polygon:

::

    cql:buffer($1, 2.0)

For more information on the various CQL functions, see the GeoServer
`filter function
reference <http://docs.geoserver.org/stable/en/user/filter/function_reference.html#filter-function-reference>`__.

JSON/Avro Transformations
~~~~~~~~~~~~~~~~~~~~~~~~~

See Parsing Json and Parsing Avro sections