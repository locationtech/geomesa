.. _converter_functions:

Transformation Function Overview
--------------------------------

Type Conversions
~~~~~~~~~~~~~~~~

-  ``::int`` or ``::integer``
-  ``::long``
-  ``::float``
-  ``::double``
-  ``::boolean``
-  ``::r``
-  ``toInt`` or ``toInteger``
-  ``toLong``
-  ``toFloat``
-  ``toDouble``
-  ``toBoolean``
-  ``intToBoolean``

String Functions
~~~~~~~~~~~~~~~~

-  ``strip``
-  ``stripPrefix``
-  ``stripSuffix``
-  ``stripQuotes``
-  ``replace``
-  ``removeChars``
-  ``length``
-  ``trim``
-  ``capitalize``
-  ``lowercase``
-  ``regexReplace``
-  ``concatenate``
-  ``substring``
-  ``toString``
-  ``emptyToNull``
-  ``printf``

Date Functions
~~~~~~~~~~~~~~

-  ``now``
-  ``date``
-  ``dateTime``
-  ``basicIsoDate``
-  ``isoDate``
-  ``isoLocalDate``
-  ``basicDateTime``
-  ``isoDateTime``
-  ``isoLocalDateTime``
-  ``isoOffsetDateTime``
-  ``basicDateTimeNoMillis``
-  ``dateHourMinuteSecondMillis``
-  ``millisToDate``
-  ``secsToDate``
-  ``dateToString``
-  ``dateToMillis``

Geometry Functions
~~~~~~~~~~~~~~~~~~

-  ``point``
-  ``pointM``
-  ``multipoint``
-  ``linestring``
-  ``multilinestring``
-  ``polygon``
-  ``multipolygon``
-  ``geometrycollection``
-  ``geometry``
-  ``projectFrom``

ID Functions
~~~~~~~~~~~~

-  ``stringToBytes``
-  ``md5``
-  ``murmur3_32``
-  ``murmur3_64``
-  ``murmurHash3``
-  ``uuid``
-  ``uuidZ3``
-  ``uuidZ3Centroid``

Math Functions
~~~~~~~~~~~~~~

-  ``add``
-  ``subtract``
-  ``multiply``
-  ``divide``
-  ``mean``
-  ``min``
-  ``max``

List and Map Functions
~~~~~~~~~~~~~~~~~~~~~~

-  ``list``
-  ``listItem``
-  ``mapValue``
-  ``parseList``
-  ``parseMap``
-  ``transformListItems``

Encoding Functions
~~~~~~~~~~~~~~~~~~

-  ``base64Encode``
-  ``base64Decode``

Control Functions
~~~~~~~~~~~~~~~~~

-  ``try``
-  ``withDefault``
-  ``require``

State Functions
~~~~~~~~~~~~~~~

-  ``inputFilePath``
-  ``lineNo``

.. _convert_scripting_functions:

Functions defined using scripting languages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can define functions using scripting languages that support JSR-223.
This is currently tested with JavaScript only as it is natively
supported in all JREs via the Nashorn extension. To define a JavaScript
function for use in the converter framework, create a file with the
``.js`` extension and the function definition as the contents of the file.
For instance, if you have defined a function such as

.. code-block:: javascript

    function hello(s) {
       return "hello: " + s;
    }

you can reference that function in a transform expression as
``js:hello($2)``

Installing Custom Scripts
~~~~~~~~~~~~~~~~~~~~~~~~~

Custom scripting functions are made available to GeoMesa comamnd line tools or
distributed (map-reduce) ingest via including them on the classpath or
setting a system property.

For local usage, geomesa defines the system property ``geomesa.convert.scripts.path``
to be a colon-separated list of script files and/or directories containing scripts.
This system property can be set when using the command line tools by setting the
``CUSTOM_JAVA_OPTS`` environmental variable:

.. code-block:: bash

    CUSTOM_JAVA_OPTS="-Dgeomesa.convert.scripts.path=/path/to/script.js:/path/to/script-dir/"

A more resilient method of including custom scripts is to package them as a JAR or ZIP
file and add it to the ``GEOMESA_EXTRA_CLASSPATHS`` environmental variable. If using
maven you can simply package them in a folder under ``src/main/resources/geomesa-convert-scripts/``
which will create a folder in your jar file named ``geomesa-convert-scripts`` with
the scripts inside. You can manually create a jar with this folder as well. An easier way
is often to package them as a zip archive with a folder similary named ``geomesa-convert-scripts``
inside the archive containing the scripts:

.. code-block:: bash

    $ unzip -l /tmp/scripts.zip
    Archive:  /tmp/scripts.zip
      Length      Date    Time    Name
    ---------  ---------- -----   ----
            0  2017-03-09 11:33   geomesa-convert-scripts/
           42  2017-03-09 11:33   geomesa-convert-scripts/my-script.js
    ---------                     -------
           42                     2 files

For either zip or jar files add them to the extra classpaths in your environment to
make them available for the tools or map-reduce ingest:

.. code-block:: bash

    GEOMESA_EXTRA_CLASSPATHS="/path/to/my-scripts.jar:/tmp/scripts.zip"

A example of ingest with a scripts on the classpath is below:

.. code-block:: bash

    GEOMESA_EXTRA_CLASSPATHS="/tmp/scripts.zip:/path/to/my-scripts.jar" bin/geomesa-accumulo ingest -u <user-name>
    -p <password> -s <sft-name> -C <converter-name> -c geomesa.catalog hdfs://localhost:9000/data/example.csv

You can also verify the classpath is properly configured with the tools:

.. code-block:: bash

    GEOMESA_EXTRA_CLASSPATHS="/tmp/scripts.zip:/path/to/my-scripts.jar" bin/geomesa-accumulo classpath


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

See :ref:`json_converter_functions` and :ref:`avro_converter_functions`.

Enrichment Functions
~~~~~~~~~~~~~~~~~~~~

The converter framework provides a mechanism for setting an attribute based on a lookup
from a cache.  The cache can be a literal cache in the system or in an external system such
as Redis.

- ``cacheLookup``
