Transformation Function Usage
-----------------------------

Control Functions
~~~~~~~~~~~~~~~~~

try
^^^

Description: Execute another function - if it fails, instead use a
default value

Usage: ``try($1, $2)``

Example: ``try("1"::int, 0) = 1``

Example: ``try("abcd"::int, 0) = 0``

String Functions
~~~~~~~~~~~~~~~~

stripQuotes
^^^^^^^^^^^

Description: Remove double quotes from a string.

Usage: ``stripQuotes($1)``

Example: ``stripQuotes('fo"o') = foo``

length
^^^^^^

Description: Returns the length of a string.

Usage: ``length($1)``

Example: ``length('foo') = 3``

trim
^^^^

Description: Trim whitespace from around a string.

Usage: ``trim($1)``

Example: ``trim('  foo ') = foo``

capitalize
^^^^^^^^^^

Description: Capitalize a string.

Usage: ``capitalize($1)``

Example: ``capitalize('foo') = Foo``

lowercase
^^^^^^^^^

Description: Lowercase a string.

Usage: ``lowercase($1)``

Example: ``lowercase('FOO') = foo``

uppercase
^^^^^^^^^

Description: Uppercase a string.

Usage: ``uppercase($1)``

Example: ``uppercase('foo') = FOO``

regexReplace
^^^^^^^^^^^^

Description: Replace a given pattern with a target pattern in a string.

Usage: ``regexReplace($regex, $replacement, $1)``

Example: ``regexReplace('foo'::r, 'bar', 'foobar') = barbar``

concatenate
^^^^^^^^^^^

Description: Concatenate two strings.

Usage: ``concatenate($0, $1)``

Example: ``concatenate('foo', 'bar') = foobar``

substring
^^^^^^^^^

Description: Return the substring of a string.

Usage: ``substring($1, $startIndex, $endIndex)``

Example: ``substring('foobarbaz', 2, 5) = oba``

toString
^^^^^^^^

Description: Convert another data type to a string.

Usage: ``toString($0)``

Example: ``concatenate(toString(5), toString(6)) = '56'``

emptyToNull
^^^^^^^^^^^

Description: Replace an empty string with ``null``. Useful for setting optional attributes from delimited
text files, where inputs will never be ``null``.

Usage: ``emptyToNull($0)``

Example: ``emptyToNull('') = null``

Date Functions
~~~~~~~~~~~~~~

now
^^^

Description: Use the current system time.

Usage: ``now()``

date
^^^^

Description: Custom date parser. The date format is defined by the Java 8 `DateTimeFormatter`__ class.

.. __: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html

Usage: ``date($format, $1)``

Example:
``date('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', '2015-01-01T00:00:00.000000')``

dateTime
^^^^^^^^

Description: A strict ISO 8601 Date parser for format
``yyyy-MM-dd'T'HH:mm:ss.SSSZZ``.

Usage: ``dateTime($1)``

Example: ``dateTime('2015-01-01T00:00:00.000Z')``

basicDate
^^^^^^^^^

Description: A basic date format for ``yyyyMMdd``.

Usage: ``basicDate($1)``

Example: ``basicDate('20150101')``

basicDateTime
^^^^^^^^^^^^^

Description: A basic format that combines a basic date and time for
format ``yyyyMMdd'T'HHmmss.SSSZ``.

Usage: ``basicDateTime($1)``

Example: ``basicDateTime('20150101T000000.000Z')``

basicDateTimeNoMillis
^^^^^^^^^^^^^^^^^^^^^

Description: A basic format that combines a basic date and time with no
millis for format ``yyyyMMdd'T'HHmmssZ``.

Usage: ``basicDateTimeNoMillis($1)``

Example: ``basicDateTimeNoMillis('20150101T000000Z')``

dateHourMinuteSecondMillis
^^^^^^^^^^^^^^^^^^^^^^^^^^

Description: Formatter for full date, and time keeping the first 3
fractional seconds for format ``yyyy-MM-dd'T'HH:mm:ss.SSS``.

Usage: ``dateHourMinuteSecondMillis($1)``

Example: ``dateHourMinuteSecondMillis('2015-01-01T00:00:00.000')``

millisToDate
^^^^^^^^^^^^

Description: Create a new date from a long representing milliseconds
since January 1, 1970.

Usage: ``millisToDate($1)``

Example: ``millisToDate('1449675054462'::long)``

secsToDate
^^^^^^^^^^

Description: Create a new date from a long representing seconds since
January 1, 1970.

Usage: ``secsToDate($1)``

Example: ``secsToDate(1449675054)``

Geometry Functions
~~~~~~~~~~~~~~~~~~

point
^^^^^

Description: Parse a Point geometry from lon/lat or WKT.

Usage: ``point($lon, $lat)`` or ``point($wkt)``

Note: Ordering is important here...GeoMesa defaults to longitude first

Example: Parsing lon/lat from JSON:

Parsing lon/lat

::

    # config
    { name = "lon", json-type="double", path="$.lon" }
    { name = "lat", json-type="double", path="$.lat" }
    { name = "geom", transform="point($lon, $lat)" }

    # data
    {
        "lat": 23.9,
        "lon": 24.2,
    }

Example: Parsing lon/lat from text without creating lon/lat fields:

::

    # config
    { name = "geom", transform="point($2::double, $3::double)" }

    # data
    id,lat,lon,date
    identity1,23.9,24.2,2015-02-03

Example: Parsing WKT as a point

::

    # config
    { name = "geom", transform="point($2)" }

    # data
    ID,wkt,date
    1,POINT(2 3),2015-01-02

linestring
^^^^^^^^^^

Description: Parse a linestring from a WKT string.

Usage: ``linestring($0)``

Example: ``linestring('LINESTRING(102 0, 103 1, 104 0, 105 1)')``

polygon
^^^^^^^

Description: Parse a polygon from a WKT string.

Usage: ``polygon($0)``

Example: ``polygon('polygon((100 0, 101 0, 101 1, 100 1, 100 0))')``

geometry
^^^^^^^^

Description: Parse a geometry from a WKT string or GeoJson.

Usage: ``geometry($0)``

Example: Parsing WKT as a geometry

::

    # config
    { name = "geom", transform="geometry($2)" }

    # data
    ID,wkt,date
    1,POINT(2 3),2015-01-02

Example: Parsing GeoJson geometry

::

    # config
    { name = "geom", json-type = "geometry", path = "$.geometry" }

    # data
    {
        id: 1,
        number: 123,
        color: "red",
        "geometry": {"type": "Point", "coordinates": [55, 56]}
    }

projectFrom
^^^^^^^^^^^

Description: Project a geometry from its native CRS to EPSG:4326. GeoMesa only supports EPSG:4326,
so geometries must be transformed when ingesting from another CRS.

Usage: ``projectFrom('EPSG:3857',$0)``

Example: Reprojecting a parsed point from EPSG:3857 to EPSG:4326:

::

    # config
    { name = "geom", transform="projectFrom('EPSG:3857',point($2::double, $3::double))" }

    # data
    id,x,y,date
    identity1,1689200.14,1113194.91,2015-02-03

ID Functions
~~~~~~~~~~~~

stringToBytes
^^^^^^^^^^^^^

Description: Converts a string to a UTF-8 byte array (to pass to ``md5()`` or ``base64()``).

Usage: ``stringToBytes($0)``

Example: ``stringToBytes('row,of,data')``

md5
^^^

Description: Creates an MD5 hash from a byte array.

Usage: ``md5($0)``

Example: ``md5(stringToBytes('row,of,data'))``

uuid
^^^^

Description: Generates a random UUID.

Usage: ``uuid()``

base64
^^^^^^

Description: Encodes a byte array as a base-64 string.

Usage; ``base64($0)``

Example: ``base64(stringToBytes('foo'))``

Type Conversions
~~~~~~~~~~~~~~~~

::int or ::integer
^^^^^^^^^^^^^^^^^^

Description: Converts a string into an integer. Invalid values will
cause the record to fail.

Example: ``'1'::int = 1``

::long
^^^^^^

Description: Converts a string into a long. Invalid values will cause
the record to fail.

Example: ``'1'::long = 1L``

::float
^^^^^^^

Description: Converts a string into a float. Invalid values will cause
the record to fail.

Example: ``'1.0'::float = 1.0f``

::double
^^^^^^^^

Description: Converts a string into a double. Invalid values will cause
the record to fail.

Example: ``'1.0'::double = 1.0d``

::boolean
^^^^^^^^^

Description: Converts a string into a boolean. Invalid values will cause
the record to fail.

Example: ``'true'::boolean = true``

::r
^^^

Description: Converts a string into a Regex object.

Example: ``'f.*'::r = f.*: scala.util.matching.Regex``

stringToInt or stringToInteger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Description: Converts a string into a integer, with a default value if
conversion fails.

Usage: ``stringToInt($1, $2)``

Example: ``stringToInt('1', 0) = 1``

Example: ``stringToInt('', 0) = 0``

stringToLong
^^^^^^^^^^^^

Description: Converts a string into a long, with a default value if
conversion fails.

Usage: ``stringToLong($1, $2)``

Example: ``stringToLong('1', 0L) = 1L``

Example: ``stringToLong('', 0L) = 0L``

stringToFloat
^^^^^^^^^^^^^

Description: Converts a string into a float, with a default value if
conversion fails.

Usage: ``stringToFloat($1, $2)``

Example: ``stringToFloat('1.0', 0.0f) = 1.0f``

Example: ``stringToFloat('not a float', 0.0f) = 0.0f``

stringToDouble
^^^^^^^^^^^^^^

Description: Converts a string into a double, with a default value if
conversion fails.

Usage: ``stringToDouble($1, $2)``

Example: ``stringToDouble('1.0', 0.0) = 1.0d``

Example: ``stringToDouble(null, 0.0) = 0.0d``

stringToBoolean
^^^^^^^^^^^^^^^

Description: Converts a string into a boolean, with a default value if
conversion fails.

Usage: ``stringToBoolean($1, $2)``

Example: ``stringToBoolean('true', false) = true``

Example: ``stringToBoolean('55', false) = false``

Math Functions
~~~~~~~~~~~~~~

Usage:

All math functions accept: Integers, Doubles, Floats, Longs and parsable Strings.
All math functions return: Doubles. If another data type is needed, convert the value afterwards. e.g. ``add($1,$2)::long``

Example:

::

  { name = "value3",   transform = "add($value1, multiply($value2, 1.2))::double" }

add
^^^

Description: Adds two or more values.

Example: ``add($1,$2)``

Example: ``add($1,$2,"10")``

subtract
^^^^^^^^

Description: Subtracts two or more values.

Example: ``subtract($1,$2)``

Example: ``subtract($1,$2,1.0f)``

multiply
^^^^^^^^

Description: Multiply two or more values.

Example: ``multiply($1,$2)``

Example: ``multiply($1,$2,0.01d)``

divide
^^^^^^

Description: Divides two or more values sequentially.

Example: ``divide($1,$2)``

Example: ``divide($1,$2,"15")`` is equivalent to ``($1/$2)/"15"``

List and Map Parsing
~~~~~~~~~~~~~~~~~~~~

parseList
^^^^^^^^^

Description: Parse a ``List[T]`` type from a string.

If your SimpleFeatureType config contains a list or map you can easily
configure a transform function to parse it using the ``parseList``
function which takes either 2 or 3 args

1. The primitive type of the list (int, string, double, float, boolean,
   etc)
2. The reference to parse
3. Optionally, the list delimiter (defaults to a comma)

Here's some sample CSV data:

::

    ID,Name,Age,LastSeen,Friends,Lat,Lon
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan",-100.236523,23
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry",40.232,-53.2356
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort",3,-62.23

For example, an SFT may specific a field:

::

    { name = "friends", type = "List[String]" }

And a transform to parse the quoted CSV field:

::

    { name = "friends", transform = "parseList('string', $5)" }

parseMap
^^^^^^^^

Description: Parse a ``Map[T,V]`` type from a string.

Parsing Maps is similar. Take for example this CSV data with a quoted
map field:

::

    1,"1->a,2->b,3->c,4->d",2013-07-17,-90.368732,35.3155
    2,"5->e,6->f,7->g,8->h",2013-07-17,-70.970585,42.36211
    3,"9->i,10->j",2013-07-17,-97.599004,30.50901

Our field type is:

::

    numbers:Map[Integer,String]

Then we specify a transform:

::

    { name = "numbers", transform = "parseMap('int -> string', $2)" }

Optionally we can also provide custom list/record and key-value
delimiters for a map:

::

    { name = "numbers", transform = "parseMap('int -> string', $2, ',', '->')" }


Enrichment Functions
~~~~~~~~~~~~~~~~~~~~

cacheLookup
^^^^^^^^^^^

Description: Looks up a value from a cache

Usage: ``cacheLookup(<cacheName>, <entityKey>, <attributeKey>)``

Example: ``cacheLookup('test', $id, 'name')``
