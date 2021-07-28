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

withDefault
^^^^^^^^^^^

Description: Replace a value with the first non-null alternative, if the value is null

Usage: ``withDefault($1, $2...)``

Example: ``withDefault('foo', 'bar') = foo``

Example: ``withDefault('foo', 'bar', 'baz') = foo``

Example: ``withDefault(null, 'bar', 'baz') = bar``

Example: ``withDefault(null, null, 'baz') = baz``

require
^^^^^^^

Description: Throw an exception if the value is null, otherwise return the value

Usage: ``require($1)``

Example: ``require('foo') = foo``

Example: ``require(null) // throws an error``

String Functions
~~~~~~~~~~~~~~~~

strip
^^^^^

Description: Removes characters from the start or end of a string. Defaults to whitespace.

Usage: ``strip($1)`` or ``strip($1, $chars)``

Examples: ``strip('afoob', 'abc') = foo``
``strip('foao', 'abc') = foao``
``strip('\t foo ') = foo``

stripQuotes
^^^^^^^^^^^

Description: Remove double or single quotes from a the start or end of a string

Usage: ``stripQuotes($1)``

Examples: ``stripQuotes('"foo"') = foo``
``stripQuotes('\'foo\'') = foo``
``stripQuotes('fo"o') = fo"o``

stripPrefix
^^^^^^^^^^^

Description: Removes characters from the start of a string. Whitespace is preserved.

Usage: ``stripPrefix($1, $chars)``

Examples: ``stripPrefix('afoob', 'abc') = foob``

stripSuffix
^^^^^^^^^^^

Description: Removes characters from the end of a string. Whitespace is preserved.

Usage: ``stripSuffix($1, $chars)``

Examples: ``stripSuffix('afoob', 'abc') = afoo``

remove
^^^^^^

Description: Removes a substring from a string

Usage: ``remove($1, $substring)``

Examples: ``remove('foabco', 'abc') = foo``

replace
^^^^^^^

Description: Replaces a literal string with another string

Usage: ``replace($1, $toReplace, $replacement)``

Examples: ``replace('foobar', 'ob', 'ab') = foabar``

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

printf
^^^^^^

Description: Format custom strings.  As an implementation detail,
this function delegates to Java's String `formatting classes`__.

.. __: https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html

Usage: ``printf('patterns', $arg1, $arg2, ...)'``

Examples: ``printf('%s-%s-%sT00:00:00.000Z', '2015', '01', '01') = '2015-01-01T00:00:00.000Z'``
          ``printf('%2f', divide(-1, 2, 3)) = '-0.17'``

Date Functions
~~~~~~~~~~~~~~

The following table summarizes the predefined date formats by name. For dates that don't match any of these
formats, a custom format can be used with the ``date`` function. ``secsToDate`` and ``millisToDate`` can be used
for parsing intervals since the Java epoch. See below for full descriptions of each function.

========================== =============================== ========================
Function                   Format                          Example
========================== =============================== ========================
basicIsoDate               ``yyyyMMdd``                    20150101
isoDate                    ``yyyy-MM-dd``                  2015-01-01
isoLocalDate               ``yyyy-MM-dd``                  2015-01-01
basicDateTimeNoMillis      ``yyyyMMdd'T'HHmmssZ``          20150101T000000Z
basicDateTime              ``yyyyMMdd'T'HHmmss.SSSZ``      20150101T000000.000Z
isoDateTime                ``yyyy-MM-dd'T'HH:mm:ss``       2015-01-01T00:00:00
isoLocalDateTime           ``yyyy-MM-dd'T'HH:mm:ss``       2015-01-01T00:00:00
isoOffsetDateTime          ``yyyy-MM-dd'T'HH:mm:ssZ``      2015-01-01T00:00:00Z
dateHourMinuteSecondMillis ``yyyy-MM-dd'T'HH:mm:ss.SSS``   2015-01-01T00:00:00.000
dateTime                   ``yyyy-MM-dd'T'HH:mm:ss.SSSZ``  2015-01-01T00:00:00.000Z
========================== =============================== ========================

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

basicIsoDate
^^^^^^^^^^^^

Description: A date format for ``yyyyMMdd``, equivalent to java.time.format.DateTimeFormatter.BASIC_ISO_DATE.

Usage: ``basicIsoDate($1)``

Example: ``basicIsoDate('20150101')``

isoDate
^^^^^^^

Description: A date format for ``yyyy-MM-dd``, equivalent to java.time.format.DateTimeFormatter.ISO_DATE.

Usage: ``isoDate($1)``

Example: ``isoDate('2015-01-01')``

basicDateTime
^^^^^^^^^^^^^

Description: A date format that combines a basic date and time for ``yyyyMMdd'T'HHmmss.SSSZ``.

Usage: ``basicDateTime($1)``

Example: ``basicDateTime('20150101T000000.000Z')``

basicDateTimeNoMillis
^^^^^^^^^^^^^^^^^^^^^

Description: A basic format that combines a basic date and time with no
millis for format ``yyyyMMdd'T'HHmmssZ``.

Usage: ``basicDateTimeNoMillis($1)``

Example: ``basicDateTimeNoMillis('20150101T000000Z')``

dateTime
^^^^^^^^

Description: A strict ISO 8601 Date parser for format
``yyyy-MM-dd'T'HH:mm:ss.SSSZZ``.

Usage: ``dateTime($1)``

Example: ``dateTime('2015-01-01T00:00:00.000Z')``

dateHourMinuteSecondMillis
^^^^^^^^^^^^^^^^^^^^^^^^^^

Description: Formatter for full date, and time keeping the first 3
fractional seconds for format ``yyyy-MM-dd'T'HH:mm:ss.SSS``.

Usage: ``dateHourMinuteSecondMillis($1)``

Example: ``dateHourMinuteSecondMillis('2015-01-01T00:00:00.000')``

isoDateTime
^^^^^^^^^^^

Description: A date format for ``yyyy-MM-dd'T'HH:mm:ss``, equivalent to
java.time.format.DateTimeFormatter.ISO_DATE_TIME.

Usage: ``isoDateTime($1)``

Example: ``isoDateTime('2015-01-01T00:00:00')``

isoLocalDate
^^^^^^^^^^^^

Description: A date format for ``yyyy-MM-dd``, equivalent to java.time.format.DateTimeFormatter.ISO_LOCAL_DATE.

Usage: ``isoLocalDate($1)``

Example: ``isoLocalDate('2015-01-01')``

isoLocalDateTime
^^^^^^^^^^^^^^^^

Description: A date format for ``yyyy-MM-dd'T'HH:mm:ss``, equivalent to
java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME.

Usage: ``isoLocalDateTime($1)``

Example: ``isoLocalDateTime('2015-01-01T00:00:00')``

isoOffsetDateTime
^^^^^^^^^^^^^^^^^
Description: A date format for ``yyyy-MM-dd'T'HH:mm:ssZ``, equivalent to
java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME.

Usage: ``isoOffsetDateTime($1)``

Example: ``isoOffsetDateTime('2015-01-01T00:00:00Z')``

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

dateToString
^^^^^^^^^^^^

Description: Formats a date as a string, based on a pattern as defined by Java's
`DateTimeFormatter <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html>`__.

Usage: ``dateToString($pattern, $date)``

Example: ``dateToString('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', now())``

dateToMillis
^^^^^^^^^^^^

Description: Converts a date to milliseconds since the Java epoch (January 1, 1970).

Usage: ``dateToMillis($date)``

Example: ``dateToMillis(now())``

Geometry Functions
~~~~~~~~~~~~~~~~~~

point
^^^^^

Description: Parse a Point geometry from lon/lat/z/m, WKT or WKB. To create a point with measure but no z,
use ``pointM``.

Usage: ``point($lon, $lat)``, ``point($lon, $lat, $z)``, ``point($lon, $lat, $z, $m)`` or ``point($wkt)``

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

pointM
^^^^^^

Description: Parse a Point geometry from lon/lat and measure

Usage: ``pointM($lon, $lat, $measure)``

Example: ``pointM(10::double,20::double,30::double)``

multipoint
^^^^^^^^^^

Description: Parse a multi-point from a WKT string, WKB byte array, or two lists of coordinates.

Usage: ``multipoint($0)``

Example: ``multipoint('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')``

Example: ``multipoint(list(10,40,20,30),list(40,30,20,10))``

linestring
^^^^^^^^^^

Description: Parse a linestring from a WKT string, WKB byte array, or two lists of coordinates.

Usage: ``linestring($0)``

Example: ``linestring('LINESTRING(102 0, 103 1, 104 0, 105 1)')``

Example: ``linestring(list(102,103,104,105),list(0,1,0,1))``

multilinestring
^^^^^^^^^^^^^^^

Description: Parse a multi-linestring from a WKT string or WKB byte array.

Usage: ``multilinestring($0)``

Example: ``multilinestring('MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))')``

polygon
^^^^^^^

Description: Parse a polygon from a WKT string or WKB byte array.

Usage: ``polygon($0)``

Example: ``polygon('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')``

multipolygon
^^^^^^^^^^^^

Description: Parse a multi-polygon from a WKT string or WKB byte array.

Usage: ``multipolygon($0)``

Example: ``multipolygon('MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))')``

geometrycollection
^^^^^^^^^^^^^^^^^^

Description: Parse a geometry collection from a WKT string or WKB byte array.

Usage: ``geometrycollection($0)``

Example: ``geometrycollection('GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))')``

geometry
^^^^^^^^

Description: Parse a geometry from a WKT string or WKB byte array.

Usage: ``geometry($0)``

Example: Parsing WKT as a geometry

::

    # config
    { name = "geom", transform="geometry($2)" }

    # data
    ID,wkt,date
    1,POINT(2 3),2015-01-02
    2,"LINESTRING(102 0, 103 1, 104 0, 105 1)",2015-01-03

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

Description: Converts a string to a UTF-8 byte array (to pass to other functions like ``md5()``).

Usage: ``stringToBytes($0)``

Example: ``stringToBytes('row,of,data')``

md5
^^^

Description: Creates an MD5 hash from a byte array.

Usage: ``md5($0)``

Example: ``md5(stringToBytes('row,of,data'))``

murmur3_32
^^^^^^^^^^

Description: Creates a 32-bit murmur3 hash from a string.

Usage: ``murmur3_32($0)``

Example: ``murmur3_32('row,of,data')``

murmur3_64
^^^^^^^^^^

Description: Creates a 64-bit murmur3 hash from a string. Note that previously this function was incorrectly
named ``murmur3_128``, and can still be invoked by that name.

Usage: ``murmur3_64($0)``

Example: ``murmur3_64('row,of,data')``

murmurHash3
^^^^^^^^^^^

Description: Creates a 128-bit murmur3 hash from a string or byte array, returned as a hex string.

Usage: ``murmurHash3($0)``

Example: ``murmurHash3('row,of,data')``

uuid
^^^^

Description: Generates a random UUID.

Usage: ``uuid()``

uuidZ3
^^^^^^

Description: Generates a Z3-based UUID for point geometries.

Usage: ``uuidZ3($geom, $date, $interval)``

Example: ``uuidZ3(point('POINT (3 2)'), dateTime('2015-01-01T00:00:00.000Z'), 'week')``

See :ref:`customizing_z_index` for details on Z3 intervals.

uuidZ3Centroid
^^^^^^^^^^^^^^

Description: Generates a Z3-based UUID for non-point geometries.

Usage: ``uuidZ3Centroid($geom, $date, $interval)``

Example: ``uuidZ3Centroid(linestring('LINESTRING(102 0, 103 1, 104 0, 105 1)', dateTime('2015-01-01T00:00:00.000Z'), 'week')``

See :ref:`customizing_z_index` for details on Z3 intervals.

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

toInt or toInteger
^^^^^^^^^^^^^^^^^^

Description: Converts a value into a integer. If the conversion fails, returns null unless a default value is defined.

Usage: ``toInt($1, $2)``

Example: ``toInt('1', 0) = 1``

Example: ``toInt('', 0) = 0``

Example: ``toInt('') = null``

toLong
^^^^^^

Description: Converts a value into a long. If the conversion fails, returns null unless a default value is defined.

Usage: ``toLong($1, $2)``

Example: ``toLong('1', 0L) = 1L``

Example: ``toLong('', 0L) = 0L``

Example: ``toLong('') = null``

toFloat
^^^^^^^

Description: Converts a value into a float. If the conversion fails, returns null unless a default value is defined.

Usage: ``toFloat($1, $2)``

Example: ``toFloat('1.0', 0.0f) = 1.0f``

Example: ``toFloat('not a float', 0.0f) = 0.0f``

Example: ``toFloat('') = null``

toDouble
^^^^^^^^

Description: Converts a value into a double. If the conversion fails, returns null unless a default value is defined.

Usage: ``toDouble($1, $2)``

Example: ``toDouble('1.0', 0.0) = 1.0d``

Example: ``toDouble(null, 0.0) = 0.0d``

Example: ``toDouble('') = null``

toBoolean
^^^^^^^^^

Description: Converts a value into a boolean. If the conversion fails, returns null unless a default value is
defined. If the input is a number, it will evaluate to false if it is equal to zero, and true otherwise.

Usage: ``toBoolean($1, $2)``

Example: ``toBoolean('true', false) = true``

Example: ``toBoolean('foo', false) = false``

Example: ``toBoolean('') = null``

intToBoolean
^^^^^^^^^^^^

Description: Converts an integer to boolean. Follows the normal rules of conversion, where 0 is false and all
other values are true.

Usage: ``intToBoolean($1)``

Example: ``intToBoolean(1) = true``

Example: ``intToBoolean(0) = false``

Math Functions
~~~~~~~~~~~~~~

Usage:

The arguments to a math functions must be numbers - Integers, Doubles, Floats, Longs or numeric Strings.

All math functions return Doubles. If another data type is needed, convert the value afterwards,
e.g. ``add($1,$2)::long``

Math functions accept multiple arguments, or will accept a single java.util.List containing the arguments.

Example:

::

  { name = "value3",   transform = "add($value1, multiply($value2, 1.2))::double" }

add
^^^

Description: Adds two or more values.

Example: ``add($1,$2)``

Example: ``add($1,$2,"10")``

Example: ``add(list(1,2,3))``

subtract
^^^^^^^^

Description: Subtracts two or more values sequentially.

Example: ``subtract($1,$2)``

Example: ``subtract($1,$2,1.0f)`` is equivalent to ``($1 - $2) - 1.0f``

Example: ``subtract(list(3,1))``

multiply
^^^^^^^^

Description: Multiply two or more values.

Example: ``multiply($1,$2)``

Example: ``multiply($1,$2,0.01d)``

Example: ``multiply(list(3,2))``

divide
^^^^^^

Description: Divides two or more values sequentially.

Example: ``divide($1,$2)``

Example: ``divide($1,$2,"15")`` is equivalent to ``($1/$2)/"15"``

Example: ``divide(list(3,2))``

mean
^^^^

Description: Takes the mean (average) of two or more numbers.

Example: ``mean($1,$2,$3)``

Example: ``mean(list(1,2,3))``

min
^^^

Description: Finds the minimum of two or more numbers.

Example: ``min($1,$2,$3)``

Example: ``min(list(1,2,3))``

max
^^^

Description: Finds the maximum of two or more numbers.

Example: ``max($1,$2,$3)``

Example: ``max(list(1,2,3))``

List and Map Functions
~~~~~~~~~~~~~~~~~~~~~~

list
^^^^

Description: Creates a list from the input arguments

Example: ``list(1,2,3)``

listItem
^^^^^^^^

Description: Selects an element out of a list

Example: ``listItem(list('1','2','3'),0)``

mapValue
^^^^^^^^

Description: Read a value out of a map instance by key

Example: ``mapValue($map,'key')``

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

    ID,Name,Age,LastSeen,Friends,Lon,Lat
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan",-100.236523,23
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry",40.232,-53.2356
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort",3,-62.23

For example, an SFT may specific a field:

::

    { name = "friends", type = "List[String]" }

And a transform to parse the quoted CSV field:

::

    { name = "friends", transform = "parseList('string', $5)" }

.. _converter_parse_map_fn:

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

    { name = "numbers", transform = "parseMap('int -> string', $2, '->', ',')" }

transformListItems
^^^^^^^^^^^^^^^^^^

Description: Applies a transform expression to every element of a list

Example: ``transformListItems(list('1','2','3'),'stringToDouble($0)')``

The expression to apply must be defined as a string. In the example shown, the list will be converted
from ``List[String]`` to ``List[Double]``.

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
Encoding Functions
~~~~~~~~~~~~~~~~~~

base64Encode
^^^^^^^^^^^^

Description: Encodes a byte array as a base-64 URL-safe string. This function can also be invoked as ``base64``,
but that name has been deprecated and will be removed in future versions.

Usage: ``base64Encode($0)``

Example: ``base64Encode(stringToBytes('foo'))``

base64Decode
^^^^^^^^^^^^

Description: Decodes a base-64 URL-safe encoded string into a byte array.

Usage: ``base64Decode($0)``

Example: ``base64Decode('Zm9v')``

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
State Functions
~~~~~~~~~~~~~~~

inputFilePath
^^^^^^^^^^^^^

Description: provides the absolute path to the file being operated on, if available

Example: ``$inputFilePath``

The file path is a variable, referenced through ``$`` notation.

The file path may not always be available, depending on how the converter is being invoked.
When invoked through the GeoMesa command-line tools or GeoMesa NiFi, it will be set appropriately.

lineNo
^^^^^^

Description: provides the current line number in the file being operated on, if available

Example: ``lineNo()``

The line number may not always be available, depending on the converter being used. For some converters,
line number may be an abstract concept. For example, in the Avro converter line number will refer to the
number of the Avro record in the file.

Enrichment Functions
~~~~~~~~~~~~~~~~~~~~

cacheLookup
^^^^^^^^^^^

Description: Looks up a value from a cache

Usage: ``cacheLookup(<cacheName>, <entityKey>, <attributeKey>)``

Example: ``cacheLookup('test', $id, 'name')``
