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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42f971fb10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 22c3a3a32c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b04d18f291 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 496b655fad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6bc7d58545 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3878f6f5de (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 657d996e28 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 64c4b371aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5e643ffda0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 46b82ddad2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b1b3a01203 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2adb521281 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5aa4d33627 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e820cbfbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 67238b4a03 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d6e3d58599 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6bc7d58545 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
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
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 22c3a3a32c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b04d18f291 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a6dd271d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e81892dbf2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3b5854c1bb (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 22c3a3a32c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cc47bbe241 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 496b655fad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6bc7d58545 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3878f6f5de (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 657d996e28 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 64c4b371aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5e643ffda0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 46b82ddad2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b1b3a01203 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2adb521281 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5aa4d33627 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2e820cbfbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 67238b4a03 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d6e3d58599 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b04d18f291 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42f971fb10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b5854c1bb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 22c3a3a32c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cc47bbe241 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 496b655fad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6bc7d58545 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3878f6f5de (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 657d996e28 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 64c4b371aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5e643ffda0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 46b82ddad2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b1b3a01203 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2adb521281 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5aa4d33627 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2e820cbfbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 67238b4a03 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d6e3d58599 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b04d18f291 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3b5854c1bb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 42f971fb10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 496b655fad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42f971fb10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 22c3a3a32c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b04d18f291 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
=======
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a83aceac86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a6dd271d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6ba18529e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e81892dbf2 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a53c4ae77a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 33d667e156 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c1d8349faf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 289064e02e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ce6cc995b6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 433f345a62 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> da2989bc34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6be5ad9465 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b5854c1bb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 914d29005e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
=======
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
<<<<<<< HEAD
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
<<<<<<< HEAD
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6436af5398 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5d6a8a22c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ff17a571bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a539011150 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ef927e4467 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ca414dc405 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9536729782 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fc35c48b18 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 339dee0a22 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f0b45e2750 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2ef7cb6fbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 89d141ae1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 542333b7e1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c678213e2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a1abb55ae6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 87fa76b076 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3fd5d67155 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 73e8d8dfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e6748a57a8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5a4980301e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0a52327e4b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 82d1dee8a6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4da62c580e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7edcee4732 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 22c3a3a32c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6b6227eac7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 33b6ffa80c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ff0dbfcfb7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8c4c7031fa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a8f608dae5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7a05bea24e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a3c59101dc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3b121e7307 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> bbd5ce4642 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 84f843971a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8685eb198b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7bd74568fd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9738e164f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> acd8285f80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aef4d2c269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee93d9232 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 30dfb4b4a4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffc41999a5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 69b2505dd2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1b372c3bfe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 443baa8c16 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a61b82cd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ecc32e2269 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 752149ca0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 72c5e08131 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cc7ee42dd3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 74fb0ce7bc (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 971e1c705f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9ea8eba6ad (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 866ecc6b5b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3dc5840cae (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3d62f0b5cd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> bd467652e9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 61fdc5a77a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de8cb22987 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f65e709243 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25868d024e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c7fcdd27b8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 589fedeabc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff631444bd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dc9e876637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d2b5f33d64 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e13e0d0d45 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e9a5809b17 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> df7438e650 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 27353f52e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9fd68c1b3c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c52bac169a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a71ff4b55b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 660db9defa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6adfbb9118 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d55e1bd3e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 79bff6c04a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 215d6881a1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7287947b9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a1b6da2609 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 95cf8886d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 58e7e95780 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7048ab588f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 07b3929933 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 024df41e7b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f5f0946155 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7e4447739b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 05331f7735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 3df35a8543 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a65a6b5ef9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> cd83d09ace (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 195d1e0018 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 78e82abbaf (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 58c1ae7526 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 748125fdcb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cc47bbe241 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 496b655fad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6bc7d58545 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3878f6f5de (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 657d996e28 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9c9762b66f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
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
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 64c4b371aa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5e643ffda0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 46b82ddad2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83f8dc29e3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a0858 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 390b81fff3 (GEOMESA-3109 Json array to object converter function (#2788))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d7283bb277 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f2c2107332 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8139d78b86 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a9b70a2c09 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b1b3a01203 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2adb521281 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f426184ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 561d58ab21 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bdad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c79a342576 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9f463efab2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24aa (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4ea7c5729e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3939f9d867 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dbc712b84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c72fc402f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ffd6869539 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89971e000d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c08df1110e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> cd0fe2ac6a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 746e3515d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 64d15a47bc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7d531a0173 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7ea7e59e0f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c26ba6a25 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 036676db27 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8ce9867514 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5aa4d33627 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194ec (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2e820cbfbd (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
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
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 67238b4a03 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d6e3d58599 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea96678625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> fbe314c698 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b059fd9ac1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebfd8ecd13 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bc481474c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d5f1bdf64f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 18993b52f2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 02663bbaaa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1bc88f7e23 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a92e10819 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ac90a45888 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a38d1a4cc5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1187c0eb37 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee6153cc5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a3e75bae21 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f586618a0c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6c121d3345 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d86a5b91dd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 715ffa7d44 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c0dc422e29 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9058baa844 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8170c2c54a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e89f27bd29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 6938112d54 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e66880e12e (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 0ee3ebb42e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbdc (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 42312eee8b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f7736a7e8c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09c8a6d2fd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6030d25073 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 16f912e1ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 04cf999171 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fd776cb831 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b04d18f291 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a3acfc6a17 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a01edd94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 62ff7eb02c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 13c9b592f1 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 674c8f43c2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6312e4ce37 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e7b61a536 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448369e575 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a79102d96a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0df4f16d9 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a76cfdacc5 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 54adfea96b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ef1041e32f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 56c3be0165 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4350edc8f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 793ec81151 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4bff038c35 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 03967b3f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 5bd54939f4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> faeaf8af53 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> 008feb67a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7982d54d93 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c676e95b0d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> c399a7eef (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4be1359e88 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d7d2a86571 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a9e41bce3a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 637206287a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4bd9eb4df (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8c33ac76a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cc3633448a (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
>>>>>>> 04d469083d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 38f95b1602 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b3bdbc9f8d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fd675cc6b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 666589fa84 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 0d4c68bda (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 919559e486 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 36a5acc573 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff07fa8368 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2cb3139392 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 31e2c2bccd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 94213b24a (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2db4ecdc72 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7558f8f4ee (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 17364899f4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 287eacef52 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 898a36edd8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> dbc712b84 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ddadfbdc64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b32f6803c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b6bf13447c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b7edeb6e6f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01f7d00b29 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 89971e000 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e634f5d579 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ff50279c43 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> eb5809d8c8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ad66dcb097 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bd5fa75254 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 4a51d3f07 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1143da1625 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a20e68fe8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 8d88859920 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 552f2ccfc3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8fec8a308 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 354c37f8b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 827f49a0be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b06af647f3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> bd75e62853 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fa3c3c3660 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2d0564a3c5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> e08000604 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 981685f1ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 350ca1e784 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 6600acc177 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 447efc836d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> de823db072 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 7a0ab99f8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 541f1862a7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 32c76144ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b4a7847d55 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99b335bafe (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 78c1eaf70e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9c337194e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b196d7bf96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c630afc60d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d89aec9bc9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b4c47024ad (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> afc15a9b81 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35b3ecb03 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7b395bc2b7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e49f1355d3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 71968f03a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> ea9667862 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7705eeb678 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3f8e82853d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a27a0a3dce (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e18131667 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a01809bcb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 063b0f26f (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6d26127ad6 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> be1369a16b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b4095c94f5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 7dd4802f6b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d69cb74390 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d5f1bdf64 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> c17c73531f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 289ca829c7 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0e2f008100 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a8d5a2eb59 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3070a314b6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 1bc88f7e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 941c4a6320 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d4d9fdd899 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 77526fe4e8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e4bed11ff8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ffe5c1383 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a38d1a4cc (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e9b36da337 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> b361489158 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 65bd42412d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 484303271a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4af6a55818 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> f586618a0 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a3e5500db0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> ebc30c95c3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> f7bd7d634d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 22ea169778 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 098d75a926 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c0dc422e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1ec3ea887f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cb62a334b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 448a77c6df (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d82e184b3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 01048d5767 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 6938112d5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1b600f3f7a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e2c230128a (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> dd1101096c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> a83aceac8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> bc5cfce599 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c48823a84a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> afa2a39911 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 9759ddc1b (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd8248bbd (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e29e638726 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> d1610750ff (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e8f0204320 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> ae5490a34 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4b08d07fef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8ca5e5c22b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 12b0b267d0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> 09c8a6d2f (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0686300b33 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 549cf6291c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 598985d172 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e43a47856c (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 61771d8a02 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b761d87009 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ab63529bf7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 30bb31e5b4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 33b2e8d04f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6c19e85ebd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4230165b78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 01daab72fe (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> f720187d06 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> a457b33249 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1f53fa56d9 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de732e8693 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7b73f9aa72 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2570d741eb (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> de603ff275 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 585cc74744 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 21046aa8a7 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 1938bb0efa (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 33d658d2dc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 553cbb229b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 177a1a08ef (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 63504e7a16 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7fbf94f0d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2ae044ddeb (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8961e92636 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fb2218c2fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 235691f96 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> e84ffc66b1 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 29bbc51e0b (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3b5854c1bb (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 42f971fb10 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> e6ede6713f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 4aef7a70f3 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 09373546be (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 559080d1e2 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f4571aac9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bc40aa4ec6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 64d8177ac0 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 7f9b4c88ad (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 0baf4b023b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0af33260a0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bad7dfea60 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 883af44eef (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 3cd1ccfe81 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9ab62f6a78 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 50a40020d1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 31b03236c6 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 940738d77d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9be384784c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4de07d7256 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e786a22f3f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e0e5de6398 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 354eaf55ec (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 778d4a3956 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 633b0ed7fb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 2c3111e686 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e459bea71c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> cd47019c5e (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bb7e8ccf4a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 59d379d9d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a670f84c (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c44517c361 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> aa65d3827d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 752571c67b (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f3feee348 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a00c7a6183 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1e76dbd1e (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9759ddc1b5 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> fcc0d635d3 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 292f229afd (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 88550fc09f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 96a1f60a2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9e7594a07 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 58ba72fde8 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6d1dac8a25 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 098dce9ec6 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> b00320b6d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a6c08b6757 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b720884250 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 35e6dce247 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 4c32324d82 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 79b819b47b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1d11096373 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 67972263e5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 177a4155cf (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a4eae87029 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 04e3f9e072 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> c4dcfeeea5 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 93ad8dd735 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f62cea0a1b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 31b03236c (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a51d3f071 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> e57217a17d (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 1d7ecceffa (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8f9133d355 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 430234ea95 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 4aef7a70f (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 097b5a085 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 354c37f8b4 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 35d929bd27 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 75586efd87 (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> adc6646ece (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 672f323955 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> 64d8177ac (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 2c3111e68 (GEOMESA-3109 Json array to object converter function (#2788))
=======
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e080006042 (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> a81060c48c (GEOMESA-3109 Json array to object converter function (#2788))
<<<<<<< HEAD
>>>>>>> 948abfdd1d (GEOMESA-3109 Json array to object converter function (#2788))
=======
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8a671ae630 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> fee659b45b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> c44517c36 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 7a0ab99f8b (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 8cda894ca7 (GEOMESA-3109 Json array to object converter function (#2788))
>>>>>>> 08f7b3b1d8 (GEOMESA-3109 Json array to object converter function (#2788))
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
