.. _converter_functions:

Transformation Functions
------------------------

Type Conversions
~~~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">toInt</code> or <code class="docutils literal notranslate">toInteger</code></summary>

Converts a value into a integer. If the conversion fails, returns null unless a default value is defined.

============ =========================================================================================
**Function** | ``toInt($value)``
             | ``toInt($value, $default)``
**Usage**    | ``toInt('1', 0) = 1``
             | ``toInt('', 0) = 0``
             | ``toInt('') = null``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">toLong</code></summary>

Converts a value into a long. If the conversion fails, returns null unless a default value is defined.

============ =========================================================================================
**Function** | ``toLong($value)``
             | ``toLong($value, $default)``
**Usage**    | ``toLong('1', 0L) = 1L``
             | ``toLong('', 0L) = 0L``
             | ``toLong('') = null``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">toFloat</code></summary>

Converts a value into a float. If the conversion fails, returns null unless a default value is defined.

============ =========================================================================================
**Function** | ``toFloat($value)``
             | ``toFloat($value, $default)``
**Usage**    | ``toFloat('1.0', 0.0f) = 1.0f``
             | ``toFloat('not a float', 0.0f) = 0.0f``
             | ``toFloat('') = null``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">toDouble</code></summary>

Converts a value into a double. If the conversion fails, returns null unless a default value is defined.

============ =========================================================================================
**Function** | ``toDouble($value)``
             | ``toDouble($value, $default)``
**Usage**    | ``toDouble('1.0', 0.0) = 1.0d``
             | ``toDouble(null, 0.0) = 0.0d``
             | ``toDouble('') = null``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">toBoolean</code></summary>

Converts a value into a Boolean. If the conversion fails, returns null unless a default value is
defined. If the input is a number, zero will evaluate to false, and all other numbers will evaluate to true.

============ =========================================================================================
**Function** | ``toBoolean($value)``
             | ``toBoolean($value, $default)``
**Usage**    | ``toBoolean('true', false) = true``
             | ``toBoolean('foo', false) = false``
             | ``toBoolean(1) = true``
             | ``toBoolean(0) = false``
             | ``toBoolean('') = null``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">::int</code> or <code class="docutils literal notranslate">::integer</code></summary>

Converts a value into an integer. If the conversion fails, it will throw an error and cause the record to fail.

============ =========================================================================================
**Function** | ``$value::int``
**Usage**    | ``'1'::int = 1``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">::long</code></summary>

Converts a value into a long. If the conversion fails, it will throw an error and cause the record to fail.

============ =========================================================================================
**Function** | ``$value::long``
**Usage**    | ``'1'::long = 1L``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">::float</code></summary>

Converts a value into a float. If the conversion fails, it will throw an error and cause the record to fail.

============ =========================================================================================
**Function** | ``$value::float``
**Usage**    | ``'1.0'::float = 1.0f``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">::double</code></summary>

Converts a value into a double. If the conversion fails, it will throw an error and cause the record to fail.

============ =========================================================================================
**Function** | ``$value::double``
**Usage**    | ``'1.0'::double = 1.0d``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">::boolean</code></summary>

Converts a value into a Boolean. If the conversion fails, it will throw an error and cause the record to fail.

============ =========================================================================================
**Function** | ``$value::boolean``
**Usage**    | ``'true'::boolean = true``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">::r</code></summary>

Converts a string into a Regex object, mainly for use with the ``regexReplace`` function.

============ =========================================================================================
**Function** | ``$value::r``
**Usage**    | ``'f.*'::r = f.*: scala.util.matching.Regex``
============ =========================================================================================

.. raw:: html

   </details>

String Functions
~~~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">strip</code></summary>

Removes characters from the start and end of a string. If the characters to strip are not specified, will strip whitespace by default.

============ =========================================================================================
**Function** | ``strip($value)``
             | ``strip($value, $chars)``
**Usage**    | ``strip('afoob', 'abc') = 'foo'``
             | ``strip('foao', 'abc') = 'foao'``
             | ``strip('\t foo ') = 'foo'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">stripPrefix</code></summary>

Removes characters from the start of a string. If the characters to strip are not specified, will strip whitespace by default.

============ =========================================================================================
**Function** | ``stripPrefix($value)``
             | ``stripPrefix($value, $chars)``
**Usage**    | ``stripPrefix('afoob', 'abc') = 'foob'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">stripSuffix</code></summary>

Removes characters from the end of a string. If the characters to strip are not specified, will strip whitespace by default.

============ =========================================================================================
**Function** | ``stripSuffix($value)``
             | ``stripSuffix($value, $chars)``
**Usage**    | ``stripSuffix('afoob', 'abc') = 'afoo'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">stripQuotes</code></summary>

Remove double and single quotes from the start and end of a string.

============ =========================================================================================
**Function** | ``stripQuotes($value)``
**Usage**    | ``stripQuotes('"foo"') = 'foo'``
             | ``stripQuotes('\'foo\'') = 'foo'``
             | ``stripQuotes('fo"o') = 'fo"o'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">replace</code></summary>

Replaces a literal string with another string.

============ =========================================================================================
**Function** | ``replace($value, $toReplace, $replacement)``
**Usage**    | ``replace('foobar', 'ob', 'ab') = 'foabar'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">remove</code></summary>

Removes a substring from a string.

============ =========================================================================================
**Function** | ``remove($value, $substring)``
**Usage**    | ``remove('foabco', 'abc') = 'foo'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">length</code></summary>

Returns the length of a string.

============ =========================================================================================
**Function** | ``length($value)``
**Usage**    | ``length('foo') = 3``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">trim</code></summary>

Trim whitespace from around a string.

============ =========================================================================================
**Function** | ``trim($value)``
**Usage**    | ``trim('  foo ') = 'foo'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">capitalize</code></summary>

Capitalize a string.

============ =========================================================================================
**Function** | ``capitalize($value)``
**Usage**    | ``capitalize('foo') = 'Foo'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">lowercase</code></summary>

Lowercase a string.

============ =========================================================================================
**Function** | ``lowercase($value)``
**Usage**    | ``lowercase('FOO') = 'foo'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">uppercase</code></summary>

Uppercase a string.

============ =========================================================================================
**Function** | ``uppercase($value)``
**Usage**    | ``uppercase('foo') = 'FOO'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">regexReplace</code></summary>

Replace a given pattern with a target pattern in a string. In the replacement String, a dollar sign ``$`` followed by a number
will be interpreted as a reference to a group in the matched pattern, with numbers 1 through 9 corresponding to the first nine
groups, and 0 standing for the whole match. Any other character is an error. The backslash ``\`` character will be interpreted
as an escape character and can be used to escape the dollar sign. 

============ =========================================================================================
**Function** | ``regexReplace($regex, $replacement, $value)``
**Usage**    | ``regexReplace('foo'::r, 'bar', 'foobar') = 'barbar'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">concatenate</code></summary>

Concatenate two or more strings.

============ =========================================================================================
**Function** | ``concatenate($first, $second, ...)``
**Usage**    | ``concatenate('foo', 'bar') = 'foobar'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">mkstring</code></summary>

Concatenate two or more strings with a delimiter between each one.

============ =========================================================================================
**Function** | ``mkstring($delimiter, $first, ...)``
**Usage**    | ``mkstring(',', 'foo', 'bar') = 'foo,bar'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">substring</code></summary>

Extract a substring from a string.

============ =========================================================================================
**Function** | ``substring($value, $startIndex, $endIndex)``
**Usage**    | ``substring('foobarbaz', 2, 5) = 'oba'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">toString</code></summary>

Convert another data type to a string.

============ =========================================================================================
**Function** | ``toString($value)``
**Usage**    | ``concatenate(toString(5), toString(6)) = '56'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">emptyToNull</code></summary>

Replace an empty string with ``null``.

============ =========================================================================================
**Function** | ``emptyToNull($value)``
**Usage**    | ``emptyToNull('') = null``
             | ``emptyToNull('foo') = 'foo'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">printf</code></summary>

Format custom strings, using a Java `Formatter`__.

.. __: https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html

============ =========================================================================================
**Function** | ``printf($pattern, $arg1, $arg2, ...)'``
**Usage**    | ``printf('%s-%s-%sT00:00:00.000Z', '2015', '01', '01') = '2015-01-01T00:00:00.000Z'``
             | ``printf('%2f', divide(-1, 2, 3)) = '-0.17'``
============ =========================================================================================

.. raw:: html

   </details>

Date Functions
~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">now</code></summary>

Returns the current system time, as a Date.

============ =========================================================================================
**Function** | ``now()``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">date</code></summary>

Custom date parser. The date format is defined by the Java `DateTimeFormatter`__ class.

.. __: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html

============ =========================================================================================
**Function** | ``date($format, $value)``
**Usage**    | ``date('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', '2015-01-01T00:00:00.000000')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">dateTime</code></summary>

A strict ISO 8601 Date parser for format ``yyyy-MM-dd'T'HH:mm:ss.SSSZZ``.

============ =========================================================================================
**Function** | ``dateTime($value)``
**Usage**    | ``dateTime('2015-01-01T00:00:00.000Z')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">basicIsoDate</code></summary>

A date format for ``yyyyMMdd``, equivalent to ``java.time.format.DateTimeFormatter.BASIC_ISO_DATE``.

============ =========================================================================================
**Function** | ``basicIsoDate($value)``
**Usage**    | ``basicIsoDate('20150101')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">isoDate</code></summary>

A date format for ``yyyy-MM-dd``, equivalent to ``java.time.format.DateTimeFormatter.ISO_DATE``.

============ =========================================================================================
**Function** | ``isoDate($value)``
**Usage**    | ``isoDate('2015-01-01')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">isoLocalDate</code></summary>

A date format for ``yyyy-MM-dd``, equivalent to ``java.time.format.DateTimeFormatter.ISO_LOCAL_DATE``.

============ =========================================================================================
**Function** | ``isoLocalDate($value)``
**Usage**    | ``isoLocalDate('2015-01-01')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">basicDateTime</code></summary>

A date format that combines a basic date and time for ``yyyyMMdd'T'HHmmss.SSSZ``.

============ =========================================================================================
**Function** | ``basicDateTime($value)``
**Usage**    | ``basicDateTime('20150101T000000.000Z')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">basicDateTimeNoMillis</code></summary>

A basic format that combines a basic date and time for format ``yyyyMMdd'T'HHmmssZ``.

============ =========================================================================================
**Function** | ``basicDateTimeNoMillis($value)``
**Usage**    | ``basicDateTimeNoMillis('20150101T000000Z')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">isoDateTime</code></summary>

A date format for ``yyyy-MM-dd'T'HH:mm:ss``, equivalent to ``java.time.format.DateTimeFormatter.ISO_DATE_TIME``.

============ =========================================================================================
**Function** | ``isoDateTime($value)``
**Usage**    | ``isoDateTime('2015-01-01T00:00:00')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">isoLocalDateTime</code></summary>

A date format for ``yyyy-MM-dd'T'HH:mm:ss``, equivalent to ``java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME``.

============ =========================================================================================
**Function** | ``isoLocalDateTime($value)``
**Usage**    | ``isoLocalDateTime('2015-01-01T00:00:00')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">isoOffsetDateTime</code></summary>

A date format for ``yyyy-MM-dd'T'HH:mm:ssZ``, equivalent to ``java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME``.

============ =========================================================================================
**Function** | ``isoOffsetDateTime($value)``
**Usage**    | ``isoOffsetDateTime('2015-01-01T00:00:00Z')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">dateHourMinuteSecondMillis</code></summary>

Formatter for full date and time, keeping the first 3 fractional seconds for format ``yyyy-MM-dd'T'HH:mm:ss.SSS``.

============ =========================================================================================
**Function** | ``dateHourMinuteSecondMillis($value)``
**Usage**    | ``dateHourMinuteSecondMillis('2015-01-01T00:00:00.000')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">millisToDate</code></summary>

Create a new date from a long representing milliseconds since the Unix epoch (January 1, 1970).

============ =========================================================================================
**Function** | ``millisToDate($value)``
**Usage**    | ``millisToDate(1449675054462L)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">secsToDate</code></summary>

Create a new date from a long representing seconds since the Unix epoch (January 1, 1970).

============ =========================================================================================
**Function** | ``secsToDate($value)``
**Usage**    | ``secsToDate(1449675054)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">dateToString</code></summary>

Formats a date as a string, based on a pattern as defined by the Java
`DateTimeFormatter <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html>`__.

============ =========================================================================================
**Function** | ``dateToString($pattern, $date)``
**Usage**    | ``dateToString('yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSS', now())``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">dateToMillis</code></summary>

Converts a date to milliseconds since the Unix epoch (January 1, 1970).

============ =========================================================================================
**Function** | ``dateToMillis($date)``
**Usage**    | ``dateToMillis(now())``
============ =========================================================================================

.. raw:: html

   </details>

Geometry Functions
~~~~~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">point</code></summary>

Parse a Point geometry from WKT, WKB, or longitude, latitude, z and measure. To create a point with measure but no z,
use ``pointM``.

============ =========================================================================================
**Function** | ``point($lon, $lat)``
             | ``point($lon, $lat, $z)``
             | ``point($lon, $lat, $z, $m)``
             | ``point($wktOrWkb)``
**Usage**    | ``point(0.0, 0.0)``
             | ``point('POINT(2 3)')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">pointM</code></summary>

Parse a Point geometry from longitude, latitude and measure.

============ =========================================================================================
**Function** | ``pointM($lon, $lat, $m)``
**Usage**    | ``pointM(10.0, 20.0, 30.0)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">multipoint</code></summary>

Parse a multi-point from a WKT string, WKB byte array, or two lists of coordinates.

============ =========================================================================================
**Function** | ``multipoint($wktOrWkb)``
             | ``multipoint($longitudes, $latitudes)``
**Usage**    | ``multipoint('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')``
             | ``multipoint(list(10,40,20,30),list(40,30,20,10))``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">linestring</code></summary>

Parse a linestring from a WKT string, WKB byte array, or two lists of coordinates.

============ =========================================================================================
**Function** | ``linestring($wktOrWkb)``
             | ``linestring($longitudes, $latitudes)``
**Usage**    | ``linestring('LINESTRING(102 0, 103 1, 104 0, 105 1)')``
             | ``linestring(list(102,103,104,105),list(0,1,0,1))``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">multilinestring</code></summary>

Parse a multi-linestring from a WKT string or WKB byte array.

============ =========================================================================================
**Function** | ``multilinestring($wktOrWkb)``
**Usage**    | ``multilinestring('MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">polygon</code></summary>

Parse a polygon from a WKT string or WKB byte array.

============ =========================================================================================
**Function** | ``polygon($wktOrWkb)``
**Usage**    | ``polygon('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">multipolygon</code></summary>

Parse a multi-polygon from a WKT string or WKB byte array.

============ =========================================================================================
**Function** | ``multipolygon($wktOrWkb)``
**Usage**    | ``multipolygon('MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">geometrycollection</code></summary>

Parse a geometry collection from a WKT string or WKB byte array.

============ =========================================================================================
**Function** | ``geometrycollection($wktOrWkb)``
**Usage**    | ``geometrycollection('GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">geometry</code></summary>

Parse a geometry from a WKT string or WKB byte array. Note that you would generally only use this method if you
are dealing with multiple geometry types, otherwise prefer a more specific method (e.g. ``point``, ``linestring``, etc).

============ =========================================================================================
**Function** | ``geometry($wktOrWkb)``
**Usage**    | ``geometry('POINT(2 3)')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">projectFrom</code></summary>

Project a geometry from its native CRS to ``EPSG:4326``. GeoMesa only supports ``EPSG:4326``, so geometries must be transformed
when ingesting from another CRS.

============ =========================================================================================
**Function** | ``projectFrom($epsg, $geom)``
**Usage**    | ``projectFrom('EPSG:3857', point(1689200.14,1113194.91))``
============ =========================================================================================

.. raw:: html

   </details>

ID Functions
~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">stringToBytes</code></summary>

Converts a string to a UTF-8 byte array (to pass to other functions like ``md5()``).

============ =========================================================================================
**Function** | ``stringToBytes($value)``
**Usage**    | ``stringToBytes('row,of,data')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">md5</code></summary>

Creates an MD5 hash from a byte array. Note that MD5 is not as secure or as performant as other hashes, and should
generally be avoided.

============ =========================================================================================
**Function** | ``md5($value)``
**Usage**    | ``md5(stringToBytes('row,of,data'))``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">murmur3_32</code></summary>

Creates a 32-bit murmur3 hash from a string.

============ =========================================================================================
**Function** | ``murmur3_32($value)``
**Usage**    | ``murmur3_32('row,of,data')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">murmur3_64</code></summary>

Creates a 64-bit murmur3 hash from a string. Note that previously this function was incorrectly
named ``murmur3_128``, and can still be invoked by that name.

============ =========================================================================================
**Function** | ``murmur3_64($value)``
**Usage**    | ``murmur3_64('row,of,data')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">murmurHash3</code></summary>

Creates a 128-bit murmur3 hash from a string or byte array, returned as a hex string.

============ =========================================================================================
**Function** | ``murmurHash3($value)``
**Usage**    | ``murmurHash3('row,of,data')``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">uuid</code></summary>

Generates a random UUID, returned as a string.

============ =========================================================================================
**Function** | ``uuid()``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">uuidZ3</code></summary>

Generates a Z3-based UUID for point geometries.

============ =========================================================================================
**Function** | ``uuidZ3($geom, $date, $interval)``
**Usage**    | ``uuidZ3(point('POINT (3 2)'), dateTime('2015-01-01T00:00:00.000Z'), 'week')``
============ =========================================================================================

See :ref:`customizing_z_index` for details on Z3 intervals.

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">uuidZ3Centroid</code></summary>

Generates a Z3-based UUID for non-point geometries.

============ =======================================================================================================================
**Function** | ``uuidZ3Centroid($geom, $date, $interval)``
**Usage**    | ``uuidZ3Centroid(linestring('LINESTRING(102 0, 103 1, 104 0, 105 1)', dateTime('2015-01-01T00:00:00.000Z'), 'week')``
============ =======================================================================================================================

See :ref:`customizing_z_index` for details on Z3 intervals.

.. raw:: html

   </details>

Math Functions
~~~~~~~~~~~~~~

The arguments to a math functions must be numbers - integers, doubles, floats, longs or numeric strings. All math functions
return doubles. If another data type is needed, convert the value afterwards, e.g. ``add($first,$second)::long``.

Most math functions accept multiple arguments, or will accept a single ``java.util.List`` containing the arguments.

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">add</code></summary>

Adds two or more values.

============ =========================================================================================
**Function** | ``add($value1, $value2, ...)``
             | ``add(list($value1, $value2))``
**Usage**    | ``add(1, 2) = 3.0``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">subtract</code></summary>

Subtracts two or more values sequentially.

============ =========================================================================================
**Function** | ``subtract($value1, $value2, ...)``
             | ``subtract(list($value1, $value2))``
**Usage**    | ``subtract(3,2) = 1.0``
             | ``subtract(3,2,1) = 0.0``
             | ``subtract(list(3,1)) = 2.0``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">multiply</code></summary>

Multiply two or more values.

============ =========================================================================================
**Function** | ``multiply($value1, $value2, ...)``
             | ``multiply(list($value1, $value2))``
**Usage**    | ``multiply(2,2,2) = 8.0``
             | ``multiply(list(3,2)) = 6.0``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">divide</code></summary>

Divides two or more values sequentially.

============ =========================================================================================
**Function** | ``divide($value1, $value2, ...)``
             | ``divide(list($value1, $value2))``
**Usage**    | ``divide(24,2,3) = 4.0``
             | ``divide(list(4,2)) = 2.0``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">modulo</code></summary>

Takes the modulo of two values. Note that unlike other math functions, modulo expects and returns integers instead
of doubles.

============ =========================================================================================
**Function** | ``modulo($value1, $value2)``
             | ``modulo(list($value1, $value2))``
**Usage**    | ``modulo(5,2) = 1``
             | ``modulo(list(7,5)) = 2``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">mean</code></summary>

Takes the mean (average) of two or more numbers.

============ =========================================================================================
**Function** | ``mean($value1, $value2, ...)``
             | ``mean(list($value1, $value2)``
**Usage**    | ``mean(10,9,8) = 9.0``
             | ``mean(list(10,9,8)) = 9.0``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">min</code></summary>

Finds the minimum of two or more numbers. The numbers must all be of the same type, and the return value will
be of the same type.

============ =========================================================================================
**Function** | ``min($value1, $value2, ...)``
             | ``min(list($value1, $value2)``
**Usage**    | ``min(10,9,8) = 8``
             | ``min(list(1,2,3)) = 1``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">max</code></summary>

Finds the maximum of two or more numbers. The numbers must all be of the same type, and the return value will
be of the same type.

============ =========================================================================================
**Function** | ``max($value1, $value2, ...)``
             | ``max(list($value1, $value2)``
**Usage**    | ``max(10,9,8) = 10``
             | ``max(list(1,2,3)) = 3``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">sin</code></summary>

Calculates the sine of a number.

============ =========================================================================================
**Function** | ``sin($value)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">cos</code></summary>

Calculates the cosine of a number.

============ =========================================================================================
**Function** | ``cos($value)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">tan</code></summary>

Calculates the tangent of a number.

============ =========================================================================================
**Function** | ``tan($value)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">asin</code></summary>

Calculates the arc sine of a number.

============ =========================================================================================
**Function** | ``asin($value)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">acos</code></summary>

Calculates the arc cosine of a number.

============ =========================================================================================
**Function** | ``acos($value)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">atan</code></summary>

Calculates the arc tangent of a number.

============ =========================================================================================
**Function** | ``atan($value)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">ln</code></summary>

Calculates the natural logarithm of a number.

============ =========================================================================================
**Function** | ``ln($value)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">exp</code></summary>

Calculates Euler's number *e* raised to the power of a number.

============ =========================================================================================
**Function** | ``exp($value)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">sqrt</code></summary>

Calculates the square root of a number.

============ =========================================================================================
**Function** | ``sqrt($value)``
============ =========================================================================================

.. raw:: html

   </details>

.. _converter_list_map_fns:

List and Map Functions
~~~~~~~~~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">list</code></summary>

Creates a list from the input arguments.

============ =========================================================================================
**Function** | ``list($value1, $value2, ...)``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">listItem</code></summary>

Selects an element out of a list by index.

============ =========================================================================================
**Function** | ``listItem($list, $index)``
**Usage**    | ``listItem(list('1', '2', '3'), 0) = '1'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">mapValue</code></summary>

Read a value out of a map by key.

============ =========================================================================================
**Function** | ``mapValue($map, $key)``
**Usage**    | ``mapValue(parseMap('string->int', 'a->1,b->2,c->3'), 'a') = 1``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">parseList</code></summary>

Parse a list from a string. Supported list item types are ``string``, ``int``, ``long``, ``float``, ``double``, ``boolean``
``bytes`` (for ``byte[]``), ``uuid`` and ``date``. If the delimiter is not specified, it defaults to a comma ``,``.

============ =========================================================================================
**Function** | ``parseList($itemType, $list)``
             | ``parseList($itemType, $list, $delimiter)``
**Usage**    | ``parseList('int', '1,2,3') = [ 1, 2, 3 ]``
             | ``parseList('int', '1:2:3', ':') = [ 1, 2, 3 ]``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">parseMap</code></summary>

Parse a map from a string. The key and value types must be specified as a string, separated by ``->``.
Supported types are ``string``, ``int``, ``long``, ``float``, ``double``, ``boolean`` ``bytes`` (for ``byte[]``), ``uuid``
and ``date``. Optionally, a record and key-value delimiter can be specified. If not specified, the record delimiter defaults to
a comma ``,``, and the key-value delimiter defaults to an "arrow" ``->``.

============ =========================================================================================
**Function** | ``parseMap($keyValueTypes, $map)``
             | ``parseMap($keyValueTypes, $map, $keyValueDelimiter)``
             | ``parseMap($keyValueTypes, $map, $keyValueDelimiter, $recordDelimiter)``
**Usage**    | ``parseMap('string->int', 'a->1,b->2,c->3') = { a=1, b=2, c=3 } ]``
             | ``parseMap('string->int', 'a->1,b->2,c->3', '->') = { a=1, b=2, c=3 }``
             | ``parseMap('string->int', 'a->1,b->2,c->3', '->', ',') = { a=1, b=2, c=3 }``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">transformListItems</code></summary>

Applies a transform expression to every element of a list. The expression to apply must be defined as a string. The list
item is available in the transform expression as ``$0``. In the example shown, the list will be converted from ``List<String>``
to ``List<Double>``.

============ =========================================================================================
**Function** | ``transformListItems($list, $expression)``
**Usage**    | ``transformListItems(list('1','2','3'), 'stringToDouble($0)') = [ 1.0, 2.0, 3.0 ]``
============ =========================================================================================

.. raw:: html

   </details>

Encoding Functions
~~~~~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">base64Encode</code></summary>

Encodes a byte array as a base-64 URL-safe string.

============ =========================================================================================
**Function** | ``base64Encode($value)``
**Usage**    | ``base64Encode(stringToBytes('foo'))``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">base64Decode</code></summary>

Decodes a base-64 URL-safe encoded string into a byte array.

============ =========================================================================================
**Function** | ``base64Decode($value)``
**Usage**    | ``base64Decode('Zm9v')``
============ =========================================================================================

.. raw:: html

   </details>

Control Functions
~~~~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">try</code></summary>

Execute another expression - if it throws an exception, return a default value. Note that throwing exceptions in Java is
relatively expensive, so ``try`` expressions that fail frequently should be avoided where possible. For instance,
``try($0::int, null)`` could be replaced with ``toInt($0)``, which is equivalent but will handle null values without throwing
exceptions.

============ =========================================================================================
**Function** | ``try($value, $default)``
**Usage**    | ``try("1"::int, 0) = 1``
             | ``try("abcd"::int, null) = null``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">withDefault</code></summary>

Replace a value with the first non-null alternative, if the value is null.

============ =========================================================================================
**Function** | ``withDefault($value, $default1, ...)``
**Usage**    | ``withDefault('foo', 'bar') = 'foo'``
             | ``withDefault('foo', 'bar', 'baz') = 'foo'``
             | ``withDefault(null, 'bar', 'baz') = 'bar'``
             | ``withDefault(null, null, 'baz') = 'baz'``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">require</code></summary>

Throw an exception if the value is null (which will fail the current record), otherwise return the value.

============ =========================================================================================
**Function** | ``require($value)``
**Usage**    | ``require('foo') = 'foo'``
             | ``require(null) // fails the current record``
============ =========================================================================================

.. raw:: html

   </details>

State Functions
~~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">inputFilePath</code></summary>

Provides the absolute path to the file being operated on, if available. Note that the file path is a variable, not a function,
and is referenced through ``$`` notation.

The file path may not always be available, depending on how the converter is being invoked.
When invoked through the GeoMesa command-line tools or GeoMesa NiFi, it will be set appropriately.

============ =========================================================================================
**Usage**    | ``$inputFilePath``
============ =========================================================================================

.. raw:: html

    </details>
    <details>
      <summary><code class="docutils literal notranslate">lineNumber</code></summary>

Provides the current line number in the file being operated on, if available.

The line number may not always be available, depending on the converter being used. For some converters,
line number may be an abstract concept. For example, in the Avro converter line number will refer to the
number of the Avro record in the file.

============ =========================================================================================
**Function** | ``lineNumber()``
============ =========================================================================================

.. raw:: html

   </details>

Enrichment Functions
~~~~~~~~~~~~~~~~~~~~

.. raw:: html

    <details>
      <summary><code class="docutils literal notranslate">cacheLookup</code></summary>

The converter framework provides a mechanism for setting an attribute based on a lookup
from a cache. See :ref:`converter_caches` for details.

============ =========================================================================================
**Function** | ``cacheLookup($cacheName, $entityKey, $attributeKey)``
**Usage**    | ``cacheLookup('test', $userId, 'email')``
============ =========================================================================================

.. raw:: html

   </details>

CQL Functions
~~~~~~~~~~~~~

Most of the basic CQL functions are available as transform functions. To use one, invoke it like a regular function, prefixed
with the ``cql`` namespace. For example, you can use the CQL ``buffer`` function to turn a point into a polygon:

::

    cql:buffer($value, 2.0)

For more information on the various CQL functions, see the GeoServer
`filter function reference <https://docs.geoserver.org/stable/en/user/filter/function_reference.html#filter-function-reference>`__.

JSON/Avro Transformations
~~~~~~~~~~~~~~~~~~~~~~~~~

See JSON :ref:`json_converter_functions` and Avro :ref:`avro_converter_functions`.
