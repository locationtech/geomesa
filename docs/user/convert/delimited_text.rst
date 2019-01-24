.. _delimited_text_converter:

Delimited Text Converter
========================

The delimited text converter handles plain delimited text files such as CSV or TSV. To use the delimited text
converter, specify ``type = "delimited-text"`` in your converter definition.

Configuration
-------------

The format of the delimited files must be defined using the ``format`` element. GeoMesa uses
`Apache Commons CSV <https://commons.apache.org/proper/commons-csv/user-guide.html>`__ for parsing. The available
formats are instances of ``org.apache.commons.csv.CSVFormat``:

* **DEFAULT** or **CSV**: ``CSVFormat.DEFAULT``
* **TDF** or **TSV**: ``CSVFormat.TDF``
* **QUOTED**: ``CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)``
* **QUOTE_ESCAPE**: ``CSVFormat.DEFAULT.withEscape('"')``
* **QUOTED_WITH_QUOTE_ESCAPE**: ``CSVFormat.DEFAULT.withEscape('"').withQuoteMode(QuoteMode.ALL)``
* **EXCEL**: ``CSVFormat.EXCEL``
* **MYSQL**: ``CSVFormat.MYSQL``
* **RFC4180**: ``CSVFormat.RFC4180``

In addition, GeoMesa supports custom quote, escape and delimiter characters, which can be used to modify the
base format. These can be specified through ``options.quote``, ``options.escape`` and ``options.delimiter``.
Quotes and escapes can be disabled by setting the option to an empty string.

If the input files have header lines, they can be skipped over by specifying a number of lines to skip
using ``options.skip-lines``, e.g. ``options.skip-lines = 1``.

Transform Functions
-------------------

The ``transform`` element supports referencing each field in the record by its column number using ``$``. ``$0``
refers to the whole line, then the first columns is ``$1``, etc. Each column will initially be a string, so
further transforms may be necessary to create the correct type. See :ref:`converter_functions` for more details.

.. _convert_example_usage:

Example Usage
-------------

Suppose you have a ``SimpleFeatureType`` with the following schema:

``phrase:String,dtg:Date,*geom:Point:srid=4326``

And you have the following comma-separated data:

::

    first,hello,2015-01-01T00:00:00.000Z,45.0,45.0
    second,world,2015-01-01T00:00:00.000Z,45.0,45.0

We want to concatenate the first two fields together to form the phrase, parse the third field as a date, and
use the last two fields as coordinates for a ``Point`` geometry. The following configuration defines an appropriate
converter for taking this CSV data and transforming it into our ``SimpleFeatureType``:

::

  geomesa.converters.example = {
    type     = "delimited-text",
    format   = "CSV",
    id-field = "md5(stringToBytes($0))",
    fields = [
      { name = "phrase", transform = "concatenate($1, $2)" },
      { name = "dtg",    transform = "dateHourMinuteSecondMillis($3)" },
      { name = "lat",    transform = "$4::double" },
      { name = "lon",    transform = "$5::double" },
      { name = "geom",   transform = "point($lon, $lat)" }
    ]
    user-data = {
      // note: keys will be treated as strings and should not be quoted
      my.user.key = "$phrase"
    }
  }

The ``id`` of the ``SimpleFeature`` is formed from an MD5 hash of the
entire record (``$0`` is the original data). The simple feature attributes
are created from the ``fields`` list with appropriate transforms (note the
use of intermediate fields 'lat' and 'lon'). If desired, user data for the
feature can be set by referencing fields. This can be used for setting
Accumulo visibility constraints, among other things (see :ref:`accumulo_visibilities`).
