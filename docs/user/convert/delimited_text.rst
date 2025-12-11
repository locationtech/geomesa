.. _delimited_text_converter:

Delimited Text Converter
========================

The delimited text converter handles plain delimited text files such as CSV or TSV.

Configuration
-------------

The delimited text converter supports the following configuration keys:

====================== ======== ======= =========================================================================================
Key                    Required Type    Description
====================== ======== ======= =========================================================================================
``type``               yes      String  Must be the string ``delimited-text``.
``format``             yes      String  The delimited text format (see below).
``options.delimiter``  no       Char    Override the delimiter character.
``options.quote``      no       Char    Override the quote character. Can be disabled by setting to an empty string.
``options.escape``     no       Char    Override the escape character. Can be disabled by setting to an empty string.
``options.skip-lines`` no       Integer Skip over header lines
====================== ======== ======= =========================================================================================

``format``
^^^^^^^^^^

The ``format`` key specifies an instance of ``org.apache.commons.csv.CSVFormat`` that will be used for parsing. The available
formats are:

============================ =================================================================================================
Name                         Format
============================ =================================================================================================
``DEFAULT`` or ``CSV``       ``CSVFormat.DEFAULT``
``TDF`` or ``TSV``           ``CSVFormat.TDF``
``QUOTED``                   ``CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)``
``QUOTE_ESCAPE``             ``CSVFormat.DEFAULT.withEscape('"')``
``QUOTED_WITH_QUOTE_ESCAPE`` ``CSVFormat.DEFAULT.withEscape('"').withQuoteMode(QuoteMode.ALL)``
``EXCEL``                    ``CSVFormat.EXCEL``
``MYSQL``                    ``CSVFormat.MYSQL``
``RFC4180``                  ``CSVFormat.RFC4180``
============================ =================================================================================================

See `Apache Commons CSV <https://commons.apache.org/proper/commons-csv/user-guide.html>`__ for additional details on each format.

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

    number,word,date,lat,lon
    first,hello,2015-01-01T00:00:00.000Z,45.0,45.0
    second,world,2015-01-01T00:00:00.000Z,45.0,45.0

We want to concatenate the first two fields together to form the phrase, parse the third field as a date, and
use the last two fields as coordinates for a ``Point`` geometry. The following configuration defines an appropriate
converter for taking this CSV data and transforming it into our ``SimpleFeatureType``:

::

  geomesa.converters.example = {
    type = "delimited-text"
    format = "CSV"
    options = {
      skip-lines = 1
    }
    id-field = "murmurHash3($0)"
    fields = [
      { name = "phrase", transform = "concatenate($1, $2)" },
      { name = "dtg",    transform = "dateHourMinuteSecondMillis($3)" },
      { name = "lat",    transform = "$4::double" },
      { name = "lon",    transform = "$5::double" },
      { name = "geom",   transform = "point($lon, $lat)" }
    ]
  }

The ``id`` of the ``SimpleFeature`` is formed from a hash of the entire record (``$0`` is the whole row). The simple feature
attributes are created from the ``fields`` list with appropriate transforms - note the use of intermediate fields 'lat' and 'lon'.
