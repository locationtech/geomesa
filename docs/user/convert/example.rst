.. _convert_example_usage:

Example Usage
-------------

Suppose you have a ``SimpleFeatureType`` with the following schema:
``phrase:String,dtg:Date,geom:Point:srid=4326`` and comma-separated data
as shown below.

::

    first,hello,2015-01-01T00:00:00.000Z,45.0,45.0
    second,world,2015-01-01T00:00:00.000Z,45.0,45.0

The first two fields should be concatenated together to form the phrase,
the third field should be parsed as a date, and the last two fields
should be formed into a ``Point`` geometry. The following Typesafe Config
string defines an appropriate converter for taking this CSV data and
transforming it into our ``SimpleFeatureType``.

::

     {
      type         = "delimited-text",
      format       = "CSV",
      id-field     = "md5($0)",
      user-data    = {
        // note: keys will be treated as strings and should not be quoted
        my.user.key = "$phrase"
      }
      fields = [
        { name = "phrase", transform = "concatenate($1, $2)" },
        { name = "lat",    transform = "$4::double" },
        { name = "lon",    transform = "$5::double" },
        { name = "dtg",    transform = "dateHourMinuteSecondMillis($3)" },
        { name = "geom",   transform = "point($lon, $lat)" }
      ]
     }

The ``id`` of the ``SimpleFeature`` is formed from an md5 hash of the
entire record (``$0`` is the original data). The simple feature attributes
are created from the ``fields`` list with appropriate transforms (note the
use of intermediate fields 'lat' and 'lon'). If desired, user data for the
feature can be set by referencing fields. This can be used for setting
Accumulo visibility constraints, among other things (see :ref:`accumulo_visibilities`).
