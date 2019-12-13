.. _fixed_width_text_converter:

Fixed-Width Text Converter
==========================

The fixed-width text converter handles text files that follow a fixed format. To use the fixed-width
converter, specify ``type = "fixed-width"`` in your converter definition.

Configuration
-------------

``fields`` in a fixed-width converter support two additional attributes, ``start`` and ``width``. These
define the offset and length of each field in relation to the entire record.

Transform Functions
-------------------

The ``transform`` element supports referencing the fixed-width substring through ``$0``. Each column will initially
be a string, so further transforms may be necessary to create the correct type. See :ref:`converter_functions` for
available functions.

Example Usage
-------------

Suppose you have a ``SimpleFeatureType`` that consists of a sole geometry: ``*geom:Point:srid=4326``. Your
input data is a fixed-width file where the latitude and longitude are defined as 2 digit numbers, following
a single digit prefix:

::

    14555
    16565

The following Typesafe Config string defines an appropriate converter for taking this data and
transforming it into our ``SimpleFeatureType``:

::

  geomesa.converters.example = {
    type     = "fixed-width",
    id-field = "uuid()",
    options = {
      validators = []
    }
    fields = [
      { name = "lat",  start = 1, width = 2, transform = "$0::double" },
      { name = "lon",  start = 3, width = 2, transform = "$0::double" },
      { name = "geom", transform = "point($lon, $lat)" }
    ]
  }

