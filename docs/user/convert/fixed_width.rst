.. _fixed_width_text_converter:

Fixed-Width Text Converter
==========================

The fixed-width text converter handles text files that follow a fixed format.

Configuration
-------------

The fixed-width converter supports the following configuration keys:

=============== ======== ======= ==========================================================================================
Key             Required Type    Description
=============== ======== ======= ==========================================================================================
``type``        yes      String  Must be the string ``fixed-width``.
=============== ======== ======= ==========================================================================================

Field Configuration
-------------------

The ``fields`` element in a fixed-width converter supports additional keys:

============= ==========================================================================================================
Key           Description
============= ==========================================================================================================
``start``     The starting offset of a field.
``width``     The width/length of a field.
============= ==========================================================================================================

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

The following configuration defines an appropriate converter for transforming this into our ``SimpleFeatureType``:

::

  geomesa.converters.example = {
    type = "fixed-width",
    id-field = "uuid()",
    fields = [
      { name = "lat",  start = 1, width = 2, transform = "$0::double" },
      { name = "lon",  start = 3, width = 2, transform = "$0::double" },
      { name = "geom", transform = "point($lon, $lat)" }
    ]
  }

