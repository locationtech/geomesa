.. _prepackaged_converters:

Prepackaged Converters
======================

The GeoMesa CLI distributions contain several prepackaged converters for publicly available data sources. These converters are
bundled with the command-line tools in the GeoMesa binary distributions, in individual directories under ``conf/sfts``,
which are included by the ``conf/reference.conf`` file.

.. note::

    The examples in the sections below are often specific to the GeoMesa Accumulo distribution,
    but the general principle is the same for each distribution. Only the home variable
    (e.g. ``$GEOMESA_ACCUMULO_HOME``) and command-line tool name will differ depending on
    GeoMesa distribution.

.. toctree::
    :maxdepth: 1

    adsbx
    gdelt
    geolife
    geonames
    gtd
    nyctaxi
    osm
    osm-gpx
    marinecadastre-ais
    tdrive
    twitter
