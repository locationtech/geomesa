.. _geomesa_process_install:

Installation
------------

While processes can be used independently, the common use case is to use them with GeoServer. The process JARs are bundled
with each GeoMesa data store plugin. See :doc:`/user/geoserver` for additional details.

GeoServer does not ship by default with WPS enabled. In order to use GeoMesa processes, follow the
`GeoServer documentation <https://docs.geoserver.org/stable/en/user/services/wps/install.html>`__ on installing the
GeoServer WPS Extension.

.. note::

  Some processes also require custom output formats, available separately in the GPL licensed
  `GeoMesa GeoServer WFS <https://github.com/geomesa/geomesa-geoserver>`__ module.

To verify the install, start GeoServer, and you should see a line like
``INFO [geoserver.wps] - Found 15 bindable processes in GeoMesa Process Factory``.

In the GeoServer web UI, click *Demos* and then *WPS request builder*. From the request builder, under *Choose Process*, click on
any of the ``geomesa:`` options to build up example requests and in some cases see results.
