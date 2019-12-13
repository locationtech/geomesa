HBase Heatmaps
==============

GeoMesa on HBase can leverage server side processing to accelerate heatmap (density) queries. GeoMesa uses a custom
coprocessor running on the HBase Region Servers so that density calculation are done at the data. The per-region results
are then returned to GeoServer, combined there, and served. This functionality can be accessed using the
``geomesa:Density`` WPS process.

In order to use the density process you must:

* Install the server side code on your HBase cluster :ref:`hbase_deploy_distributed_runtime`
* Register the coprocessors site wide or for the table you wish to use :ref:`registering_coprocessors`
* Install the WPS processes in GeoServer :ref:_GeoMesa-process`
* Install the heatmap SLD in GeoServer :ref:`install-heatmap-sld`

.. _install-heatmap-sld:

Install Heatmap SLD
-------------------

In order to style the density result to produce a heatmap, GeoServer needs a Style Layer Descriptor (SLD).

To start, add a new SLD style to GeoServer named "heatmap" and use this sld
:download:`heatmap.sld </user/_static/sld/clientside-heatmap.sld>`

Instruction on how to install SLDs can be found here `GeoServer Styles <http://docs.geoserver.org/latest/en/user/styling/webadmin/index.html>`_.

Now you can change the style parameter to be ``styles=heatmap`` in the Layer Preview or PNG preview in GeoServer.

.. note::

  You may have to change the layer configuration to support publishing the heatmap style.


