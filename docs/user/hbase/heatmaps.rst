HBase Heatmaps
==============

HBase can generate client-side heatmaps in GeoServer by defining a style and using the built in ``gs:Heatmap`` process
within GeoServer.

To start, add a new SLD style to GeoServer named "hbase-heatmap" and use this sld
:download:`hbase-heatmap.sld <_static/sld/hbase-heatmap.sld>`

Now you can change style parameter to be styles=hbase-heatmap in the Layer Preview or PNG preview in GeoServer.
