Cassandra Heatmaps
==================

Cassandra can generate client-side heatmaps in GeoServer by defining a style and using the built in ``gs:Heatmap``
process within GeoServer.

To start, add a new SLD style to GeoServer named "heatmap" and use this sld
:download:`clientside-heatmap.sld </user/_static/sld/clientside-heatmap.sld>`

Now you can change the style parameter to be styles=heatmap in the Layer Preview or PNG preview in GeoServer.
