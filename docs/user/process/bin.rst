BinConversionProcess
^^^^^^^^^^^^^^^^^^^^^^

The ``BinConversionProcess`` converts an input feature collection to a custom minimized 16-byte binary format. It accepts the
following parameters:

==========  ===========
Parameter   Description
==========  ===========
features    Input feature collection to query
track       Track field to use for BIN records
geom        Geometry field to use for BIN records
dtg         Date field to use for BIN records
label       Label field to use for BIN records
axisOrder   Axis order - must be either ``latlon`` (latitude first) or ``lonlat`` (longitude first)
==========  ===========
