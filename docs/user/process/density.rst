DensityProcess
^^^^^^^^^^^^^^

The ``DensityProcess`` computes a heat map over a set of features, returning a raster image. It accepts the following
parameters:

============  ===========
Parameter     Description
============  ===========
data          Input Simple Feature Collection to run the density process over
radiusPixels  Radius of the density kernel in pixels; controls the "fuzziness" of the density map
geomAttr      Name of the geometry attribute to render; if not specified will use the default geometry for the layer
weightAttr    Name of the attribute to use for data point weights; if not specified all points will be constant weight
outputBBOX    Bounding box and CRS of the output raster
outputWidth   Width of the output raster in pixels
outputHeight  Height of the output raster in pixels
============  ===========
