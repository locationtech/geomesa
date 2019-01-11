/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import java.awt.image.DataBuffer

import javax.media.jai.RasterFactory
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.GeoTools
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.{BBOXExpandingFilterVisitor, HeatmapSurface}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.process.GeoMesaProcess
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.coverage.grid.GridGeometry
import org.opengis.filter.Filter
import org.opengis.util.ProgressListener

/**
 * Stripped down version of org.geotools.process.vector.HeatmapProcess
 */
@DescribeProcess(
  title = "Density Map",
  description = "Computes a density map over a set of features stored in Geomesa"
)
class DensityProcess extends GeoMesaProcess {

  import DensityProcess.DefaultRadiusPixels

  @throws(classOf[ProcessException])
  @DescribeResult(name = "result", description = "Output raster")
  def execute(@DescribeParameter(name = "data", description = "Input features")
              obsFeatures: SimpleFeatureCollection,
              @DescribeParameter(name = "radiusPixels", description = "Radius of the density kernel in pixels")
              argRadiusPixels: Integer,
              @DescribeParameter(name = "weightAttr", description = "Name of the attribute to use for data point weight", min = 0, max = 1)
              argWeightAttr: String,
              @DescribeParameter(name = "outputBBOX", description = "Bounding box of the output")
              argOutputEnv: ReferencedEnvelope,
              @DescribeParameter(name = "outputWidth", description = "Width of output raster in pixels")
              argOutputWidth: Integer,
              @DescribeParameter(name = "outputHeight", description = "Height of output raster in pixels")
              argOutputHeight: Integer,
              monitor: ProgressListener): GridCoverage2D = {

    val pixels = Option(argRadiusPixels).map(_.intValue).getOrElse(DefaultRadiusPixels)

    // buffer our calculations based on the pixel radius to avoid edge artifacts
    val outputWidth  = argOutputWidth + 2 * pixels
    val outputHeight = argOutputHeight + 2 * pixels
    val bufferWidth  = pixels * argOutputEnv.getWidth / argOutputWidth
    val bufferHeight = pixels * argOutputEnv.getHeight / argOutputHeight
    val envelope = new ReferencedEnvelope(argOutputEnv)
    envelope.expandBy(bufferWidth, bufferHeight)

    val decode = DensityScan.decodeResult(envelope, outputWidth, outputHeight)

    val heatMap = new HeatmapSurface(pixels, envelope, outputWidth, outputHeight)

    try {
      WithClose(obsFeatures.features()) { features =>
        while (features.hasNext) {
          val pts = decode(features.next())
          while (pts.hasNext) {
            val (x, y, weight) = pts.next()
            heatMap.addPoint(x, y, weight)
          }
        }
      }
    } catch {
      case e: Exception => throw new ProcessException("Error processing heatmap", e)
    }

    val heatMapGrid = DensityProcess.flipXY(heatMap.computeSurface)

    // create the raster from our unbuffered envelope and discard the buffered pixels in our final image
    val raster = RasterFactory.createBandedRaster(DataBuffer.TYPE_FLOAT, argOutputWidth, argOutputHeight, 1, null)

    var i, j = pixels
    while (j < heatMapGrid.length - pixels) {
      val row = heatMapGrid(j)
      while (i < row.length - pixels) {
        raster.setSample(i - pixels, j - pixels, 0, row(i))
        i += 1
      }
      j += 1
      i = pixels
    }

    val gcf = CoverageFactoryFinder.getGridCoverageFactory(GeoTools.getDefaultHints)
    gcf.create("Process Results", raster, argOutputEnv)
  }

  /**
   * Given a target query and a target grid geometry returns the query to be used to read the
   * input data of the process involved in rendering. In this process this method is used to:
   * <ul>
   * <li>determine the extent & CRS of the output grid
   * <li>expand the query envelope to ensure stable surface generation
   * <li>modify the query hints to ensure point features are returned
   * </ul>
   * Note that in order to pass validation, all parameters named here must also appear in the
   * parameter list of the <tt>execute</tt> method, even if they are not used there.
   *
   * @param argRadiusPixels the feature type attribute that contains the observed surface value
   * @param targetQuery the query used against the data source
   * @param targetGridGeometry the grid geometry of the destination image
   * @return The transformed query
   */
  @throws(classOf[ProcessException])
  def invertQuery(@DescribeParameter(name = "radiusPixels", description = "Radius to use for the kernel", min = 0, max = 1)
                  argRadiusPixels: Integer,
                  @DescribeParameter(name = "weightAttr", description = "Name of the attribute to use for data point weight", min = 0, max = 1)
                  argWeightAttr: String,
                  @DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output")
                  argOutputEnv: ReferencedEnvelope,
                  @DescribeParameter(name = "outputWidth", description = "Width of the output raster")
                  argOutputWidth: Integer,
                  @DescribeParameter(name = "outputHeight", description = "Height of the output raster")
                  argOutputHeight: Integer,
                  targetQuery: Query,
                  targetGridGeometry: GridGeometry): Query = {
    if (argOutputWidth == null || argOutputHeight == null) {
      throw new IllegalArgumentException("outputWidth and/or outputHeight not specified")
    } else if (argOutputWidth < 0 || argOutputHeight < 0) {
      throw new IllegalArgumentException("outputWidth and outputHeight must both be positive")
    }

    val pixels = Option(argRadiusPixels).map(_.intValue).getOrElse(DefaultRadiusPixels)

    // buffer our calculations based on the pixel radius to avoid edge artifacts
    val outputWidth  = argOutputWidth + 2 * pixels
    val outputHeight = argOutputHeight + 2 * pixels

    val bufferWidth  = pixels * argOutputEnv.getWidth / argOutputWidth
    val bufferHeight = pixels * argOutputEnv.getHeight / argOutputHeight

    val envelope = new ReferencedEnvelope(argOutputEnv)
    envelope.expandBy(bufferWidth, bufferHeight)

    val filter = {
      val buf = math.max(bufferWidth, bufferHeight)
      targetQuery.getFilter.accept(new BBOXExpandingFilterVisitor(buf, buf, buf, buf), null).asInstanceOf[Filter]
    }

    val invertedQuery = new Query(targetQuery)
    invertedQuery.setFilter(filter)
    invertedQuery.setProperties(null)
    invertedQuery.getHints.put(QueryHints.DENSITY_BBOX, envelope)
    invertedQuery.getHints.put(QueryHints.DENSITY_WIDTH, outputWidth)
    invertedQuery.getHints.put(QueryHints.DENSITY_HEIGHT, outputHeight)
    if (argWeightAttr != null) {
      invertedQuery.getHints.put(QueryHints.DENSITY_WEIGHT, argWeightAttr)
    }
    invertedQuery
  }
}

object DensityProcess {

  val DefaultRadiusPixels: Int = 10

  /**
   * Flips an XY matrix along the X=Y axis, and inverts the Y axis. Used to convert from
   * "map orientation" into the "image orientation" used by GridCoverageFactory. The surface
   * interpolation is done on an XY grid, with Y=0 being the bottom of the space. GridCoverages
   * are stored in an image format, in a YX grid with Y=0 being the top.
   *
   * @param grid the grid to flip
   * @return the flipped grid
   */
  def flipXY(grid: Array[Array[Float]]): Array[Array[Float]] = {
    val length_x = grid.length
    val length_y = grid(0).length

    val res = Array.fill(length_y,length_x)(0f)

    for ( x <- 0 until length_x ; y <- 0 until length_y ) {
      val x1 = length_y - 1 - y
      val y1 = x
      res(x1)(y1) = grid(x)(y)
    }

    res
  }
}
