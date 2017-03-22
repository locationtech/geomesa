/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.process

import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.GeoTools
import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.{HeatmapSurface, VectorProcess}
import org.locationtech.geomesa.accumulo.iterators.KryoLazyDensityIterator
import org.locationtech.geomesa.index.conf.QueryHints
import org.opengis.coverage.grid.GridGeometry
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression
import org.opengis.filter.spatial.BBOX
import org.opengis.util.ProgressListener

/**
 * Stripped down version of org.geotools.process.vector.HeatmapProcess
 */
@DescribeProcess(
  title = "Density Map",
  description = "Computes a density map over a set of features stored in Geomesa"
)
class DensityProcess extends VectorProcess {

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

    val gridWidth: Int = argOutputWidth
    val gridHeight: Int = argOutputHeight
    val radiusCells: Int = if (argRadiusPixels == null) 10 else argRadiusPixels

    val heatMap = new HeatmapSurface(radiusCells, argOutputEnv, gridWidth, gridHeight)
    val decode = KryoLazyDensityIterator.decodeResult(argOutputEnv, gridWidth, gridHeight)

    try {
      val features = obsFeatures.features()
      while (features.hasNext) {
        val pts = decode(features.next())
        while (pts.hasNext) {
          val (x, y, weight) = pts.next()
          heatMap.addPoint(x, y, weight)
        }
      }
      features.close()
    } catch {
      case e: Exception => throw new ProcessException("Error processing heatmap", e)
    }

    val heatMapGrid = DensityProcess.flipXY(heatMap.computeSurface)
    val gcf = CoverageFactoryFinder.getGridCoverageFactory(GeoTools.getDefaultHints)
    gcf.create("Process Results", heatMapGrid, argOutputEnv)
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
    val radiusPixels: Int = math.max(0, argRadiusPixels)
    val pixelSize = if (argOutputEnv.getWidth <= 0) 0 else  argOutputWidth / argOutputEnv.getWidth
    val queryBuffer: Double = radiusPixels / pixelSize
    val filter = targetQuery.getFilter.accept(new BBOXExpandingFilterVisitor(queryBuffer), null).asInstanceOf[Filter]
    val invertedQuery = new Query(targetQuery)
    invertedQuery.setFilter(filter)
    invertedQuery.setProperties(null)
    invertedQuery.getHints.put(QueryHints.DENSITY_BBOX, argOutputEnv)
    invertedQuery.getHints.put(QueryHints.DENSITY_WIDTH, argOutputWidth)
    invertedQuery.getHints.put(QueryHints.DENSITY_HEIGHT, argOutputHeight)
    if (argWeightAttr != null) {
      invertedQuery.getHints.put(QueryHints.DENSITY_WEIGHT, argWeightAttr)
    }
    invertedQuery
  }
}

object DensityProcess {

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
    val xSize = grid.length
    val ySize = grid(0).length
    val grid2 = Array.ofDim[Float](ySize, xSize)
    var ix = 0
    while (ix < xSize) {
      var iy = 0
      while (iy < ySize) {
        val iy2 = ySize - iy - 1
        grid2(iy2)(ix) = grid(ix)(iy)
        iy += 1
      }
      ix += 1
    }
    grid2
  }
}

/**
 * Copied from package protected org.geotools.process.vector.BBOXExpandingFilterVisitor
 */
class BBOXExpandingFilterVisitor(expandBy: Double) extends DuplicatingFilterVisitor {

  // noinspection ScalaDeprecation
  override def visit(filter: BBOX, extraData: AnyRef): AnyRef = {
    val propertyName: Expression = filter.getExpression1
    val minx = filter.getMinX
    val miny = filter.getMinY
    val maxx = filter.getMaxX
    val maxy = filter.getMaxY
    val srs = filter.getSRS
    val f = getFactory(extraData)
    f.bbox(propertyName, minx - expandBy, miny - expandBy, maxx + expandBy, maxy + expandBy, srs)
  }
}