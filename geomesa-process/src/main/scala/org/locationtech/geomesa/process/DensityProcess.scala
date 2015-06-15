/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.process

import org.geotools.data.Query
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.factory.{DescribeParameter, DescribeProcess}
import org.geotools.process.vector.HeatmapProcess
import org.locationtech.geomesa.accumulo.index.QueryHints
import org.opengis.coverage.grid.GridGeometry

@DescribeProcess(
  title = "Density Map",
  description = "Computes a density map over a set of features stored in Geomesa"
)
class DensityProcess extends HeatmapProcess {
  override def invertQuery(@DescribeParameter(name = "radiusPixels", description = "Radius to use for the kernel", min = 0, max = 1) argRadiusPixels: Integer,
                           @DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output") argOutputEnv: ReferencedEnvelope,
                           @DescribeParameter(name = "outputWidth", description = "Width of the output raster")  argOutputWidth: Integer,
                           @DescribeParameter(name = "outputHeight", description = "Height of the output raster") argOutputHeight: Integer,
                           targetQuery: Query,
                           targetGridGeometry: GridGeometry): Query = {
    val q =
      super.invertQuery(argRadiusPixels,
        argOutputEnv,
        argOutputWidth,
        argOutputHeight,
        targetQuery,
        targetGridGeometry)

    q.getHints.put(QueryHints.BBOX_KEY, argOutputEnv)
    q.getHints.put(QueryHints.DENSITY_KEY, java.lang.Boolean.TRUE)
    q.getHints.put(QueryHints.WIDTH_KEY, argOutputWidth)
    q.getHints.put(QueryHints.HEIGHT_KEY, argOutputHeight)
    q
  }
}
