/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.locationtech.geomesa.plugin.process

import org.geotools.data.Query
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.factory.{DescribeParameter, DescribeProcess}
import org.geotools.process.vector.HeatmapProcess
import org.locationtech.geomesa.core.index.QueryHints
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
