/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.ingest

import java.awt.RenderingHints
import java.io.File
import javax.media.jai.{ImageLayout, JAI}

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader
import org.geotools.coverageio.gdal.dted.DTEDReader
import org.geotools.factory.Hints
import org.geotools.gce.geotiff.GeoTiffReader
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeohashUtils}

import scala.util.Try

object SimpleRasterIngest {

  def getReader(imageFile: File, imageType: String): AbstractGridCoverage2DReader = {
    imageType match {
      case "tiff" => getTiffReader(imageFile)
      case "dted" => getDtedReader(imageFile)
      case _ => throw new Exception("Image type is not supported.")
    }
  }

  def getTiffReader(imageFile: File): AbstractGridCoverage2DReader = {
    new GeoTiffReader(imageFile, new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true))
  }

  def getDtedReader(imageFile: File): AbstractGridCoverage2DReader = {
    val l = new ImageLayout()
    l.setTileGridXOffset(0).setTileGridYOffset(0).setTileHeight(512).setTileWidth(512)
    val hints = new Hints
    hints.add(new RenderingHints(JAI.KEY_IMAGE_LAYOUT, l))
    new DTEDReader(imageFile, hints)
  }

}

import SimpleRasterIngest._

class SimpleRasterIngest(config: Map[String, Option[String]], cs: AccumuloCoverageStore) extends Logging {

  lazy val path = config(IngestRasterParams.FILE_PATH).get
  lazy val fileType = config(IngestRasterParams.FORMAT).get
  lazy val rasterName = config(IngestRasterParams.RASTER_NAME).get
  lazy val visibilities = config(IngestRasterParams.VISIBILITIES).get


  val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def runIngestTask() = Try {
    val file = new File(path)
    val ingestTime = config(IngestRasterParams.TIME).map(df.parseDateTime(_)).getOrElse(new DateTime(DateTimeZone.UTC))

    val rasterReader = getReader(file, fileType)
    val rasterGrid: GridCoverage2D = rasterReader.read(null)

    val envelope = rasterGrid.getEnvelope2D
    val bbox = BoundingBox(envelope.getMinX, envelope.getMaxX, envelope.getMinY, envelope.getMaxY)

    val metadata = DecodedIndex(Raster.getRasterId(rasterName), bbox.geom, Option(ingestTime.getMillis))

    val raster = Raster(rasterGrid.getRenderedImage, metadata)

    cs.saveRaster(raster)

  }
}
