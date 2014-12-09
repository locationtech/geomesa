package org.locationtech.geomesa.raster.data

import java.io.File

import org.geotools.coverage.grid.GridCoverage2D
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.raster.ingest.SimpleRasterIngest._
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterStoreTest extends Specification {
  val rs = RasterStore("user", "pass", getClass.toString, "zk", getClass.toString, "S,USA", "S,USA", true)

  // Next three lines are the inputs
  val rasterName = "testRaster"
  val file = new File("/opt/devel/wcs/data/nlcd_DC_source_r_512.tif")
  val ingestTime = new DateTime()

  val rasterReader = getTiffReader(file)
  val rasterGrid: GridCoverage2D = rasterReader.read(null)

  val envelope = rasterGrid.getEnvelope2D
  val bbox = BoundingBox(envelope.getMinX, envelope.getMaxX, envelope.getMinY, envelope.getMaxY)
  val rasterMetadata = rasterMetadataFromFile(rasterName, file, "TIFF", ingestTime)
  val metadata = DecodedIndex(Raster.getRasterId(rasterName), bbox.geom, Option(ingestTime.getMillis))
  val raster = Raster(rasterGrid.getRenderedImage, metadata)

  rs.putRaster(raster)
}
