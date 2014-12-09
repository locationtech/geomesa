package org.locationtech.geomesa.plugin.wcs

import java.awt.Rectangle

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.io.{AbstractGridCoverage2DReader, AbstractGridFormat}
import org.geotools.coverage.grid.{GridCoverage2D, GridEnvelope2D, GridGeometry2D}
import org.geotools.factory.Hints
import org.geotools.geometry.GeneralEnvelope
import org.geotools.parameter.Parameter
import org.geotools.util.Utilities
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.raster.data.{RasterQuery, RasterStore}
import org.locationtech.geomesa.raster.feature
import org.locationtech.geomesa.utils.geohash.{BoundingBox, Bounds}
import org.opengis.parameter.GeneralParameterValue

object GeoMesaCoverageReader {
  val GeoServerDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val DefaultDateString = GeoServerDateFormat.print(new DateTime(DateTimeZone.forID("UTC")))
  val FORMAT = """accumulo://(.*):(.*)@(.*)/(.*)#columns=(.*)#geohash=(.*)#resolution=([0-9]*)#timeStamp=(.*)#rasterName=(.*)#zookeepers=([^#]*)(?:#auths=)?(.*)$""".r
}

import org.locationtech.geomesa.plugin.wcs.GeoMesaCoverageReader._

class GeoMesaCoverageReader(val url: String, hints: Hints) extends AbstractGridCoverage2DReader() with Logging {

  //TODO: WCS: Implement function/class for parsing our "new" url
  // right now we want to extract the table name and magnification like this "dataSource_mag"
  // later, if the magnification is not provided in the URL, we should estimate it later in the read() method

  // JNH: This todo is for Jake.
  logger.debug(s"""creating coverage reader for url "${url.replaceAll(":.*@", ":********@").replaceAll("#auths=.*","#auths=********")}"""")

  // Big goal: Set up values in the AGC2DReader class
  val FORMAT(user, password, instanceId, table, columnsStr, geohash, resolutionStr, timeStamp, rasterName, zookeepers, authtokens) = url

  logger.debug(s"extracted user $user, password ********, instance id $instanceId, table $table, columns $columnsStr, " +
    s"resolution $resolutionStr, zookeepers $zookeepers, auths ********")

  // TODO: Either this is needed for rasterToCoverages or remove it.
  this.crs = AbstractGridFormat.getDefaultCRS
  this.originalEnvelope = new GeneralEnvelope(Array(-180.0, -90.0), Array(180.0, 90.0))
  this.originalEnvelope.setCoordinateReferenceSystem(this.crs)
  this.originalGridRange = new GridEnvelope2D(new Rectangle(0, 0, 1024, 512))
  this.coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(this.hints)

  // </end setup>

  /**
   * Default implementation does not allow a non-default coverage name
   * @param coverageName
   * @return
   */
  override protected def checkName(coverageName: String) = {
    Utilities.ensureNonNull("coverageName", coverageName)
    true
  }

  override def getCoordinateReferenceSystem = this.crs

  override def getCoordinateReferenceSystem(coverageName: String) = this.getCoordinateReferenceSystem

  override def getFormat = new GeoMesaCoverageFormat

  def getGeohashPrecision = resolutionStr.toInt

  // TODO: Provide writeVisibilites??  Sort out read visibilites
  val ars: RasterStore = RasterStore(user, password, instanceId, zookeepers, table, authtokens, "")

  def read(parameters: Array[GeneralParameterValue]): GridCoverage2D = {
    logger.debug(s"READ: $parameters")
    val rq = getRQ(parameters)
    val rasters: Iterator[feature.Raster] = ars.getRasters(rq)
    rastersToCoverage(rasters)
  }

  def rastersToCoverage(rasters: Iterator[feature.Raster]): GridCoverage2D = {
    val raster = rasters.next // TODO: Mosaic
    this.coverageFactory.create(coverageName, raster.chunk, raster.envelope)
  }

  // TODO: JNH: Consider spinning this out as a with just the purpose of parsing GPV[]
  // NOTE: We need the resolutionStr from this class
  def getRQ(parameters: Array[GeneralParameterValue]): RasterQuery = {
    val paramsMap = parameters.map(gpv => (gpv.getDescriptor.getName.getCode, gpv)).toMap
    val gridGeometry = paramsMap(AbstractGridFormat.READ_GRIDGEOMETRY2D.getName.toString).asInstanceOf[Parameter[GridGeometry2D]].getValue
    val env = gridGeometry.getEnvelope
    val min = Array(Math.max(env.getMinimum(0), -180) + .00000001, Math.max(env.getMinimum(1), -90) + .00000001)
    val max = Array(Math.min(env.getMaximum(0), 180) - .00000001, Math.min(env.getMaximum(1), 90) - .00000001)
    val bbox = BoundingBox(Bounds(min(0), max(0)), Bounds(min(1), max(1)))
    RasterQuery(bbox, resolutionStr.toDouble, None, None)
  }

}