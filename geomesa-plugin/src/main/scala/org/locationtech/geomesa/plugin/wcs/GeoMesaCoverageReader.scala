package org.locationtech.geomesa.plugin.wcs

import java.awt.image._
import java.awt.{Point, Rectangle}
import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{IteratorSetting, Scanner, ZooKeeperInstance}
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.io.{AbstractGridCoverage2DReader, AbstractGridFormat}
import org.geotools.coverage.grid.{GridCoverage2D, GridEnvelope2D, GridGeometry2D}
import org.geotools.factory.Hints
import org.geotools.geometry.GeneralEnvelope
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.parameter.Parameter
import org.geotools.referencing.CRS
import org.geotools.util.{DateRange, Utilities}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.iterators.{AggregatingKeyIterator, SurfaceAggregatingIterator}
import org.locationtech.geomesa.core.util.{BoundingBoxUtil, SelfClosingBatchScanner}
import org.locationtech.geomesa.plugin.ImageUtils._
import org.locationtech.geomesa.raster.index.RasterIndexEntry
import org.locationtech.geomesa.utils.geohash.{BoundingBox, Bounds, GeoHash, TwoGeoHashBoundingBox}
import org.opengis.coverage.grid.GridCoverage
import org.opengis.geometry.Envelope
import org.opengis.parameter.GeneralParameterValue

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random

object GeoMesaCoverageReader {
  val GeoServerDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val DefaultDateString = GeoServerDateFormat.print(new DateTime(DateTimeZone.forID("UTC")))
  val FORMAT = """accumulo://(.*):(.*)@(.*)/(.*)#columns=(.*)#geohash=(.*)#resolution=([0-9]*)#timeStamp=(.*)#rasterName=(.*)#zookeepers=([^#]*)(?:#auths=)?(.*)$""".r
}

import org.locationtech.geomesa.plugin.wcs.GeoMesaCoverageReader._

class GeoMesaCoverageReader(val url: String, hints: Hints) extends AbstractGridCoverage2DReader() with Logging {

  logger.debug(s"""creating coverage reader for url "${url.replaceAll(":.*@", ":********@").replaceAll("#auths=.*","#auths=********")}"""")

  val FORMAT(user, password, instanceId, table, columnsStr, geohash, resolutionStr, timeStamp, rasterName, zookeepers, authtokens) = url

  logger.debug(s"extracted user $user, password ********, instance id $instanceId, table $table, columns $columnsStr, " +
    s"resolution $resolutionStr, zookeepers $zookeepers, auths ********")

  coverageName = table + ":" + columnsStr
  val columns = columnsStr.split(",").map(_.split(":").take(2) match {
    case Array(columnFamily, columnQualifier, _) => (columnFamily, columnQualifier)
    case Array(columnFamily) => (columnFamily, "")
    case _ =>
  })

  this.crs = AbstractGridFormat.getDefaultCRS
  this.originalEnvelope = new GeneralEnvelope(Array(-180.0, -90.0), Array(180.0, 90.0))
  this.originalEnvelope.setCoordinateReferenceSystem(this.crs)
  this.originalGridRange = new GridEnvelope2D(new Rectangle(0, 0, 1024, 512))
  this.coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(this.hints)

  val zkInstance = new ZooKeeperInstance(instanceId, zookeepers)
  val connector = zkInstance.getConnector(user, new PasswordToken(password.getBytes))

  // When parsing an old-form Accumulo layer URI the authtokens field matches the empty string, requesting no authorizations
  val auths = new Authorizations(authtokens.split(","): _*)

  val aggPrefix = AggregatingKeyIterator.aggOpt
  val timeStampString = timeStamp.toLong

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

  def read(parameters: Array[GeneralParameterValue]): GridCoverage2D = {
    val paramsMap = parameters.map(gpv => (gpv.getDescriptor.getName.getCode, gpv)).toMap
    val gridGeometry = paramsMap(AbstractGridFormat.READ_GRIDGEOMETRY2D.getName.toString).asInstanceOf[Parameter[GridGeometry2D]].getValue
    val env = gridGeometry.getEnvelope
    val min = Array(Math.max(env.getMinimum(0), -180) + .00000001, Math.max(env.getMinimum(1), -90) + .00000001)
    val max = Array(Math.min(env.getMaximum(0), 180) - .00000001, Math.min(env.getMaximum(1), 90) - .00000001)
    val bbox = BoundingBox(Bounds(min(0), max(0)), Bounds(min(1), max(1)))
    
    val chunks = getChunks(geohash, getGeohashPrecision, None, bbox)
    val image = mosaicGridCoverages(chunks, env = env)
    this.coverageFactory.create(coverageName, image, env)
  }

  def getChunks(geohash: String, iRes: Int, timeParam: Option[Either[Date, DateRange]], bbox: BoundingBox): Iterator[GridCoverage] = {
    withScanner(scanner => {
      val row = new Text(s"~$iRes~$geohash")
      scanner.setRange(new org.apache.accumulo.core.data.Range(row))
      val name = "version-" + Random.alphanumeric.take(5).mkString
      val cfg = new IteratorSetting(2, name, classOf[VersioningIterator])
      VersioningIterator.setMaxVersions(cfg, 1)
      scanner.addScanIterator(cfg)
    })(_.map(entry => {
      this.coverageFactory.create(coverageName,
        rasterImageDeserialize(entry.getValue.get),
        new ReferencedEnvelope(RasterIndexEntry.decodeIndexCQMetadata(entry.getKey).geom.getEnvelopeInternal, CRS.decode("EPSG:4326")))
    })).toIterator
  }

  protected def withScanner[A](configure: Scanner => Unit)(f: Scanner => A): A = {
    val scanner = connector.createScanner(table, auths)
    try {
      configure(scanner)
      f(scanner)
    } catch {
      case e: Exception => throw new Exception(s"Error accessing table ", e)
    }
  }

  def getCoverages(env: Envelope, gridGeometry: GridGeometry2D): Iterator[GridCoverage2D] = {
    val xDim = gridGeometry.getGridRange2D.getSpan(0)
    val yDim = gridGeometry.getGridRange2D.getSpan(1)
    val min = Array(Math.max(env.getMinimum(0), -180) + .00000001, Math.max(env.getMinimum(1), -90) + .00000001)
    val max = Array(Math.min(env.getMaximum(0), 180) - .00000001, Math.min(env.getMaximum(1), 90) - .00000001)
    val bbox = BoundingBox(Bounds(min(0), max(0)), Bounds(min(1), max(1)))
    val ghBbox = TwoGeoHashBoundingBox(bbox, getGeohashPrecision)
    val rescaleX = ghBbox.ur.bbox.longitudeSize - 1
    val rescaleY = ghBbox.ur.bbox.latitudeSize - 1
    val xdim = math.max(1, math.min(xDim, math.round(ghBbox.bbox.longitudeSize / rescaleX).toInt))
    val ydim = math.max(1, math.min(yDim, math.round(ghBbox.bbox.latitudeSize / rescaleY).toInt))

    val scanBuffers = getScanBuffers(bbox, xdim, ydim)
    val bufferList: List[Array[Byte]] = scanBuffers.map(_.getValue.get()).toList
    val geomList: List[Geometry] = scanBuffers.map(e => RasterIndexEntry.decodeIndexCQMetadata(e.getKey).geom).toList
    val coverageList = new ListBuffer[GridCoverage2D]()
    bufferList.zipWithIndex.foreach({ case (raster, idx) =>
      val dbuffer = new DataBufferByte(raster, xdim * ydim)
      val sampleModel = new BandedSampleModel(DataBuffer.TYPE_BYTE,
        xdim,
        ydim,
        1)
      val tile = Raster.createWritableRaster(sampleModel, dbuffer, new Point(0, 0))
      val envelope = new ReferencedEnvelope(geomList.get(idx).getEnvelopeInternal, CRS.decode("EPSG:4326"))
      coverageList += this.coverageFactory.create(coverageName, tile, envelope)
    })
    coverageList.toIterator
  }

  def getScanBuffers(bbox: BoundingBox, xDim: Int, yDim: Int) = {
    val scanner = connector.createBatchScanner(table, auths, 10)
    scanner.fetchColumn(new Text(""), new Text(s"$rasterName~$timeStampString"))

    val ranges = BoundingBoxUtil.getRangesByRow(BoundingBox.getGeoHashesFromBoundingBox(bbox))
    scanner.setRanges(ranges)

    AggregatingKeyIterator.setupAggregatingKeyIterator(scanner,
      1000,
      classOf[SurfaceAggregatingIterator],
      Map[String, String](
        s"${aggPrefix}bottomLeft" -> GeoHash(bbox.ll, getGeohashPrecision).hash,
        s"${aggPrefix}topRight" -> GeoHash(bbox.ur, getGeohashPrecision).hash,
        s"${aggPrefix}precision" -> getGeohashPrecision.toString,
        s"${aggPrefix}dims" -> s"$xDim,$yDim"
      )
    )

    SelfClosingBatchScanner(scanner)
  }

  def mosaicGridCoverages(coverageList: Iterator[GridCoverage], width: Int = 256, height: Int = 256, env: Envelope, startImage: BufferedImage = null) = {
    val image = if (startImage == null) { getEmptyImage(width, height) } else { startImage }
    while (coverageList.hasNext) {
      val coverage = coverageList.next()
      val coverageEnv = coverage.getEnvelope
      val coverageImage = coverage.getRenderedImage
      val posx = ((coverageEnv.getMinimum(0) - env.getMinimum(0)) / 1.0).asInstanceOf[Int]
      val posy = ((env.getMaximum(1) - coverageEnv.getMaximum(1)) / 1.0).asInstanceOf[Int]
      image.getRaster.setDataElements(posx, posy, coverageImage.getData)
    }
    image
  }

  import org.geotools.coverage.grid.io.GridCoverage2DReader._

  override def getMetadataNames: Array[String] = Array[String](TIME_DOMAIN, HAS_TIME_DOMAIN)

  override def getMetadataValue(name: String): String = name match {
    case TIME_DOMAIN =>
      // fetch the list, formatted for GeoServer, of all of the date/times
      // for which the current Accumulo surface is available
      // (NB:  that this should be a list is dictated by the code that
      // originally registered the surface with GeoServer)

      // short-cut:  each of the surface-dates will have a separate "count"
      // entry among the metadata; this provides a single list of contiguous
      // entries to scan for timestamps
      val scanner: Scanner = connector.createScanner(table, auths)
      scanner.setRange(new org.apache.accumulo.core.data.Range("~METADATA"))
      columns.foreach{ case (cf: String, _) => scanner.fetchColumn(new Text(cf), new Text("count"))}

      val dtListString =
        scanner
          .iterator()
          .map(entry => entry.getKey.getTimestamp * 1000L)
          .map(millis => new DateTime(millis, DateTimeZone.forID("UTC")))
          .map(dt => GeoServerDateFormat.print(dt))
          .toList
          .distinct
          .mkString(",")

      // ensure that at least one (albeit, dummy) date is returned
      if (dtListString.trim.length < 1) DefaultDateString else dtListString
    case HAS_TIME_DOMAIN => "true"
    case _ => null
  }
}