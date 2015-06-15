/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.wfs.output

import java.io.{BufferedOutputStream, OutputStream}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, Executors}
import javax.xml.namespace.QName

import com.typesafe.scalalogging.slf4j.Logging
import net.opengis.wfs.{GetFeatureType => GetFeatureTypeV1, QueryType => QueryTypeV1}
import net.opengis.wfs20.{GetFeatureType => GetFeatureTypeV2, QueryType => QueryTypeV2}
import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.data.DataStore
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.util.Version
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator._
import org.locationtech.geomesa.accumulo.iterators.BinSorter
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

import scala.collection.JavaConversions._

/**
 * Output format for wfs requests that encodes features into a binary format.
 * To trigger, use outputFormat=application/vnd.binary-viewer in your wfs request
 *
 * Required flags:
 * format_options=trackId:<track_attribute_name>;
 *
 * Optional flags:
 * format_options=trackId:<track_attribute_name>;geom:<geometry_attribute_name>;dtg:<dtg_attribute_name>;label:<label_attribute_name>
 *
 * @param gs
 */
class BinaryViewerOutputFormat(gs: GeoServer)
    extends WFSGetFeatureOutputFormat(gs, Set("bin", BinaryViewerOutputFormat.MIME_TYPE)) with Logging {

  import org.locationtech.geomesa.plugin.wfs.output.BinaryViewerOutputFormat._

  override def getMimeType(value: AnyRef, operation: Operation) = MIME_TYPE

  override def getPreferredDisposition(value: AnyRef, operation: Operation) = Response.DISPOSITION_INLINE

  override def getAttachmentFileName(value: AnyRef, operation: Operation) = {
    val gfr = GetFeatureRequest.adapt(operation.getParameters()(0))
    val name = Option(gfr.getHandle).getOrElse(gfr.getQueries.get(0).getTypeNames.get(0).getLocalPart)
    // if they have requested a label, then it will be 24 byte encoding (assuming the field exists...)
    val size = if (gfr.getFormatOptions.containsKey(LABEL_FIELD)) "24" else "16"
    s"$name.$FILE_EXTENSION$size"
  }

  override def write(featureCollections: FeatureCollectionResponse,
                     output: OutputStream,
                     getFeature: Operation): Unit = {

    // format_options flags for customizing the request
    val request = GetFeatureRequest.adapt(getFeature.getParameters()(0))
    val trackId = Option(request.getFormatOptions.get(TRACK_ID_FIELD).asInstanceOf[String]).getOrElse {
      throw new IllegalArgumentException(s"$TRACK_ID_FIELD is a required format option")
    }
    val geom = Option(request.getFormatOptions.get(GEOM_FIELD).asInstanceOf[String])
    val dtg = Option(request.getFormatOptions.get(DATE_FIELD).asInstanceOf[String])
    val label = Option(request.getFormatOptions.get(LABEL_FIELD).asInstanceOf[String])
    val binSize = if (label.isDefined) 24 else 16

    // depending on srs requested and wfs versions, axis order can be flipped
    val axisOrder = checkAxisOrder(getFeature)
    val requestedSort =
      Option(request.getFormatOptions.get(SORT_FIELD).asInstanceOf[String]).exists(_.toBoolean)

    val bos = new BufferedOutputStream(output)

    val sort = requestedSort || sys.props.getOrElse(SORT_SYS_PROP, DEFAULT_SORT).toBoolean
    val tserverSort = sort || sys.props.getOrElse(PARTIAL_SORT_SYS_PROP, DEFAULT_SORT).toBoolean
    val batchSize = sys.props.getOrElse(BATCH_SIZE_SYS_PROP, DEFAULT_BATCH_SIZE).toInt

    // set hints into thread local state - this prevents any wrapping feature collections from messing with
    // the aggregation
    val hints = {
      val some = Map(BIN_TRACK_KEY -> trackId, BIN_SORT_KEY -> tserverSort, BIN_BATCH_SIZE_KEY -> batchSize)
      val opts = Map(BIN_GEOM_KEY -> geom, BIN_DTG_KEY -> dtg, BIN_LABEL_KEY -> label)
          .collect { case (k, Some(v)) => k -> v }
      (some ++ opts).asInstanceOf[Map[AnyRef, AnyRef]]
    }
    QueryPlanner.setPerThreadQueryHints(hints)

    try {
      featureCollections.getFeatures.foreach { fc =>
        val iter = fc.asInstanceOf[SimpleFeatureCollection].features()

        // this check needs to be done *after* getting the feature iterator so that the return sft will be set
        val aggregated = fc.getSchema == BIN_SFT
        if (aggregated) {
          // for accumulo, encodings have already been computed in the tservers
          val aggregates = iter.map(_.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])

          if (sort) {
            // we do some asynchronous pre-merging while we are waiting for all the data to come in
            // the pre-merging is expensive, as it merges in memory
            // the final merge doesn't have to allocate space for merging, as it writes directly to the output
            val numThreads = sys.props.getOrElse(SORT_THREADS_SYS_PROP, DEFAULT_SORT_THREADS).toInt
            val executor = Executors.newFixedThreadPool(numThreads)
            // access to this is manually synchronized so we can pull off 2 items at once
            val mergeQueue = collection.mutable.PriorityQueue.empty[Array[Byte]](new Ordering[Array[Byte]] {
              // shorter first
              override def compare(x: Array[Byte], y: Array[Byte]) = y.length.compareTo(x.length)
            })
            // holds buffers we don't want to consider anymore due to there size - also manually synchronized
            val doneMergeQueue = collection.mutable.ArrayBuffer.empty[Array[Byte]]
            val maxSizeToMerge = sys.props.getOrElse(SORT_HEAP_SYS_PROP, DEFAULT_SORT_HEAP).toInt
            val latch = new CountDownLatch(numThreads)
            val keepMerging = new AtomicBoolean(true)
            var i = 0
            while (i < numThreads) {
              executor.submit(new Runnable() {
                override def run() = {
                  while (keepMerging.get()) {
                    // pull out the 2 smallest items to merge
                    // the final merge has to compare the first item in each buffer
                    // so reducing the number of buffers helps
                    val (left, right) = mergeQueue.synchronized {
                      if (mergeQueue.length > 1) {
                        (mergeQueue.dequeue(), mergeQueue.dequeue())
                      } else {
                        (null, null)
                      }
                    }
                    if (left != null) { // right will also not be null
                      if (right.length > maxSizeToMerge) {
                        if (left.length > maxSizeToMerge) {
                          doneMergeQueue.synchronized(doneMergeQueue.append(left, right))
                        } else {
                          doneMergeQueue.synchronized(doneMergeQueue.append(right))
                          mergeQueue.synchronized(mergeQueue.enqueue(left))
                        }
                        Thread.sleep(10)
                      } else {
                        val result = BinSorter.mergeSort(left, right, binSize)
                        mergeQueue.synchronized(mergeQueue.enqueue(result))
                      }
                    } else {
                      // if we didn't find anything to merge, wait a bit before re-checking
                      Thread.sleep(10)
                    }
                  }
                  latch.countDown() // indicate this thread is done
                }
              })
              i += 1
            }
            // queue up the aggregates coming in so that they can be processed by the merging threads above
            aggregates.foreach(a => mergeQueue.synchronized(mergeQueue.enqueue(a)))
            // once all data is back from the tservers, stop pre-merging and start streaming back to the client
            keepMerging.set(false)
            executor.shutdown() // this won't stop the threads, but will cleanup once they're done
            latch.await() // wait for the merge threads to finish
            // get an iterator that returns in sorted order
            val bins = BinSorter.mergeSort((doneMergeQueue ++ mergeQueue).iterator, binSize)
            while (bins.hasNext) {
              val (aggregate, offset) = bins.next()
              bos.write(aggregate, offset, binSize)
            }
          } else {
            // no sort, just write directly to the output
            aggregates.foreach(bos.write)
          }
        } else {
          logger.warn(s"Server side bin aggregation is not enabled for feature collection '${fc.getClass}'")
          // for non-accumulo fs we do the encoding here
          val sfc = fc.asInstanceOf[SimpleFeatureCollection]
          val track = Some(trackId)
          val date = dtg.orElse(getDateField(getFeature)).getOrElse {
            throw new IllegalArgumentException("DTG is required for non-accumulo feature collections")
          }
          BinaryOutputEncoder.encodeFeatureCollection(sfc, bos, date, track, label, None, axisOrder, sort)
        }

        iter.close()
        bos.flush()
      }
    } finally {
      QueryPlanner.clearPerThreadQueryHints()
    }
    // none of the implementations in geoserver call 'close' on the output stream
  }

  /**
   * Try to pull out the default date field from the SimpleFeatureType associated with this request
   *
   * @param getFeature
   * @return
   */
  private def getDateField(getFeature: Operation): Option[String] =
    for {
      tn        <- getTypeName(getFeature)
      name      =  tn.getLocalPart
      layer     <- Option(gs.getCatalog.getLayerByName(name))
      storeId   =  layer.getResource.getStore.getId
      dataStore =  gs.getCatalog.getDataStore(storeId).getDataStore(null).asInstanceOf[DataStore]
      schema    <- Option(dataStore.getSchema(name))
      dtgField  <- schema.getDtgField
    } yield dtgField

}

object BinaryViewerOutputFormat extends Logging {

  import org.locationtech.geomesa.filter.function.AxisOrder
  import org.locationtech.geomesa.filter.function.AxisOrder.{LatLon, LonLat}

  val MIME_TYPE = "application/vnd.binary-viewer"
  val FILE_EXTENSION = "bin"
  val TRACK_ID_FIELD = "TRACKID"
  val GEOM_FIELD     = "GEOM"
  val DATE_FIELD     = "DTG"
  val LABEL_FIELD    = "LABEL"
  val SORT_FIELD     = "SORT"

  val SORT_SYS_PROP         = "geomesa.output.bin.sort"
  val PARTIAL_SORT_SYS_PROP = "geomesa.output.bin.sort.partial"
  val SORT_THREADS_SYS_PROP = "geomesa.output.bin.sort.threads"
  val SORT_HEAP_SYS_PROP    = "geomesa.output.bin.sort.memory"
  val BATCH_SIZE_SYS_PROP   = "geomesa.output.bin.batch.size"

  val DEFAULT_SORT = "false"
  val DEFAULT_SORT_THREADS = "2"
  val DEFAULT_SORT_HEAP = "2097152" // 2MB
  val DEFAULT_BATCH_SIZE = "65536" // 1MB for 16 byte bins

  // constants used to determine axis order from geoserver
  val wfsVersion1 = new Version("1.0.0")
  val srsVersionOnePrefix = "http://www.opengis.net/gml/srs/epsg.xml#"
  val srsVersionOnePlusPrefix = "urn:x-ogc:def:crs:epsg:"
  val srsNonStandardPrefix = "epsg:"

  /**
   * Determines the order of lat/lon in simple features returned by this request.
   *
   * See http://docs.geoserver.org/2.5.x/en/user/services/wfs/basics.html#axis-ordering for details
   * on how geoserver handles axis order.
   *
   * @param getFeature
   * @return
   */
  def checkAxisOrder(getFeature: Operation): AxisOrder.AxisOrder =
    getSrs(getFeature) match {
      // if an explicit SRS is requested, that takes priority
      // SRS format associated with WFS 1.1.0 and 2.0.0 - lat is first
      case Some(srs) if srs.toLowerCase.startsWith(srsVersionOnePlusPrefix) => AxisOrder.LatLon
      // SRS format associated with WFS 1.0.0 - lon is first
      case Some(srs) if srs.toLowerCase.startsWith(srsVersionOnePrefix) => LonLat
      // non-standard SRS format - geoserver puts lon first
      case Some(srs) if srs.toLowerCase.startsWith(srsNonStandardPrefix) => LonLat
      case Some(srs) =>
        val valid = s"${srsVersionOnePrefix}xxxx, ${srsVersionOnePlusPrefix}xxxx, ${srsNonStandardPrefix}xxxx"
        throw new IllegalArgumentException(s"Invalid SRS format: '$srs'. Valid options are: $valid")
      // if no explicit SRS: wfs 1.0.0 stores x = lon y = lat, anything greater stores x = lat y = lon
      case None => if (getFeature.getService.getVersion.compareTo(wfsVersion1) > 0) LatLon else LonLat
    }

  def getTypeName(getFeature: Operation): Option[QName] = {
    val typeNamesV2 = getFeatureTypeV2(getFeature)
        .flatMap(getQueryType)
        .map(_.getTypeNames.toList)
        .getOrElse(Seq.empty)
    val typeNamesV1 = getFeatureTypeV1(getFeature)
        .flatMap(getQueryType)
        .map(_.getTypeName.toList)
        .getOrElse(Seq.empty)
    val typeNames = typeNamesV2 ++ typeNamesV1
    if (typeNames.size > 1) {
      logger.warn(s"Multiple TypeNames detected in binary format request (using first): $typeNames")
    }
    typeNames.headOption.map(_.asInstanceOf[QName])
  }

  /**
   * Function to pull requested SRS out of a WFS request
   *
   * @param getFeature
   * @return
   */
  def getSrs(getFeature: Operation): Option[String] =
    getFeatureTypeV2(getFeature).flatMap(getSrs)
        .orElse(getFeatureTypeV1(getFeature).flatMap(getSrs))

  /**
   * Function to pull requested SRS out of WFS 1.0.0/1.1.0 request
   *
   * @param getFeatureType
   * @return
   */
  def getSrs(getFeatureType: GetFeatureTypeV1): Option[String] =
    getQueryType(getFeatureType).flatMap(qt => Option(qt.getSrsName)).map(_.toString)

  /**
   * Function to pull requested SRS out of WFS 2 request
   *
   * @param getFeatureType
   * @return
   */
  def getSrs(getFeatureType: GetFeatureTypeV2): Option[String] =
    getQueryType(getFeatureType).flatMap(qt => Option(qt.getSrsName)).map(_.toString)

  /**
   *
   * @param getFeature
   * @return
   */
  def getFeatureTypeV2(getFeature: Operation): Option[GetFeatureTypeV2] =
    getFeature.getParameters.find(_.isInstanceOf[GetFeatureTypeV2])
        .map(_.asInstanceOf[GetFeatureTypeV2])

  /**
   *
   * @param getFeature
   * @return
   */
  def getFeatureTypeV1(getFeature: Operation): Option[GetFeatureTypeV1] =
    getFeature.getParameters.find(_.isInstanceOf[GetFeatureTypeV1])
        .map(_.asInstanceOf[GetFeatureTypeV1])

  /**
   * Pull out query object from request
   *
   * @param getFeatureType
   * @return
   */
  def getQueryType(getFeatureType: GetFeatureTypeV1): Option[QueryTypeV1] =
    getFeatureType.getQuery.iterator()
        .find(_.isInstanceOf[QueryTypeV1])
        .map(_.asInstanceOf[QueryTypeV1])

  /**
   * Pull out query object from request
   *
   * @param getFeatureType
   * @return
   */
  def getQueryType(getFeatureType: GetFeatureTypeV2): Option[QueryTypeV2] =
    getFeatureType.getAbstractQueryExpressionGroup.iterator()
        .find(_.getValue.isInstanceOf[QueryTypeV2])
        .map(_.getValue.asInstanceOf[QueryTypeV2])
}
