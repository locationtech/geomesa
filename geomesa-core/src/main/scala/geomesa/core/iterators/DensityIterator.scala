/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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


package geomesa.core.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.{util => ju}

import com.google.common.collect._
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Coordinate, Point, Polygon}
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import geomesa.utils.geotools.GridSnap
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => ARange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder, ReferencedEnvelope}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.{Failure, Random, Success, Try}

class DensityIterator extends SimpleFeatureFilteringIterator {

  import geomesa.core.iterators.DensityIterator.{DENSITY_FEATURE_STRING, SparseMatrix}

  var bbox: ReferencedEnvelope = null
  var curRange: ARange = null
  var result: SparseMatrix = HashBasedTable.create[Double, Double, Long]()
  var srcIter: SortedKeyValueIterator[Key, Value] = null
  var projectedSFT: SimpleFeatureType = null
  var featureBuilder: SimpleFeatureBuilder = null
  var snap: GridSnap = null
  var topDensityKey: Option[Key] = None
  var topDensityValue: Option[Value] = None

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: ju.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    bbox = JTS.toEnvelope(WKTUtils.read(options.get(DensityIterator.BBOX_KEY)))
    val (w, h) = DensityIterator.getBounds(options)
    snap = new GridSnap(bbox, w, h)
    projectedSFT = DataUtilities.createType(simpleFeatureType.getTypeName, DENSITY_FEATURE_STRING)
    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(projectedSFT)
  }

  /**
   * Repeatedly calls the SimpleFeatureFilteringIterators 'next' method and compiles the results
   * into a single feature
   */
  override def next() = {
    // reset our 'top' (current) variables
    result.clear()
    topDensityKey = None
    topDensityValue = None
    var geometry: Point = null
    var featureOption: Option[SimpleFeature] = Option(nextFeature)

    super.next()
    while(super.hasTop && !curRange.afterEndKey(topKey)) {
      topDensityKey = Some(topKey)
      val feature = featureOption.getOrElse(featureEncoder.decode(simpleFeatureType, topValue))
      geometry = feature.point
      val coord = geometry.getCoordinate
      // snap the point into a 'bin' of close points and increment the count for the bin
      val x = snap.x(snap.i(coord.x))
      val y = snap.y(snap.j(coord.y))
      val cur = Option(result.get(y, x)).getOrElse(0L)
      result.put(y, x, cur + 1L)
      // get the next feature that will be returned before calling super.next(), for the next loop
      // iteration, where it will the the feature corresponding to topValue
      featureOption = Option(nextFeature)
      super.next()
    }

    // if we found anything, set the current value
    topDensityKey match {
      case None =>
      case Some(k) =>
        featureBuilder.reset()
        // encode the bins into the feature - this will be expanded by the IndexSchema
        featureBuilder.add(DensityIterator.encodeSparseMatrix(result))
        featureBuilder.add(geometry)
        val feature = featureBuilder.buildFeature(Random.nextString(6))
        topDensityValue = Some(featureEncoder.encode(feature))
    }
  }

  override def seek(range: ARange,
                    columnFamilies: ju.Collection[ByteSequence],
                    inclusive: Boolean): Unit = {
    curRange = range
    super.seek(range, columnFamilies, inclusive)
  }

  override def hasTop: Boolean = topDensityKey.nonEmpty

  override def getTopKey: Key = topDensityKey.getOrElse(null)

  override def getTopValue = topDensityValue.getOrElse(null)
}

object DensityIterator extends Logging {

  val BBOX_KEY = "geomesa.density.bbox"
  val BOUNDS_KEY = "geomesa.density.bounds"
  val ENCODED_RASTER_ATTRIBUTE = "encodedraster"
  val DENSITY_FEATURE_STRING = s"$ENCODED_RASTER_ATTRIBUTE:String,geom:Point:srid=4326"
  type SparseMatrix = HashBasedTable[Double, Double, Long]
  val densitySFT = DataUtilities.createType("geomesadensity", "weight:Double,geom:Point:srid=4326")
  val geomFactory = JTSFactoryFinder.getGeometryFactory

  def configure(cfg: IteratorSetting, polygon: Polygon, w: Int, h: Int) = {
    setBbox(cfg, polygon)
    setBounds(cfg, w, h)
  }
  
  def setBbox(iterSettings: IteratorSetting, poly: Polygon): Unit = {
    iterSettings.addOption(BBOX_KEY, WKTUtils.write(poly))
  }

  def setBounds(iterSettings: IteratorSetting, width: Int, height: Int): Unit = {
    iterSettings.addOption(BOUNDS_KEY, s"$width,$height")
  }

  def getBounds(options: ju.Map[String, String]): (Int, Int) = {
    val Array(w, h) = options.get(BOUNDS_KEY).split(",").map(_.toInt)
    (w, h)
  }

  def expandFeature(sf: SimpleFeature): Iterable[SimpleFeature] = {
    val builder = AvroSimpleFeatureFactory.featureBuilder(densitySFT)

    val decodedMap = Try(decodeSparseMatrix(sf.getAttribute(ENCODED_RASTER_ATTRIBUTE).toString))

    decodedMap match {
      case Success(raster) =>
        raster.rowMap().flatMap { case (latIdx, col) =>
          col.map { case (lonIdx, count) =>
            builder.reset()
            val pt = geomFactory.createPoint(new Coordinate(lonIdx, latIdx))
            builder.buildFeature(sf.getID, Array(count, pt).asInstanceOf[Array[AnyRef]])
          }
        }
      case Failure(e) =>
        logger.error(s"Error expanding encoded raster ${sf.getAttribute(ENCODED_RASTER_ATTRIBUTE)}: ${e.toString}", e)
        List(builder.buildFeature(sf.getID, Array(1, sf.point).asInstanceOf[Array[AnyRef]]))
    }
  }

  def encodeSparseMatrix(sparseMatrix: SparseMatrix): String = {
    val baos = new ByteArrayOutputStream()
    val os = new DataOutputStream(baos)
    sparseMatrix.rowMap().foreach { case (rowIdx, cols) =>
      os.writeDouble(rowIdx)
      os.writeInt(cols.size())
      cols.foreach { case (colIdx, v) =>
        os.writeDouble(colIdx)
        os.writeLong(v)
      }
    }
    os.flush()
    Base64.encodeBase64URLSafeString(baos.toByteArray)
  }

  def decodeSparseMatrix(encoded: String): SparseMatrix = {
    val bytes = Base64.decodeBase64(encoded)
    val is = new DataInputStream(new ByteArrayInputStream(bytes))
    val table = HashBasedTable.create[Double, Double, Long]()
    while(is.available() > 0) {
      val rowIdx = is.readDouble()
      val colCount = is.readInt()
      (0 until colCount).foreach { _ =>
        val colIdx = is.readDouble()
        val v = is.readLong()
        table.put(rowIdx, colIdx, v)
      }
    }
    table
  }

}