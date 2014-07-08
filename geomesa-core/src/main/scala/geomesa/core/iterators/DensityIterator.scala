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
import com.vividsolutions.jts.geom.{Coordinate, Point, Polygon}
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.geotools.GridSnap
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, PartialKey, Value, Range => ARange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder, ReferencedEnvelope}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Random

class DensityIterator extends SimpleFeatureFilteringIterator {

  import geomesa.core.iterators.DensityIterator.SparseMatrix

  var bbox: ReferencedEnvelope = null
  var curRange: ARange = null
  var result: SparseMatrix = HashBasedTable.create[Double, Double, Int]()
  var srcIter: SortedKeyValueIterator[Key, Value] = null
  var projectedSFT: SimpleFeatureType = null
  var featureBuilder: SimpleFeatureBuilder = null
  var snap: GridSnap = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: ju.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    bbox = JTS.toEnvelope(WKTUtils.read(options.get(DensityIterator.BBOX_KEY)))
    val (w, h) = DensityIterator.getBounds(options)
    snap = new GridSnap(bbox, w, h)
    projectedSFT =
      DataUtilities.createType(simpleFeatureType.getTypeName, "encodedraster:String,geom:Point:srid=4326")
    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(projectedSFT)
  }

  override def next() = {
    do {
      if (super.hasTop) {
        val geom = curFeature.getDefaultGeometry.asInstanceOf[Point]
        val coord = geom.getCoordinate
        val x = snap.x(snap.i(coord.x))
        val y = snap.y(snap.j(coord.y))
        val cur = Option(result.get(x, y)).getOrElse(0)
        result.put(x, y, cur + 1)
        super.next()
      }
    } while(nextKey != null && super.hasTop && !curRange.afterEndKey(topKey.followingKey(PartialKey.ROW)))
  }

  override def seek(range: ARange,
                    columnFamilies: ju.Collection[ByteSequence],
                    inclusive: Boolean): Unit = {
    curRange = range
    super.seek(range, columnFamilies, inclusive)
  }

  override def getTopValue = {
    featureBuilder.reset()
    featureBuilder.add(DensityIterator.encodeSparseMatrix(result))
    featureBuilder.add(curFeature.getDefaultGeometry)
    val feature = featureBuilder.buildFeature(Random.nextString(6))
    result.clear()

    featureEncoder.encode(feature)
  }
}

object DensityIterator {

  val BBOX_KEY = "geomesa.density.bbox"
  val BOUNDS_KEY = "geomesa.density.bounds"
  type SparseMatrix = HashBasedTable[Double, Double, Int]
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
    val builder =  AvroSimpleFeatureFactory.featureBuilder(densitySFT)
    val m = decodeSparseMatrix(sf.getAttribute("encodedraster").toString)
    m.rowMap().flatMap { case (lonIdx, col) =>
      col.map { case (latIdx, count) =>
        builder.reset()
        val pt = geomFactory.createPoint(new Coordinate(lonIdx, latIdx))
        builder.buildFeature(sf.getID, Array(count, pt).asInstanceOf[Array[AnyRef]])
      }
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
        os.writeInt(v)
      }
    }
    os.flush()
    Base64.encodeBase64URLSafeString(baos.toByteArray)
  }

  def decodeSparseMatrix(encoded: String): SparseMatrix = {
    val bytes = Base64.decodeBase64(encoded)
    val is = new DataInputStream(new ByteArrayInputStream(bytes))
    val table = HashBasedTable.create[Double, Double, Int]()
    while(is.available() > 0) {
      val rowIdx = is.readDouble()
      val colCount = is.readInt()
      (0 until colCount).foreach { _ =>
        val colIdx = is.readDouble()
        val v = is.readInt()
        table.put(rowIdx, colIdx, v)
      }
    }
    table
  }

}