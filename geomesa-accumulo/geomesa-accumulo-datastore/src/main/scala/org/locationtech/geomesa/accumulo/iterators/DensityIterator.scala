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


package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Map => JMap}

import com.google.common.collect._
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder, ReferencedEnvelope}
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.index.{IndexEntryDecoder, IndexSchema}
import org.locationtech.geomesa.accumulo.iterators.DensityIterator.{DENSITY_FEATURE_SFT_STRING, SparseMatrix}
import org.locationtech.geomesa.accumulo.iterators.FeatureAggregatingIterator._
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions.{RichSimpleFeature, toRichSimpleFeatureIterator}
import org.locationtech.geomesa.utils.geotools.{GridSnap, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class DensityIterator(other: DensityIterator, env: IteratorEnvironment)
  extends FeatureAggregatingIterator[DensityIteratorResult](other, env) {

  protected var decoder: IndexEntryDecoder = null

  var bbox: ReferencedEnvelope = null
  var snap: GridSnap = null

  projectedSFTDef = DENSITY_FEATURE_SFT_STRING

  def this() = this(null, null)

  override def initProjectedSFTDefClassSpecificVariables(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment): Unit = {
    bbox = JTS.toEnvelope(WKTUtils.read(options.get(DensityIterator.BBOX_KEY)))
    val (w, h) = DensityIterator.getBounds(options)
    snap = new GridSnap(bbox, w, h)

    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
    decoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)
  }

  override def handleKeyValue(resultO: Option[DensityIteratorResult],
                              topSourceKey: Key,
                              topSourceValue: Value): DensityIteratorResult = {
    val feature = originalDecoder.deserialize(topSourceValue.get)
    lazy val geoHashGeom = decoder.decode(topSourceKey).getDefaultGeometry.asInstanceOf[Geometry]
    val geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
    val result = resultO.getOrElse(DensityIteratorResult(geometry))  // result.result will get updated

    geometry match {
      case point: Point =>
        addResultPoint(result.densityGrid, point)

      case multiPoint: MultiPoint =>
        (0 until multiPoint.getNumGeometries).foreach {
          i => addResultPoint(result.densityGrid, multiPoint.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[Point])
        }

      case line: LineString =>
        handleLineString(result.densityGrid, line.intersection(geoHashGeom).asInstanceOf[LineString])

      case multiLineString: MultiLineString =>
        (0 until multiLineString.getNumGeometries).foreach {
          i => handleLineString(result.densityGrid, multiLineString.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[LineString])
        }

      case polygon: Polygon =>
        handlePolygon(result.densityGrid, polygon.intersection(geoHashGeom).asInstanceOf[Polygon])

      case multiPolygon: MultiPolygon =>
        (0 until multiPolygon.getNumGeometries).foreach {
          i => handlePolygon(result.densityGrid, multiPolygon.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[Polygon])
        }

      case someGeometry: Geometry =>
        addResultPoint(result.densityGrid, someGeometry.getCentroid)

      case _ => Nil
    }

    result.copy(geometry = geometry)
  }

  /** take in a line string and seed in points between each window of two points
    * take the set of the resulting points to remove duplicate endpoints */
  def handleLineString(result: SparseMatrix, inLine: LineString) = {
    inLine.getCoordinates.sliding(2).flatMap {
      case Array(p0, p1) =>
        snap.generateLineCoordSet(p0, p1)
    }.toSet[Coordinate].foreach(c => addResultCoordinate(result, c))
  }

  /** for a given polygon, take the centroid of each polygon from the BBOX coverage grid
    * if the given polygon contains the centroid then it is passed on to addResultPoint */
  def handlePolygon(result: SparseMatrix, inPolygon: Polygon) = {
    val grid = snap.generateCoverageGrid
    val featureIterator = grid.getFeatures.features
    featureIterator
      .filter{ f => inPolygon.intersects(f.polygon) }
      .foreach{ f => addResultPoint(result, f.polygon.getCentroid) }
  }

  /** calls addResultCoordinate on a given Point's coordinate */
  def addResultPoint(result: SparseMatrix, inPoint: Point) = addResultCoordinate(result, inPoint.getCoordinate)

  /** take a given Coordinate and add 1 to the result coordinate that it corresponds to via the snap grid */
  def addResultCoordinate(result: SparseMatrix, coord: Coordinate) = {
    // snap the point into a 'bin' of close points and increment the count for the bin
    val x = snap.x(snap.i(coord.x))
    val y = snap.y(snap.j(coord.y))
    val cur = Option(result.get(y, x)).getOrElse(0L)
    result.put(y, x, cur + 1L)
  }
}

object DensityIterator extends Logging {

  val BBOX_KEY = "geomesa.density.bbox"
  val BOUNDS_KEY = "geomesa.density.bounds"
  val ENCODED_RASTER_ATTRIBUTE = "encodedraster"
  val DENSITY_FEATURE_SFT_STRING = s"$ENCODED_RASTER_ATTRIBUTE:String,geom:Point:srid=4326"
  type SparseMatrix = HashBasedTable[Double, Double, Long]
  val densitySFT = SimpleFeatureTypes.createType("geomesadensity", "weight:Double,geom:Point:srid=4326")
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

  def getBounds(options: JMap[String, String]): (Int, Int) = {
    val Array(w, h) = options.get(BOUNDS_KEY).split(",").map(_.toInt)
    (w, h)
  }

  def expandFeature(sf: SimpleFeature): Iterable[SimpleFeature] = {
    val builder = ScalaSimpleFeatureFactory.featureBuilder(densitySFT)

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

case class DensityIteratorResult(geometry: Geometry,
                                 densityGrid: SparseMatrix = HashBasedTable.create[Double, Double, Long]()) extends Result {
  override def addToFeature(featureBuilder: SimpleFeatureBuilder): Unit = {
    featureBuilder.add(DensityIterator.encodeSparseMatrix(densityGrid))
    featureBuilder.add(geometry)
  }
}
