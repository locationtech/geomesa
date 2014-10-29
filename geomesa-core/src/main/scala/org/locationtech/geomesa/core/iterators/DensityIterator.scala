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


package org.locationtech.geomesa.core.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.{util => ju}

import com.google.common.collect._
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => ARange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder, ReferencedEnvelope}
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.{FeatureEncoding, SimpleFeatureDecoder, SimpleFeatureEncoder}
import org.locationtech.geomesa.core.index.{IndexEntryDecoder, IndexSchema}
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions.{RichSimpleFeature, toRichSimpleFeatureIterator}
import org.locationtech.geomesa.utils.geotools.{GridSnap, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.{Failure, Random, Success, Try}

class DensityIterator(other: DensityIterator, env: IteratorEnvironment) extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.core.iterators.DensityIterator.{DENSITY_FEATURE_STRING, SparseMatrix}

  var bbox: ReferencedEnvelope = null
  var curRange: ARange = null
  var result: SparseMatrix = HashBasedTable.create[Double, Double, Long]()
  var projectedSFT: SimpleFeatureType = null
  var featureBuilder: SimpleFeatureBuilder = null
  var snap: GridSnap = null
  var topDensityKey: Option[Key] = None
  var topDensityValue: Option[Value] = None
  protected var decoder: IndexEntryDecoder = null

  var simpleFeatureType: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key,Value] = null

  var topSourceKey: Key = null
  var topSourceValue: Value = null
  var originalDecoder: SimpleFeatureDecoder = null
  var densityFeatureEncoder: SimpleFeatureEncoder = null

  if (other != null && env != null) {
    source = other.source.deepCopy(env)
    simpleFeatureType = other.simpleFeatureType
  }

  def this() = this(null, null)

  def init(source: SortedKeyValueIterator[Key, Value],
                    options: ju.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = source

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
    simpleFeatureType = SimpleFeatureTypes.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)
    simpleFeatureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    originalDecoder = SimpleFeatureDecoder(simpleFeatureType, encodingOpt)

    bbox = JTS.toEnvelope(WKTUtils.read(options.get(DensityIterator.BBOX_KEY)))
    val (w, h) = DensityIterator.getBounds(options)
    snap = new GridSnap(bbox, w, h)
    projectedSFT = SimpleFeatureTypes.createType(simpleFeatureType.getTypeName, DENSITY_FEATURE_STRING)

    // Use density SFT for the encoder since we are transforming the feature into
    // a sparse matrix as the result type of this iterator
    densityFeatureEncoder = SimpleFeatureEncoder(projectedSFT, encodingOpt)
    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(projectedSFT)
    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
    decoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)
  }

  /**
   * Combines the results from the underlying iterator stack
   * into a single feature
   */
  def findTop() = {
    // reset our 'top' (current) variables
    result.clear()
    topSourceKey = null
    topSourceValue = null
    var geometry: Geometry = null

    while(source.hasTop && !curRange.afterEndKey(source.getTopKey)) {
      topSourceKey = source.getTopKey
      topSourceValue = source.getTopValue

      val feature = originalDecoder.decode(topSourceValue)
      lazy val geoHashGeom = decoder.decode(topSourceKey).getDefaultGeometry.asInstanceOf[Geometry]
      geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
      geometry match {
        case point: Point =>
          addResultPoint(point)

        case multiPoint: MultiPoint =>
          (0 until multiPoint.getNumGeometries).foreach {
            i => addResultPoint(multiPoint.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[Point])
          }

        case line: LineString =>
          handleLineString(line.intersection(geoHashGeom).asInstanceOf[LineString])

        case multiLineString: MultiLineString =>
          (0 until multiLineString.getNumGeometries).foreach {
           i => handleLineString(multiLineString.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[LineString])
          }

        case polygon: Polygon =>
          handlePolygon(polygon.intersection(geoHashGeom).asInstanceOf[Polygon])

        case multiPolygon: MultiPolygon =>
          (0 until multiPolygon.getNumGeometries).foreach {
            i => handlePolygon(multiPolygon.getGeometryN(i).intersection(geoHashGeom).asInstanceOf[Polygon])
          }

        case someGeometry: Geometry =>
          addResultPoint(someGeometry.getCentroid)

        case _ => Nil

      }

      // Advance the source iterator
      source.next()
    }

    // if we found anything, set the current value
    if(topSourceKey != null) {
      featureBuilder.reset()
      // encode the bins into the feature - this will be expanded by the IndexSchema
      featureBuilder.add(DensityIterator.encodeSparseMatrix(result))
      featureBuilder.add(geometry)
      val feature = featureBuilder.buildFeature(Random.nextString(6))
      topDensityKey = Some(topSourceKey)
      topDensityValue = Some(new Value(densityFeatureEncoder.encode(feature)))
    }
  }

    /** take in a line string and seed in points between each window of two points
      * take the set of the resulting points to remove duplicate endpoints */
  def handleLineString(inLine: LineString) = {
    inLine.getCoordinates.sliding(2).flatMap {
      case Array(p0, p1) =>
        snap.generateLineCoordSet(p0, p1)
    }.toSet[Coordinate].foreach(c => addResultCoordinate(c))
  }

  /** for a given polygon, take the centroid of each polygon from the BBOX coverage grid
    * if the given polygon contains the centroid then it is passed on to addResultPoint */
  def handlePolygon(inPolygon: Polygon) = {
    val grid = snap.generateCoverageGrid
    val featureIterator = grid.getFeatures.features
    featureIterator
      .filter{ f => inPolygon.intersects(f.polygon) }
      .foreach{ f=> addResultPoint(f.polygon.getCentroid) }
  }

  /** calls addResultCoordinate on a given Point's coordinate */
  def addResultPoint(inPoint: Point) = addResultCoordinate(inPoint.getCoordinate)

  /** take a given Coordinate and add 1 to the result coordinate that it corresponds to via the snap grid */
  def addResultCoordinate(coord: Coordinate) = {
    // snap the point into a 'bin' of close points and increment the count for the bin
    val x = snap.x(snap.i(coord.x))
    val y = snap.y(snap.j(coord.y))
    val cur = Option(result.get(y, x)).getOrElse(0L)
    result.put(y, x, cur + 1L)
  }

  override def seek(range: ARange,
                    columnFamilies: ju.Collection[ByteSequence],
                    inclusive: Boolean): Unit = {
    curRange = range
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  def hasTop: Boolean = topDensityKey.nonEmpty

  def getTopKey: Key = topDensityKey.orNull

  def getTopValue = topDensityValue.orNull

  def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = new DensityIterator(this, env)

  def next(): Unit = if(!source.hasTop) {
    topDensityKey = None
    topDensityValue = None
  } else {
    findTop()
  }
}

object DensityIterator extends Logging {

  val BBOX_KEY = "geomesa.density.bbox"
  val BOUNDS_KEY = "geomesa.density.bounds"
  val ENCODED_RASTER_ATTRIBUTE = "encodedraster"
  val DENSITY_FEATURE_STRING = s"$ENCODED_RASTER_ATTRIBUTE:String,geom:Point:srid=4326"
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
