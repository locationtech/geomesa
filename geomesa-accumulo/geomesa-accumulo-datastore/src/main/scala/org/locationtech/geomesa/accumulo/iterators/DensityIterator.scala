/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.iterators



import java.util.{Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{IndexEntryDecoder, IndexSchema, Strategy}
import org.locationtech.geomesa.accumulo.iterators.KryoLazyDensityIterator.DensityResult
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureDeserializer, SimpleFeatureDeserializers}
import org.locationtech.geomesa.utils.geotools.Conversions.{RichSimpleFeature, toRichSimpleFeatureIterator}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

/**
 * Iterator that extends the kryo density iterator with support for non-kryo serialization types.
 */
class DensityIterator extends KryoLazyDensityIterator with LazyLogging {

  var deserializer: SimpleFeatureDeserializer = null
  var indexDecoder: IndexEntryDecoder = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(src, jOptions, env)
    val options = jOptions.asScala

    val encodingOpt = options.get(FEATURE_ENCODING).map(SerializationType.withName).getOrElse(DEFAULT_ENCODING)
    deserializer = SimpleFeatureDeserializers(sft, encodingOpt)

    // only required for non-point geoms
    val schemaEncoding = options(DEFAULT_SCHEMA_NAME)
    indexDecoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)
  }

  override def decode(value: Array[Byte]): SimpleFeature = deserializer.deserialize(value)

  override def writeNonPoint(geom: Geometry, weight: Double, result: DensityResult): Unit = {
    geom match {
      case g: MultiPoint => writeMultiPoint(g, weight, result)
      case g: LineString => writeLineString(g, weight, result)
      case g: Polygon    => writePolygon(g, weight, result)
      case _             => super.writeNonPoint(geom, weight, result)
    }
  }

  def writeMultiPoint(geom: MultiPoint, weight: Double, result: DensityResult): Unit = {
    val geohash = indexDecoder.decode(source.getTopKey).getDefaultGeometry.asInstanceOf[Geometry]
    (0 until geom.getNumGeometries).foreach { i =>
      val pt = geom.getGeometryN(i).intersection(geohash).asInstanceOf[Point]
      writePointToResult(pt, weight, result)
    }
  }

  /** take in a line string and seed in points between each window of two points
    * take the set of the resulting points to remove duplicate endpoints */
  def writeLineString(geom: LineString, weight: Double, result: DensityResult): Unit = {
    val geohash = indexDecoder.decode(source.getTopKey).getDefaultGeometry.asInstanceOf[Geometry]
    geom.intersection(geohash).asInstanceOf[LineString].getCoordinates.sliding(2).flatMap {
      case Array(p0, p1) => gridSnap.generateLineCoordSet(p0, p1)
    }.toSet[Coordinate].foreach(c => writePointToResult(c, weight, result))
  }

  def writePolygon(geom: Polygon, weight: Double, result: DensityResult): Unit = {
    val geohash = indexDecoder.decode(source.getTopKey).getDefaultGeometry.asInstanceOf[Geometry]
    val poly = geom.intersection(geohash).asInstanceOf[Polygon]
    val grid = gridSnap.generateCoverageGrid
    grid.getFeatures.features.foreach { f =>
      if (poly.intersects(f.polygon)) {
        writePointToResult(f.polygon.getCentroid, weight, result)
      }
    }
  }
}

object DensityIterator extends LazyLogging {

  /**
   * Creates an iterator config that expects entries to be precomputed bin values
   */
  def configure(sft: SimpleFeatureType,
                serializationType: SerializationType,
                schema: String,
                filter: Option[Filter],
                envelope: Envelope,
                gridWidth: Int,
                gridHeight: Int,
                weightAttribute: Option[String],
                priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "density-iter", classOf[DensityIterator])
    Strategy.configureFeatureEncoding(is, serializationType)
    is.addOption(DEFAULT_SCHEMA_NAME, schema)
    KryoLazyDensityIterator.configure(is, sft, filter, envelope, gridWidth, gridHeight, weightAttribute)
  }
}
