/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.iterators



import java.util.{Map => jMap}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.{IndexSchema, Strategy}
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureDeserializers}
import org.locationtech.geomesa.utils.geotools.Conversions.{RichSimpleFeature, toRichSimpleFeatureIterator}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

/**
 * Iterator that expands the z3 density iterator by adding support for non-kryo serialization types and
 * non-point geoms.
 */
class DensityIterator extends Z3DensityIterator with Logging {

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(src, jOptions, env)
    val options = jOptions.asScala

    val encodingOpt = options.get(FEATURE_ENCODING).map(SerializationType.withName).getOrElse(DEFAULT_ENCODING)
    val deserializer = SimpleFeatureDeserializers(sft, encodingOpt)

    handleValue = if (sft.getGeometryDescriptor.getType.getBinding == classOf[Point]) {
      // optimized point method without a match for each feature
      () => {
        val feature = deserializer.deserialize(source.getTopValue.get)
        if (filter == null || filter.evaluate(feature)) {
          topKey = source.getTopKey
          writePointToResult(feature.getDefaultGeometry.asInstanceOf[Point], weightFn(feature))
        }
      }
    } else {
      // only required for non-point geoms
      val schemaEncoding = options(DEFAULT_SCHEMA_NAME)
      val indexDecoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)

      () => {
        val feature = deserializer.deserialize(source.getTopValue.get)
        if (filter == null || filter.evaluate(feature)) {
          topKey = source.getTopKey
          val weight = weightFn(feature)
          lazy val geohash = indexDecoder.decode(source.getTopKey).getDefaultGeometry.asInstanceOf[Geometry]
          feature.getDefaultGeometry match {
            case g: Point           => writePointToResult(g, weight)
            case g: MultiPoint      => writeMultiPoint(g, geohash, weight)
            case g: LineString      => writeLineString(g, geohash, weight)
            case g: MultiLineString => writeMultiLineString(g, geohash, weight)
            case g: Polygon         => writePolygon(g, geohash, weight)
            case g: MultiPolygon    => writeMultiPolygon(g, geohash, weight)
            case g: Geometry        => writePointToResult(g.getCentroid, weight)
          }
        }
      }
    }
  }

  def writeMultiPoint(geom: MultiPoint, geohash: Geometry, weight: Double): Unit = {
    (0 until geom.getNumGeometries).foreach { i =>
      val pt = geom.getGeometryN(i).intersection(geohash).asInstanceOf[Point]
      writePointToResult(pt, weight)
    }
  }

  /** take in a line string and seed in points between each window of two points
    * take the set of the resulting points to remove duplicate endpoints */
  def writeLineString(geom: LineString, geohash: Geometry, weight: Double): Unit = {
    geom.intersection(geohash).asInstanceOf[LineString].getCoordinates.sliding(2).flatMap {
      case Array(p0, p1) => gridSnap.generateLineCoordSet(p0, p1)
    }.toSet[Coordinate].foreach(c => writePointToResult(c, weight))
  }

  def writeMultiLineString(geom: MultiLineString, geohash: Geometry, weight: Double): Unit = {
    (0 until geom.getNumGeometries).foreach { i =>
      writeLineString(geom.getGeometryN(i).asInstanceOf[LineString], geohash, weight)
    }
  }

  /** for a given polygon, take the centroid of each polygon from the BBOX coverage grid
    * if the given polygon contains the centroid then it is passed on to addResultPoint */
  def writePolygon(geom: Polygon, geohash: Geometry, weight: Double): Unit = {
    val poly = geom.intersection(geohash).asInstanceOf[Polygon]
    val grid = gridSnap.generateCoverageGrid
    grid.getFeatures.features.foreach { f =>
      if (poly.intersects(f.polygon)) {
        writePointToResult(f.polygon.getCentroid, weight)
      }
    }
  }

  def writeMultiPolygon(geom: MultiPolygon, geohash: Geometry, weight: Double): Unit = {
    (0 until geom.getNumGeometries).foreach { i =>
      writePolygon(geom.getGeometryN(i).asInstanceOf[Polygon], geohash, weight)
    }
  }
}

object DensityIterator extends Logging {

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
    Z3DensityIterator.configure(is, sft, filter, envelope, gridWidth, gridHeight, weightAttribute)
  }
}
