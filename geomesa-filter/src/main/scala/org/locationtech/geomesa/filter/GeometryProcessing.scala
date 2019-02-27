/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import java.util.Locale

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.spatial.BBOXImpl
import org.locationtech.geomesa.filter.FilterHelper.trimToWorld
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.GeometryUtils.distanceDegrees
import org.locationtech.jts.geom.{Geometry, GeometryCollection}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.spatial._
import org.opengis.filter.{Filter, FilterFactory2}

import scala.util.{Failure, Success}

/**
  * Process a geometry for querying
  */
trait GeometryProcessing {

  /**
    * Process a spatial filter
    *
    * @param op filter operation
    * @param sft simple feature type
    * @param factory filter factory
    * @return
    */
  def process(op: BinarySpatialOperator, sft: SimpleFeatureType, factory: FilterFactory2): Filter

  /**
    * Extract geometries from a filter, to use for querying
    *
    * @param op filter
    * @param attribute name of geometry attribute to extract
    * @return
    */
  def extract(op: BinarySpatialOperator, attribute: String): Seq[Geometry]
}

object GeometryProcessing extends GeometryProcessing with LazyLogging {

  private val SafeGeomString = "gm-safe"

  private val processor = FilterProperties.GeometryProcessing.get match {
    case p if p.equalsIgnoreCase("spatial4j")  => Spatial4jStrategy
    case p if p.equalsIgnoreCase("none") => NoneStrategy
    case p =>
      logger.warn(s"Invalid value for '${FilterProperties.GeometryProcessing.property}', using default (spatial4j): $p")
      Spatial4jStrategy
  }

  override def process(op: BinarySpatialOperator, sft: SimpleFeatureType, factory: FilterFactory2): Filter =
    processor.process(op, sft, factory)

  override def extract(op: BinarySpatialOperator, attribute: String): Seq[Geometry] =
    processor.extract(op, attribute)

  /**
    * Multiplier to convert to meters from a dwithin unit
    *
    * @param units units defined in a dwithin filter
    * @return
    */
  def metersMultiplier(units: String): Double = {
    if (units == null) { 1d } else {
      units.trim.toLowerCase(Locale.US) match {
        case "meters"         => 1d
        case "kilometers"     => 1000d
        case "feet"           => 0.3048
        case "statute miles"  => 1609.347
        case "nautical miles" => 1852d
        case _                => 1d // not part of ECQL spec...
      }
    }
  }

  /**
    * Expand a geometry collection into a seq of geometries
    *
    * @param geometry geometry
    * @return
    */
  private def flatten(geometry: Geometry): Seq[Geometry] = geometry match {
    case g: GeometryCollection => Seq.tabulate(g.getNumGeometries)(g.getGeometryN).flatMap(flatten)
    case _ => Seq(geometry)
  }

  private trait AbstractGeometryProcessing extends GeometryProcessing {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    protected def split(geom: Geometry, op: BinarySpatialOperator): Geometry

    override def process(op: BinarySpatialOperator, sft: SimpleFeatureType, factory: FilterFactory2): Filter = {
      val prop = org.locationtech.geomesa.filter.checkOrderUnsafe(op.getExpression1, op.getExpression2)
      val geom = prop.literal.evaluate(null, classOf[Geometry])
      if (geom.getUserData == SafeGeomString) {
        op // we've already visited this geom once
      } else {
        // check for null or empty attribute and replace with default geometry name
        val attribute = Option(prop.name).filterNot(_.isEmpty).orElse(Option(sft).map(_.getGeomField)).orNull
        // trim to world boundaries
        val trimmedGeom = trimToWorld(geom)
        if (trimmedGeom.isEmpty) {
          Filter.EXCLUDE
        } else {
          val property = factory.property(attribute)
          val filters = flatten(split(trimmedGeom, op)).map { geom =>
            // mark it as being visited
            geom.setUserData(SafeGeomString) // note: side effect
            val literal = factory.literal(geom)
            val (e1, e2) = if (prop.flipped) { (literal, property) } else { (property, literal) }
            op match {
              case _: Within     => factory.within(e1, e2)
              case _: Intersects => factory.intersects(e1, e2)
              case _: Overlaps   => factory.overlaps(e1, e2)
              case d: DWithin    => factory.dwithin(e1, e2, d.getDistance, d.getDistanceUnits)
              // use the direct constructor so that we preserve our geom user data
              case _: BBOX       => new BBOXImpl(e1, e2)
              case _: Contains   => factory.contains(e1, e2)
            }
          }
          orFilters(filters)(factory)
        }
      }
    }

    override def extract(op: BinarySpatialOperator, attribute: String): Seq[Geometry] = {
      val geometry = for {
        prop <- checkOrder(op.getExpression1, op.getExpression2)
        if prop.name == null || prop.name == attribute
        geom <- Option(prop.literal.evaluate(null, classOf[Geometry]))
      } yield {
        val buffered = op match {
          case f: DWithin => geom.buffer(distanceDegrees(geom, f.getDistance * metersMultiplier(f.getDistanceUnits))._2)
          case _          => geom
        }
        split(trimToWorld(buffered), op)
      }
      geometry.map(flatten).getOrElse(Seq.empty)
    }
  }

  private object Spatial4jStrategy extends AbstractGeometryProcessing {
    override protected def split(geom: Geometry, op: BinarySpatialOperator): Geometry = {
      // add waypoints if needed so that IDL is handled correctly
      val waypoints = if (op.isInstanceOf[BBOX]) { FilterHelper.addWayPointsToBBOX(geom) } else { geom }
      GeohashUtils.getInternationalDateLineSafeGeometry(waypoints) match {
        case Success(g) => g
        case Failure(e) => logger.warn(s"Error splitting geometry on AM for $waypoints", e); waypoints
      }
    }
  }

  private object NoneStrategy extends AbstractGeometryProcessing {
    override protected def split(geom: Geometry, op: BinarySpatialOperator): Geometry = geom
  }
}
