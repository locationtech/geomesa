/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import org.geotools.referencing.GeodeticCalculator
import org.locationtech.geomesa.filter.GeometryProcessing
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.operation.distance.DistanceOp
import org.opengis.filter.FilterVisitor
import org.opengis.filter.MultiValuedFilter.MatchAction
import org.opengis.filter.expression.{Expression, Literal}
import org.opengis.filter.spatial.DWithin

object FastDWithin {

  private val calculators = new ThreadLocal[GeodeticCalculator] {
    override def initialValue: GeodeticCalculator = new GeodeticCalculator()
  }

  /**
    * DWithin a literal geometry. Evaluates precise distance using a geodetic calculator, if necessary
    *
    * @param exp1 expression 1
    * @param exp2 expression 2
    * @param distance distance
    * @param units distance units
    */
  class DWithinLiteral(exp1: Expression, exp2: Literal, distance: Double, units: String) extends DWithin {
    private val geometry = exp2.evaluate(null).asInstanceOf[Geometry]
    private val envelope = geometry.getEnvelopeInternal
    private val meters = distance * GeometryProcessing.metersMultiplier(units)
    private val (minDegrees, maxDegrees) = GeometryUtils.distanceDegrees(geometry, meters)

    override def evaluate(obj: AnyRef): Boolean = {
      val geom = exp1.evaluate(obj).asInstanceOf[Geometry]
      if (geom == null || envelope.distance(geom.getEnvelopeInternal) > maxDegrees) { false } else {
        val op = new DistanceOp(geometry, geom)
        op.distance <= minDegrees || {
          val Array(p1, p2) = op.nearestPoints()
          val calculator = calculators.get
          calculator.setStartingGeographicPoint(p1.x, p1.y)
          calculator.setDestinationGeographicPoint(p2.x, p2.y)
          calculator.getOrthodromicDistance <= meters
        }
      }
    }

    override def getExpression1: Expression = exp1
    override def getExpression2: Expression = exp2
    override def getDistance: Double = distance
    override def getDistanceUnits: String = units
    override def getMatchAction: MatchAction = MatchAction.ANY
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)

    override def toString: String = s"[ $exp1 dwithin $exp2 , distance: $distance $units ]"
  }
}
