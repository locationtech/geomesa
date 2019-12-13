/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geometry

import org.locationtech.jts.geom.Polygon
import org.geotools.geometry.jts.JTS

@deprecated
object Geometry {

  @deprecated
  val noPolygon : Polygon  = null

  @deprecated
  implicit class RichPolygon(self: Polygon) {
    def getSafeUnion(other: Polygon): Polygon = {
      if (self != noPolygon && other != noPolygon) {
        if (self.overlaps(other)) {
          val p = self.union(other)
          p.normalize()
          p.asInstanceOf[Polygon]
        } else {
          // they don't overlap; take the merge of their envelopes
          // (since we don't support MultiPolygon returns yet)
          val env = self.getEnvelopeInternal
          env.expandToInclude(other.getEnvelopeInternal)
          JTS.toGeometry(env)
        }
      } else noPolygon
    }

    def getSafeIntersection(other: Polygon): Polygon =
      if (self == noPolygon) other
      else if (other == noPolygon) self
      else if (self.intersects(other)) {
        val p = self.intersection(other)
        p.normalize()
        p match {
          case poly: Polygon => poly
          case _ => noPolygon
        }
      } else noPolygon
  }
}
