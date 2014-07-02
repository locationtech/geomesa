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

package geomesa.utils.geometry

import com.vividsolutions.jts.geom.Polygon
import org.geotools.geometry.jts.JTS

object Geometry {
  val noPolygon : Polygon  = null

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
