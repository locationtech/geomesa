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

package org.locationtech.geomesa.plugin.process

import java.util.UUID

import org.geotools.data.DataUtilities
import org.geotools.geometry.jts.GeometryBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class DBSCANProcessTest extends Specification {

  "DBSCANProcess" should {
    val sft = SimpleFeatureTypes.createType("input", "geom:Point:srid=4326")
    val geomBuilder = new GeometryBuilder
    
    // cluster 1
    val c1 = Seq((10, 10), (11, 11), (10, 11), (11, 10))

    // cluster 2
    val c2 = Seq((70, 70), (71, 71), (70, 71), (71, 70))

    val sf = (c1 ++ c2).map { case (x, y) =>
      val pt = geomBuilder.point(x, y)
      val id = UUID.randomUUID().toString
      val f = AvroSimpleFeatureFactory.buildAvroFeature(sft, Seq(), id)
      f.setDefaultGeometry(pt)
      f
    }

    val featureColl = DataUtilities.collection(sf)
    
    "cluster simple features" >> {
      val proc = new DBSCANProcess
      val procresults = proc.execute(featureColl, 200000, 3)
      val clusters = procresults.features().toList
      "into two clusters" >> { clusters.length must beEqualTo(2) }
      "with non-zero radius" >> { clusters.head.getAttribute("radius").asInstanceOf[Double] must be greaterThan 0.0 }
    }
  }
}
