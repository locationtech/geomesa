/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.features.kryo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.kryo.serialization.KryoGeometrySerializer
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.languageFeature.postfixOps

@RunWith(classOf[JUnitRunner])
class KryoGeometrySerializerTest extends Specification {

  "KryoGeometrySerializer" should {

    "correctly serialize and deserialize different geometries" in {
      val geoms = Seq(
        "LINESTRING(0 2, 2 0, 8 6)",
        "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))",
        "MULTIPOINT(0 0, 2 2)",
        "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))",
        "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))",
        "MULTIPOINT(0 0, 2 2)",
        "POINT(55.0 49.0)"
      ).map(WKTUtils.read)

      "using byte arrays" >> {
        geoms.foreach { geom =>
          val serialized = KryoGeometrySerializer.write(geom)
          val deserialized = KryoGeometrySerializer.read(serialized)
          deserialized mustEqual geom
        }
        success
      }
      "using streams" >> {
        geoms.foreach { geom =>
          val out = new ByteArrayOutputStream()
          KryoGeometrySerializer.write(geom, out)
          val in = new ByteArrayInputStream(out.toByteArray)
          val deserialized = KryoGeometrySerializer.read(in)
          deserialized mustEqual geom
        }
        success
      }
    }
  }
}
