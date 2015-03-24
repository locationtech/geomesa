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

package org.locationtech.geomesa.jobs.scalding

import com.twitter.scalding.{Hdfs, Read, Write}
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaSourceTest extends Specification {

  val sftName = "GeoMesaSourceTest"
  val params = Map(
    "instanceId" -> "GeoMesaSourceTest",
    "zookeepers" -> "zoo",
    "tableName"  -> "GeoMesaSourceTest",
    "user"       -> "user",
    "password"   -> "password",
    "useMock"    -> "true"
  )

  val input = GeoMesaInputOptions(params, sftName, None)
  val output = GeoMesaOutputOptions(params)

  "GeoMesaSource" should {
    "create read and write taps" in {
      implicit val mode = Hdfs(true, new Configuration())
      val readTap = GeoMesaSource(input).createTap(Read)
      val writeTap = GeoMesaSource(output).createTap(Write)
      readTap must haveClass[GeoMesaTap]
      writeTap must haveClass[GeoMesaTap]
      readTap.getIdentifier mustNotEqual(writeTap.getIdentifier)
    }
  }
}
