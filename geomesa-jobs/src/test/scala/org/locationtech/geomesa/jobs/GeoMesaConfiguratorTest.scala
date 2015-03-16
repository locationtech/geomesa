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

package org.locationtech.geomesa.jobs

import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaConfiguratorTest extends Specification {

  "GeoMesaConfigurator" should {
    "set and retrieve data store params" in {
      val in  = Map("user" -> "myuser", "password" -> "mypassword", "instance" -> "myinstance")
      val out = Map("user" -> "myuser2", "password" -> "mypassword2", "instance" -> "myinstance2")
      val conf = new Configuration()
      GeoMesaConfigurator.setDataStoreInParams(conf, in)
      GeoMesaConfigurator.setDataStoreOutParams(conf, out)
      val recoveredIn = GeoMesaConfigurator.getDataStoreInParams(conf)
      val recoveredOut = GeoMesaConfigurator.getDataStoreOutParams(conf)
      recoveredIn mustEqual(in)
      recoveredOut mustEqual(out)
    }
  }

}
