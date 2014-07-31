/*
 *
 *  * Copyright 2014 Commonwealth Computer Research, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the License);
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an AS IS BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package geomesa.core.integration

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.AccumuloFeatureStore
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import geomesa.utils.geotools.Conversions._

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class LiveMiniClusterTest extends Specification with Logging {

  sequential

  val sftName = "test"

  val recordsTable = sftName + "_" + sftName + "_records"

  val instance = "miniInstance"

  val zookeepers = "localhost:10099"

  val user = "root"

  val password = "password"

  val params = Map("instanceId" -> instance,
                    "zookeepers" -> zookeepers,
                    "user"       -> user,
                    "password"   -> password,
                    "tableName"  -> sftName)

  "MiniCluster" should {

    "query the data through connectors" in {

      skipped("Integration testing")

      val connector = new ZooKeeperInstance(instance, zookeepers).getConnector(user, new PasswordToken(password))

      logger.info("records:")
      connector.createScanner(recordsTable, new Authorizations).asScala.take(10).foreach { e =>
        logger.info(e.getKey.getRow + " " + e.getKey.getColumnFamily + " " + e.getKey.getColumnQualifier() + " " + e.getValue)
      }

      success
    }

    "query the data through geotools" in {

      skipped("Meant for integration testing")

      val dataStore = DataStoreFinder.getDataStore(params.asJava)

      val filter = CQL.toFilter("INCLUDE")

      val query = new Query(sftName, filter)

      // get the feature store used to query the GeoMesa data
      val featureStore = dataStore.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]

      val features = featureStore.getFeatures(query).features()

      features.foreach { sf =>
        logger.info("found " + sf.getID)
      }

      success
    }
  }
}
