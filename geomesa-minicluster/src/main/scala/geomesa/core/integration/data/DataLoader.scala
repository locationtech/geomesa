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

package geomesa.core.integration.data

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import geomesa.core.data.AccumuloDataStore
import geomesa.feature.AvroSimpleFeatureFactory
import org.geotools.data.Transaction
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder

import scala.io.Source

class DataLoader(dataType: DataType) extends Logging {

  /**
   *
   * @param dataStore
   */
  def loadData(dataStore: AccumuloDataStore): Unit = {

    val writer = dataStore.getFeatureWriter(dataType.sftName, Transaction.AUTO_COMMIT)

    val featureBuilder = AvroSimpleFeatureFactory.featureBuilder(dataType.simpleFeatureType)

    val source = dataType.getSource

    var count = 0

    logger.debug("Loading features...")

    // drop header
    source.getLines().drop(1).foreach { line =>
      val values = line.split("\t")

      // id is always assumed to be first column
      val id = values(0)

      featureBuilder.reset()
      // add all the attributes except id
      featureBuilder.addAll(values.drop(1).asInstanceOf[Array[AnyRef]])

      val simpleFeature = featureBuilder.buildFeature(id)

      val next = writer.next

      // copy the features into the writer
      (0 to simpleFeature.getAttributeCount - 1).foreach(i => next.setAttribute(i, simpleFeature.getAttribute(i)))
      next.getIdentifier.asInstanceOf[FeatureIdImpl].setID(simpleFeature.getID)
      next.getUserData.put(Hints.USE_PROVIDED_FID, true.asInstanceOf[AnyRef])

      writer.write

      count = count + 1

      if (count % 10000 == 0) {
        logger.debug("loaded {} features", count.toString)
      }
    }

    writer.close()

    source.close()

    logger.debug("loaded {} features", count.toString)
  }
}
