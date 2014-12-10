/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.tools

import java.io.File

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.tools.ShpIngest._
import org.locationtech.geomesa.tools.commands.IngestCommand.IngestParameters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

class ShpIngest(params: IngestParameters) extends Logging {

  /**
   * @return true or false indicating the success of the ingest
   */
  def run() = {
    val fileUrl = new File(params.files(0)).toURI.toURL
    val shpParams = Map(ShapefileDataStoreFactory.URLP.getName -> fileUrl)
    val shpDataStore = DataStoreFinder.getDataStore(shpParams)
    val featureTypeName = shpDataStore.getTypeNames.head
    val featureSource = shpDataStore.getFeatureSource(featureTypeName)

    val ds = new DataStoreHelper(params).ds

    val targetTypeName = if (params.featureName != null) params.featureName else featureTypeName

    if(ds.getSchema(targetTypeName) != null) {
      logger.error("Type name already exists")
      false
    }
    else {
      // create the new feature type
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(featureSource.getSchema)
      builder.setName(targetTypeName)
      val targetType = builder.buildFeatureType()

      ds.createSchema(targetType)
      val writer = ds.getFeatureWriterAppend(targetTypeName, Transaction.AUTO_COMMIT)
      featureSource.getFeatures.features.foreach { f =>
        val toWrite = writer.next()
        copyFeature(f, toWrite)
        writer.write()
      }
      writer.close()
      true
    }
  }

}

object ShpIngest {

  // todo copy Hints if necessary? GEOMESA-534
  def copyFeature(from: SimpleFeature, to: SimpleFeature): Unit = {
    from.getAttributes.zipWithIndex.foreach { case (attr, idx) => to.setAttribute(idx, attr) }
    to.setDefaultGeometry(from.getDefaultGeometry)
    to.getIdentifier.asInstanceOf[FeatureIdImpl].setID(from.getID)
  }
}
