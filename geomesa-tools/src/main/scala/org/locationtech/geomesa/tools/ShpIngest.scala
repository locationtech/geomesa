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
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object ShpIngest extends Logging {
  
  def doIngest(config: IngestArguments, dsConf: Map[String, _]): Boolean = {
    val fileUrl = new File(config.file).toURI.toURL
    val params = Map(ShapefileDataStoreFactory.URLP.getName -> fileUrl)
    val shpDataStore = DataStoreFinder.getDataStore(params)
    val featureTypeName = shpDataStore.getTypeNames.head
    val feature = shpDataStore.getFeatureSource(featureTypeName)

    val ds = DataStoreFinder.getDataStore(dsConf)

    val targetTypeName =
      if(config.featureName != null) config.featureName
      else featureTypeName

    if(ds.getSchema(targetTypeName) != null) {
      logger.error("Type name already exists")
      false
    }
    else {
      // create the new feature type
      val sftb = new SimpleFeatureTypeBuilder()
      sftb.init(feature.getSchema)
      sftb.setName(targetTypeName)
      val targetType = sftb.buildFeatureType()

      ds.createSchema(targetType)
      val writer = ds.getFeatureWriterAppend(targetTypeName, Transaction.AUTO_COMMIT)
      feature.getFeatures.features().foreach { f =>
        val toWrite = writer.next()
        copyFeature(f, toWrite)
        writer.write()
      }
      writer.close()
      true
    }
  }

  def copyFeature(from: SimpleFeature, to: SimpleFeature): Unit = {
    from.getAttributes.zipWithIndex.foreach { case (attr, idx) => to.setAttribute(idx, attr) }
    to.setDefaultGeometry(from.getDefaultGeometry)
    to.getIdentifier.asInstanceOf[FeatureIdImpl].setID(from.getID)
  }
  
}
