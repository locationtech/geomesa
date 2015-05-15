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

import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.tools.commands.CreateFeatureParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

object FeatureCreator extends Logging {

  def createFeature(params: CreateFeatureParams): Unit = {
    val ds = new DataStoreHelper(params).getOrCreateDs
    createFeature(ds, params)
  }

  def createFeature(ds: AccumuloDataStore, params: CreateFeatureParams): Unit =
    createFeature(
      ds,
      params.spec,
      params.featureName,
      Option(params.dtgField),
      Option(params.useSharedTables),
      params.catalog,
      Option(params.numShards))

  def createFeature(ds: AccumuloDataStore,
                    sftspec: String,
                    featureName: String,
                    dtField: Option[String],
                    sharedTable: Option[Boolean],
                    catalog: String,
                    maxShards: Option[Integer] = None): Unit = {
    logger.info(s"Creating '$featureName' on catalog table '$catalog' with spec " +
      s"'$sftspec'. Just a few moments...")

    if (ds.getSchema(featureName) == null) {

      logger.info("Creating GeoMesa tables...")

      val sft = SimpleFeatureTypes.createType(featureName, sftspec)
      if (dtField.orNull != null) {
        // Todo: fix logic here, it is a bit strange
        sft.getUserData.put(SF_PROPERTY_START_TIME, dtField.getOrElse(Constants.SF_PROPERTY_START_TIME))
      }

      sharedTable.foreach { org.locationtech.geomesa.accumulo.index.setTableSharing(sft, _) }

      if (maxShards.isDefined) {
        ds.createSchema(sft, maxShards.get)
      } else {
        ds.createSchema(sft)
      }

      if (ds.getSchema(featureName) != null) {
        logger.info(s"Feature '$featureName' on catalog table '$catalog' with spec " +
          s"'$sftspec' successfully created.")
        println(s"Created feature $featureName")
      } else {
        logger.error(s"There was an error creating feature '$featureName' on catalog table " +
          s"'$catalog' with spec '$sftspec'. Please check that all arguments are correct " +
          "in the previous command.")
      }
    } else {
      logger.error(s"A feature named '$featureName' already exists in the data store with " +
        s"catalog table '$catalog'.")
    }
    
  }

}
