/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.tools.commands.{CreateFeatureParams, GeoMesaParams}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

object FeatureCreator extends Logging {

  def createFeature(params: CreateFeatureParams, convert: String = null): Unit = {
    val ds = new DataStoreHelper(params).getOrCreateDs()
    createFeature(
      ds,
      SftArgParser.getSft(params.spec, params.featureName, convert),
      params.featureName,
      Option(params.dtgField),
      Option(params.useSharedTables),
      params.catalog)
  }


  def createFeature(params: GeoMesaParams, sft: SimpleFeatureType): Unit =
    new DataStoreHelper(params).getOrCreateDs().createSchema(sft)

  def createFeature(ds: AccumuloDataStore,
                    sft: SimpleFeatureType,
                    featureName: String,
                    dtField: Option[String],
                    sharedTable: Option[Boolean],
                    catalog: String): Unit = {
    val sftString = SimpleFeatureTypes.encodeType(sft)
    logger.info(s"Creating '$featureName' on catalog table '$catalog' with spec " +
      s"'$sftString'. Just a few moments...")

    if (ds.getSchema(featureName) == null) {

      logger.info("Creating GeoMesa tables...")
      if (dtField.orNull != null) {
        // Todo: fix logic here, it is a bit strange
        sft.setDtgField(dtField.getOrElse(Constants.SF_PROPERTY_START_TIME))
      }

      sharedTable.foreach(sft.setTableSharing)

      ds.createSchema(sft)

      if (ds.getSchema(featureName) != null) {
        logger.info(s"Feature '$featureName' on catalog table '$catalog' with spec " +
          s"'$sftString' successfully created.")
        println(s"Created feature $featureName")
      } else {
        logger.error(s"There was an error creating feature '$featureName' on catalog table " +
          s"'$catalog' with spec '$sftString'. Please check that all arguments are correct " +
          "in the previous command.")
      }
    } else {
      logger.error(s"A feature named '$featureName' already exists in the data store with " +
        s"catalog table '$catalog'.")
    }
    
  }

}
