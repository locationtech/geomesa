/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.tools.kafka

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataStore
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper
import org.locationtech.geomesa.tools.accumulo.CLArgResolver
import org.locationtech.geomesa.tools.kafka.commands.CreateFeatureParams
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

object FeatureCreator extends LazyLogging {

  def createFeatureType(params: CreateFeatureParams): Unit = {
    val ds = new DataStoreHelper(params).getDataStore()
    createFeatureType(
      ds,
      CLArgResolver.getSft(params.spec, params.featureName),
      params.featureName,
      params.zkPath,
      Option(params.dtgField))
  }

  def createFeatureType(ds: DataStore,
                        sft: SimpleFeatureType,
                        featureName: String,
                        zkPath: String,
                        dtField: Option[String]): Unit = {
    val sftString = SimpleFeatureTypes.encodeType(sft)
    logger.info(s"Creating '$featureName' using a KafkaDataStore with spec " +
      s"'$sftString'. Just a few moments...")

    val streamingSFT = KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
    if (dtField.orNull != null) {
      sft.setDtgField(dtField.getOrElse(Constants.SF_PROPERTY_START_TIME))
    }

    try {
      logger.info("Creating GeoMesa Kafka schema...")
      ds.createSchema(streamingSFT)

      if (ds.getSchema(featureName) != null) {
        logger.info(s"Feature '$featureName' with spec " +
          s"'$sftString' successfully created in Kafka with zkPath '$zkPath'.")
        println(s"Created feature $featureName")
      } else {
        logger.error(s"There was an error creating feature '$featureName' with spec " +
          s"'$sftString'. Please check that all arguments are correct " +
          "in the previous command.")
      }
    } catch {
      // if the schema already exists at the specified zkPath
      case e: IllegalArgumentException => {
        logger.warn(e.getMessage)
      }
    }
  }
}
