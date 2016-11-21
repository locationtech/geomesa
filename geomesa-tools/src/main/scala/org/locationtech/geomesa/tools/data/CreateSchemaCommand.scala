/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.data

import org.geotools.data.DataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

trait CreateSchemaCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  override val name = "create-schema"
  override def params: CreateSchemaParams

  override def execute() = {
    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    Option(params.dtgField).foreach(sft.setDtgField)
    Option(params.useSharedTables).foreach(sft.setTableSharing)
    withDataStore(createSchema(_, sft))
  }

  protected def createSchema(ds: DS, sft: SimpleFeatureType): Unit = {
    lazy val sftString = SimpleFeatureTypes.encodeType(sft)
    logger.info(s"Creating '${params.featureName}' with spec '$sftString'. Just a few moments...")

    if (!ds.getTypeNames.contains(sft.getTypeName)) {
      ds.createSchema(sft)
      if (ds.getSchema(sft.getTypeName) != null) {
        logger.info(s"Created schema '${sft.getTypeName}'")
        println(s"Created schema ${sft.getTypeName}")
      } else {
        logger.error(s"Could not create schema '${sft.getTypeName}'")
      }
    } else {
      logger.error(s"Schema '${sft.getTypeName}' already exists in the data store")
    }
  }
}

// @Parameters(commandDescription = "Create a GeoMesa feature type")
trait CreateSchemaParams
    extends RequiredFeatureSpecParam with RequiredTypeNameParam with OptionalDtgParam with OptionalSharedTablesParam
