/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.data

import java.io.IOException

import com.beust.jcommander.Parameter
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.CreateSchemaParams
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

trait CreateSchemaCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  override val name = "create-schema"
  override def params: CreateSchemaParams

  override def execute(): Unit = {
    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    Option(params.dtgField).foreach(sft.setDtgField)
    // note: this will pass through to the datastore, log a warning and then be ignored
    Option(params.useSharedTables).foreach(s => sft.setTableSharing(s.booleanValue()))
    setBackendSpecificOptions(sft)
    withDataStore(createSchema(_, sft))
  }

  protected def setBackendSpecificOptions(featureType: SimpleFeatureType): Unit = {}

  protected def createSchema(ds: DS, sft: SimpleFeatureType): Unit = {
    lazy val sftString = SimpleFeatureTypes.encodeType(sft)
    Command.user.info(s"Creating '${sft.getTypeName}' with spec '$sftString'. Just a few moments...")

    if (try { ds.getSchema(sft.getTypeName) == null } catch { case _: IOException => true }) {
      ds.createSchema(sft)
      if (try { ds.getSchema(sft.getTypeName) != null } catch { case _: IOException => false }) {
        Command.user.info(s"Created schema '${sft.getTypeName}'")
      } else {
        Command.user.error(s"Could not create schema '${sft.getTypeName}'")
      }
    } else {
      Command.user.error(s"Schema '${sft.getTypeName}' already exists in the data store")
    }
  }
}

object CreateSchemaCommand {
  // @Parameters(commandDescription = "Create a GeoMesa feature type")
  trait CreateSchemaParams extends RequiredFeatureSpecParam with OptionalTypeNameParam with OptionalDtgParam {
    @deprecated("shared tables no longer supported")
    @Parameter(names = Array("--use-shared-tables"), description = "Use shared tables for feature storage (deprecated)", arity = 1)
    var useSharedTables: java.lang.Boolean = _
  }
}
