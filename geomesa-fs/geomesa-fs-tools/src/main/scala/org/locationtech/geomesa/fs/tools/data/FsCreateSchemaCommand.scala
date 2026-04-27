/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

/***********************************************************************
  * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * https://www.apache.org/licenses/LICENSE-2.0
  ***********************************************************************/

package org.locationtech.geomesa.fs.tools.data

import com.beust.jcommander.{ParameterException, Parameters}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.core.StorageKeys
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, OptionalSchemeParams}
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand.FsCreateSchemaParams
import org.locationtech.geomesa.tools.data.CreateSchemaCommand
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.CreateSchemaParams

class FsCreateSchemaCommand extends CreateSchemaCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params: FsCreateSchemaParams = new FsCreateSchemaParams

  override protected def setBackendSpecificOptions(sft: SimpleFeatureType): Unit =
    FsCreateSchemaCommand.setOptions(sft, params)
}

object FsCreateSchemaCommand {

  import scala.collection.JavaConverters._

  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class FsCreateSchemaParams extends CreateSchemaParams with FsParams with OptionalSchemeParams

  def setOptions(sft: SimpleFeatureType, params: OptionalSchemeParams): Unit = {
    import org.locationtech.geomesa.fs.storage.core.RichSimpleFeatureType

    if (params.scheme == null || params.scheme.isEmpty) {
      if (sft.getUserData.get(StorageKeys.SchemeKey) == null) {
        throw new ParameterException("--partition-scheme is required")
      }
    } else {
      sft.setScheme(params.scheme)
    }

    if (params.targetFileSize != null) {
      sft.setTargetFileSize(params.targetFileSize)
    }

    // Can use this to set things like compression and summary levels for parquet in the sft user data
    // to be picked up by the ingest job
    params.storageOpts.asScala.foreach { case (k, v) => sft.getUserData.put(k,v) }
  }
}
