/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.ingest.UpdateMetadataCommand.UpdateMetadataParams
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}


class UpdateMetadataCommand extends FsDataStoreCommand {
  override val name: String = "update-metadata"
  override val params = new UpdateMetadataParams

  override def execute(): Unit = {
    withDataStore(addMetadata)
  }

  def addMetadata(ds: FileSystemDataStore): Unit = {
    ds.storage.updateMetadata(params.featureName)
    val m = ds.storage.getMetadata(params.featureName)
    Command.user.info(s"Updated metadata. Found ${m.getNumPartitions} partitions and ${m.getNumStorageFiles} files")
  }
}

object UpdateMetadataCommand {
  @Parameters(commandDescription = "Generate the metadata file")
  class UpdateMetadataParams extends FsParams with RequiredTypeNameParam
}
