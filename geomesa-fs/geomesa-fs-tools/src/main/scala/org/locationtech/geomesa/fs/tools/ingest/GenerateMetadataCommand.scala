/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}


class GenerateMetadataCommand extends FsDataStoreCommand {
  override val name: String = "gen-metadata"
  override val params = new GenMetaCommands

  override def execute(): Unit = {
    withDataStore(addMetadata)
  }

  def addMetadata(ds: FileSystemDataStore): Unit = {
    val partitions = ds.storage.getMetadata(params.featureName).getPartitions
    Command.user.info(s"Generated metadata file with ${partitions.size} partitions")
  }
}

@Parameters(commandDescription = "Generate the metadata file")
class GenMetaCommands extends FsParams with RequiredTypeNameParam
