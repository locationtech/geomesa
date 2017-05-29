/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File

import com.beust.jcommander.{ParameterException, Parameters}
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams}
import org.locationtech.geomesa.tools.ingest.{IngestCommand, IngestParams}

class FsIngestCommand extends IngestCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params = new FsIngestParams

  override val libjarsFile: String = ""

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator.empty

  override def execute(): Unit = {
    import scala.collection.JavaConversions._
    if (params.files.exists(IngestCommand.isDistributedUrl)) {
      throw new ParameterException(s"Only local ingestion supported: ${params.files}")
    }
    super.execute()
  }
}

@Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
class FsIngestParams extends IngestParams with FsParams
