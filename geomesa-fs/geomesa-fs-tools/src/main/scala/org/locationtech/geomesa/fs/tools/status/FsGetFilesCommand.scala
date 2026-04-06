/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.storage.api.StorageMetadata
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.status.FsGetFilesCommand.FSGetFilesParams
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

import java.time.Instant
import java.util.Locale

class FsGetFilesCommand extends FsDataStoreCommand {

  import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

  import scala.collection.JavaConverters._

  override val params = new FSGetFilesParams

  override val name: String = "get-files"

  override def execute(): Unit = withDataStore { ds =>
    val storage = ds.storage(params.featureName)
    val metadata = storage.metadata
    val files =
      if (params.partitions.isEmpty) {
        Command.user.info("Listing files for all partitions")
        metadata.getFiles()
      } else {
        Command.user.info(s"Listing files for partition(s): ${params.partitions.asScala.mkString(", ")}")
        params.partitions.asScala.flatMap(metadata.getFiles)
      }

    files.groupBy(_.partition).toSeq.sortBy(_._1.toString).foreach { case (p, files) =>
      Command.output.info(s"$p:")
      // sort by chronological order
      files.sorted(StorageMetadata.StorageFileOrdering.reverse).foreach { f =>
        Command.output.info(s"  ${f.file} ${f.action.toString.toUpperCase(Locale.US)} " +
          s"${GeoToolsDateFormat.format(Instant.ofEpochMilli(f.timestamp))} ${f.count} features")
      }
    }
  }
}

object FsGetFilesCommand {
  @Parameters(commandDescription = "List files for partitions")
  class FSGetFilesParams extends FsParams with RequiredTypeNameParam with PartitionParam
}
