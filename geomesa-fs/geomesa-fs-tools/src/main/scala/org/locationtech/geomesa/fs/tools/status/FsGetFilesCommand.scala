/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.Parameters
import org.apache.iceberg.DataFile
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergSchemaMapper
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.status.FsGetFilesCommand.FSGetFilesParams
import org.locationtech.geomesa.tools.{Command, OptionalCqlFilterParam, RequiredTypeNameParam}

import java.time.Instant
import java.util.Locale

class FsGetFilesCommand extends FsDataStoreCommand {

  import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

  override val params = new FSGetFilesParams

  override val name: String = "get-files"

  override def execute(): Unit = withDataStore { ds =>
    val storage = ds.storage(params.featureName)
    val metadata = storage.metadata

    // Create mapper to extract partitions from DataFiles
    val mapper = IcebergSchemaMapper(metadata.sft, metadata.schemes.toSeq, storage.context)

    lazy val fromFilter = {
      Command.user.info(s"Listing files for filter: ${ECQL.toCQL(params.cqlFilter)}")
      metadata.getFiles(params.cqlFilter)
    }
    lazy val fromPartitions = {
      Command.user.info(s"Listing files for partition(s): ${params.loadedPartitions.mkString(", ")}")
      params.loadedPartitions.flatMap(metadata.getFiles)
    }

    val files =
      if (params.cqlFilter == null && params.loadedPartitions.isEmpty) {
        Command.user.info("Listing files for all partitions")
        metadata.getFiles()
      } else if (params.loadedPartitions.isEmpty) {
        fromFilter
      } else if (params.cqlFilter == null) {
        fromPartitions
      } else {
        (fromFilter ++ fromPartitions).distinct
      }

    def extractAction(file: DataFile): String = {
      val location = file.location()
      val filename = location.substring(location.lastIndexOf('/') + 1)
      filename.take(2) match {
        case "w_" => "WRITTEN"
        case "c_" => "COMPACTED"
        case "m_" => "MODIFIED"
        case "d_" => "DELETED"
        case _ => "UNKNOWN"
      }
    }

    files.groupBy(f => mapper.partition(f)).toSeq.sortBy(_._1.toString).foreach { case (p, files) =>
      Command.output.info(s"$p:")
      // sort by record count descending
      files.sortBy(_.recordCount())(Ordering[Long].reverse).foreach { f =>
        Command.output.info(s"  ${f.location()} ${extractAction(f)} ${f.recordCount()} features")
      }
    }
  }
}

object FsGetFilesCommand {
  @Parameters(commandDescription = "List files for partitions")
  class FSGetFilesParams extends FsParams with RequiredTypeNameParam with PartitionParam with OptionalCqlFilterParam
}
