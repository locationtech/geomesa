/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.Parameters
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.status.FsGetFilesCommand.FSGetFilesParams
import org.locationtech.geomesa.tools.{Command, OptionalCqlFilterParam, RequiredTypeNameParam}

class FsGetFilesCommand extends FsDataStoreCommand {

  override val params = new FSGetFilesParams

  override val name: String = "get-files"

  override def execute(): Unit = withDataStore { ds =>
    val metadata = ds.storage(params.featureName).metadata

    lazy val fromFilter = {
      Command.user.info(s"Listing files for filter: ${ECQL.toCQL(params.cqlFilter)}")
      metadata.files().forFilter(params.cqlFilter).scan()
    }
    lazy val fromPartitions = {
      Command.user.info(s"Listing files for partition(s): ${params.loadedPartitions.mkString(", ")}")
      params.loadedPartitions.flatMap(metadata.files().forPartition(_).scan())
    }

    val files =
      if (params.cqlFilter == null && params.loadedPartitions.isEmpty) {
        Command.user.info("Listing files for all partitions")
        metadata.files().scan()
      } else if (params.loadedPartitions.isEmpty) {
        fromFilter
      } else if (params.cqlFilter == null) {
        fromPartitions
      } else {
        (fromFilter ++ fromPartitions).distinct
      }

    files.groupBy(f => metadata.partition(f)).toSeq.sortBy(_._1.toString).foreach { case (p, files) =>
      Command.output.info(s"$p:")
      // sort by record count descending
      files.sortBy(_.recordCount())(Ordering[Long].reverse).foreach { f =>
        Command.output.info(s"  ${f.location()} ${f.recordCount()} features")
      }
    }
  }
}

object FsGetFilesCommand {
  @Parameters(commandDescription = "List files for partitions")
  class FSGetFilesParams extends FsParams with RequiredTypeNameParam with PartitionParam with OptionalCqlFilterParam
}
