/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.{Parameter, Parameters}
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.status.FsGetPartitionsCommand.FsGetPartitionsParams
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

class FsGetPartitionsCommand extends FsDataStoreCommand {

  override val name: String = "get-partitions"
  override val params = new FsGetPartitionsParams

  override def execute(): Unit = withDataStore { ds =>
    Command.user.info(s"Partitions for type ${params.featureName}:")
    if (!params.noHeader) {
      Command.output.info("partition\tfile_count\tfeature_count")
    }
    ds.storage(params.featureName).metadata.getFiles().groupBy(_.partition.toString).toSeq.sortBy(_._1).foreach { case (p, files) =>
      Command.output.info(s"$p\t${files.size}\t${files.map(_.count).sum}")
    }
  }
}

object FsGetPartitionsCommand {
  @Parameters(commandDescription = "List partitions for a given feature type")
  class FsGetPartitionsParams extends FsParams with RequiredTypeNameParam {
    @Parameter(
      names = Array("--no-header"),
      description = "Do not export the column header")
    var noHeader: Boolean = false
  }
}
