/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.status.FsGetPartitionsCommand.FsGetPartitionsParams
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

class FsGetPartitionsCommand extends FsDataStoreCommand {

  override val name: String = "get-partitions"
  override val params = new FsGetPartitionsParams

  override def execute(): Unit = withDataStore { ds =>
    Command.user.info(s"Partitions for type ${params.featureName}:")
    ds.storage(params.featureName).metadata.getPartitions().map(_.name).sorted.foreach(p => Command.output.info(p))
  }
}

object FsGetPartitionsCommand {
  @Parameters(commandDescription = "List partitions for a given feature type")
  class FsGetPartitionsParams extends FsParams with RequiredTypeNameParam
}
