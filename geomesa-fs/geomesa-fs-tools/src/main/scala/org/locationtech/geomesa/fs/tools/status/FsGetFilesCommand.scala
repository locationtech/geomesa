/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.{ParameterException, Parameters}
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{PartitionParam, FsParams}
import org.locationtech.geomesa.fs.tools.status.FsGetFilesCommand.FSGetFilesParams
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

import scala.collection.JavaConversions._

class FsGetFilesCommand extends FsDataStoreCommand {

  override val params = new FSGetFilesParams

  override val name: String = "get-files"

  override def execute(): Unit = withDataStore { ds =>
    val storage = ds.storage(params.featureName)
    val existingPartitions = storage.getMetadata.getPartitions
    val toList = if (params.partitions.nonEmpty) {
      params.partitions.filterNot(existingPartitions.contains).headOption.foreach { p =>
        throw new ParameterException(s"Partition $p cannot be found in metadata")
      }
      params.partitions
    } else {
      existingPartitions
    }

    Command.user.info(s"Listing files for ${toList.size()} partitions")
    toList.sorted.foreach { p =>
      Command.output.info(s"$p:")
      storage.getMetadata.getFiles(p).foreach(f => Command.output.info(s"\t$f"))
    }
  }
}

object FsGetFilesCommand {
  @Parameters(commandDescription = "List files for partitions")
  class FSGetFilesParams extends FsParams with RequiredTypeNameParam with PartitionParam
}
