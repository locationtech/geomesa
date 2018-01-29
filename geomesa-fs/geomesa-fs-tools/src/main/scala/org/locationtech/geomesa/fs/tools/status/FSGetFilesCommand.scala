/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.{ParameterException, Parameters}
import org.locationtech.geomesa.fs.tools.status.FSGetFilesCommand.FSGetFilesParams
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams, PartitionParam}
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

import scala.collection.JavaConversions._

class FSGetFilesCommand extends FsDataStoreCommand {
  override val params = new FSGetFilesParams

  override val name: String = "get-files"

  override def execute(): Unit = withDataStore { ds =>
    val existingPartitions = ds.storage.getMetadata(params.featureName).getPartitions
    val toList = if (params.partitions.nonEmpty) {
      params.partitions.filterNot(existingPartitions.contains).headOption.foreach { p =>
        throw new ParameterException(s"Partition $p cannot be found in metadata")
      }
      params.partitions
    } else {
      existingPartitions
    }

    Command.user.info(s"Listing files for ${toList.size()} partitions")
    toList.foreach { p =>
      ds.storage.getMetadata(params.featureName).getFiles(p).foreach { f =>
        Command.output.info(s"$p\t$f")
      }
    }
  }
}

object FSGetFilesCommand {
  @Parameters(commandDescription = "List files for partitions")
  class FSGetFilesParams extends FsParams with RequiredTypeNameParam with PartitionParam
}
