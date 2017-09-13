/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams, PartitionParam}
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

import scala.collection.JavaConversions._

class FSGetFilesCommand extends FsDataStoreCommand {
  override val params = new FSGetFilesParams

  override val name: String = "get-files"

  override def execute(): Unit = withDataStore { ds =>
    val toList = if (params.partitions.nonEmpty) {
      params.partitions
    } else {
      ds.storage.getMetadata(params.featureName).getPartitions
    }

    Command.user.info(s"Listing files for ${toList.size()} partitions")
    toList.foreach { p =>
      ds.storage.getMetadata(params.featureName).getFiles(p).foreach { f =>
        Command.output.info(s"$p\t$f")
      }
    }
  }
}

@Parameters(commandDescription = "List files for partitions")
class FSGetFilesParams extends FsParams with RequiredTypeNameParam with PartitionParam