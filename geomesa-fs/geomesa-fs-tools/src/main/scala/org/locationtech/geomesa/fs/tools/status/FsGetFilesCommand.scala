/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.status

import java.time.Instant
import java.util.Locale

import com.beust.jcommander.{ParameterException, Parameters}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.status.FsGetFilesCommand.FSGetFilesParams
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

class FsGetFilesCommand extends FsDataStoreCommand {

  import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

  import scala.collection.JavaConverters._

  override val params = new FSGetFilesParams

  override val name: String = "get-files"

  override def execute(): Unit = withDataStore { ds =>
    val metadata = ds.storage(params.featureName).metadata
    val partitions = if (params.partitions.isEmpty) { metadata.getPartitions() } else {
      params.partitions.asScala.map { name =>
        metadata.getPartition(name).getOrElse {
          throw new ParameterException(s"Partition $name cannot be found in metadata")
        }
      }
    }

    Command.user.info(s"Listing files for ${partitions.length} partitions")
    partitions.sortBy(_.name).foreach { partition =>
      Command.output.info(s"${partition.name}:")
      // sort by chronological order
      partition.files.sorted(StorageMetadata.StorageFileOrdering.reverse).foreach { f =>
        Command.output.info(s"\t${f.action.toString.toUpperCase(Locale.US)} " +
            s"${GeoToolsDateFormat.format(Instant.ofEpochMilli(f.timestamp))} ${f.name}")
      }
    }
  }
}

object FsGetFilesCommand {
  @Parameters(commandDescription = "List files for partitions")
  class FSGetFilesParams extends FsParams with RequiredTypeNameParam with PartitionParam
}
