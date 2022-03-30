/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import org.locationtech.geomesa.fs.tools.compact.FsCompactCommand
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand
import org.locationtech.geomesa.fs.tools.export.{FsExportCommand, FsPlaybackCommand}
import org.locationtech.geomesa.fs.tools.ingest.{FsGeneratePartitionFiltersCommand, FsIngestCommand, FsManageMetadataCommand}
import org.locationtech.geomesa.fs.tools.stats._
import org.locationtech.geomesa.fs.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object FsRunner extends Runner {

  override val name: String = "geomesa-fs"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new FsCreateSchemaCommand,
      new FsDescribeSchemaCommand,
      new FsExportCommand,
      new FsPlaybackCommand,
      new FsIngestCommand,
      new FsGetTypeNamesCommand,
      new FsGetSftConfigCommand,
      new FsManageMetadataCommand,
      new FsCompactCommand,
      new FsGetPartitionsCommand,
      new FsGetFilesCommand,
      new FsGeneratePartitionFiltersCommand,
      new FsStatsBoundsCommand,
      new FsStatsCountCommand,
      new FsStatsHistogramCommand,
      new FsStatsTopKCommand
    )
  }
}
