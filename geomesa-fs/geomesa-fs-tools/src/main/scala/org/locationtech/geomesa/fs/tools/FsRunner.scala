/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import org.locationtech.geomesa.tools.{Command, Runner}

object FsRunner extends Runner {

  override val name: String = "geomesa-fs"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new compact.FsCompactCommand(),
      new data.FsCreateSchemaCommand(),
      new data.FsUpdateSchemaCommand(),
      new export.FsExportCommand(),
      new export.FsPlaybackCommand(),
      new ingest.FsGeneratePartitionFiltersCommand(),
      new ingest.FsIngestCommand(),
      new ingest.FsManageMetadataCommand(),
      new stats.FsStatsBoundsCommand(),
      new stats.FsStatsCountCommand(),
      new stats.FsStatsHistogramCommand(),
      new stats.FsStatsTopKCommand(),
      new status.FsDescribeSchemaCommand(),
      new status.FsGetFilesCommand(),
      new status.FsGetPartitionsCommand(),
      new status.FsGetSftConfigCommand(),
      new status.FsGetTypeNamesCommand(),
    )
  }
}
