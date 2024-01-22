/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools

import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.HBaseDistributedCommand
import org.locationtech.geomesa.tools.{Command, Runner}

object HBaseRunner extends Runner {

  override val name: String = "geomesa-hbase"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new data.HBaseCreateSchemaCommand,
      new data.HBaseDeleteCatalogCommand,
      new data.HBaseDeleteFeaturesCommand,
      new data.HBaseManagePartitionsCommand,
      new data.HBaseRemoveSchemaCommand,
      new data.HBaseUpdateSchemaCommand,
      new export.HBaseExportCommand with HBaseDistributedCommand,
      new export.HBasePlaybackCommand,
      new ingest.HBaseBulkIngestCommand,
      new ingest.HBaseBulkLoadCommand,
      new ingest.HBaseIngestCommand with HBaseDistributedCommand,
      new stats.HBaseStatsAnalyzeCommand,
      new stats.HBaseStatsBoundsCommand,
      new stats.HBaseStatsCountCommand,
      new stats.HBaseStatsTopKCommand,
      new stats.HBaseStatsHistogramCommand,
      new status.HBaseDescribeSchemaCommand,
      new status.HBaseExplainCommand,
      new status.HBaseGetSftConfigCommand,
      new status.HBaseGetTypeNamesCommand,
      new status.HBaseVersionRemoteCommand
    )
  }

  override protected def classpathEnvironments: Seq[String] = Seq("HBASE_HOME", "HADOOP_HOME")
}
