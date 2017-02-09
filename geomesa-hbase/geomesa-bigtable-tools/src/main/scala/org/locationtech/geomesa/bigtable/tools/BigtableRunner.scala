/************************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
************************************************************************/

package org.locationtech.geomesa.bigtable.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.hbase.tools.data._
import org.locationtech.geomesa.hbase.tools.export.HBaseExportCommand
import org.locationtech.geomesa.hbase.tools.stats._
import org.locationtech.geomesa.hbase.tools.status._
import org.locationtech.geomesa.tools.export.GenerateAvroSchemaCommand
import org.locationtech.geomesa.tools.status.{EnvironmentCommand, HelpCommand, VersionCommand}
import org.locationtech.geomesa.tools.{Command, Runner}

object BigtableRunner extends Runner {

  override val name: String = "geomesa-bigtable"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new HBaseCreateSchemaCommand,
    new HBaseDeleteCatalogCommand,
    new HBaseDeleteFeaturesCommand,
    new HBaseDescribeSchemaCommand,
    new EnvironmentCommand,
    new HBaseExplainCommand,
    new HBaseExportCommand,
    new HelpCommand(this, jc),
    new BigtableIngestCommand,
    new HBaseKeywordsCommand,
    new HBaseGetTypeNamesCommand,
    new HBaseRemoveSchemaCommand,
    new HBaseVersionRemoteCommand,
    new VersionCommand,
    new HBaseGetSftConfigCommand,
    new GenerateAvroSchemaCommand,
    new HBaseStatsAnalyzeCommand,
    new HBaseStatsBoundsCommand,
    new HBaseStatsCountCommand,
    new HBaseStatsTopKCommand,
    new HBaseStatsHistogramCommand
  )
}
