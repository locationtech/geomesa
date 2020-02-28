/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object KuduRunner extends Runner {

  override val name: String = "geomesa-kudu"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new data.KuduCreateSchemaCommand,
    new data.KuduDeleteCatalogCommand,
    new data.KuduRemoveSchemaCommand,
    new data.KuduUpdateSchemaCommand,
    new export.KuduExportCommand,
    new ingest.KuduDeleteFeaturesCommand,
    new ingest.KuduIngestCommand,
    new status.KuduDescribeSchemaCommand,
    new status.KuduExplainCommand,
    new status.KuduGetTypeNamesCommand,
    new status.KuduGetSftConfigCommand,
    new stats.KuduStatsBoundsCommand,
    new stats.KuduStatsCountCommand,
    new stats.KuduStatsTopKCommand,
    new stats.KuduStatsHistogramCommand,
    // common commands, placeholders for script functions
    new ConvertCommand,
    new ConfigureCommand,
    new ClasspathCommand,
    new EnvironmentCommand,
    new GenerateAvroSchemaCommand,
    new HelpCommand(this, jc),
    new ScalaConsoleCommand,
    new VersionCommand
  )
}
