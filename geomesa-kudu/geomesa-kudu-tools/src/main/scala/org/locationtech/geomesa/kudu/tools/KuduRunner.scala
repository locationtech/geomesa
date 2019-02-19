/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.kudu.tools.data._
import org.locationtech.geomesa.kudu.tools.export.KuduExportCommand
import org.locationtech.geomesa.kudu.tools.ingest.KuduIngestCommand
import org.locationtech.geomesa.kudu.tools.stats._
import org.locationtech.geomesa.kudu.tools.status._
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object KuduRunner extends Runner {

  override val name: String = "geomesa-kudu"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new KuduCreateSchemaCommand,
    new KuduDeleteCatalogCommand,
    new KuduDeleteFeaturesCommand,
    new KuduDescribeSchemaCommand,
    new EnvironmentCommand,
    new KuduExplainCommand,
    new KuduExportCommand,
    new HelpCommand(this, jc),
    new KuduIngestCommand,
    new KuduKeywordsCommand,
    new KuduGetTypeNamesCommand,
    new KuduRemoveSchemaCommand,
    new VersionCommand,
    new KuduGetSftConfigCommand,
    new GenerateAvroSchemaCommand,
    new KuduStatsBoundsCommand,
    new KuduStatsCountCommand,
    new KuduStatsTopKCommand,
    new KuduStatsHistogramCommand,
    new ConvertCommand,
    new ConfigureCommand,
    new ClasspathCommand,
    new ScalaConsoleCommand
  )
}
