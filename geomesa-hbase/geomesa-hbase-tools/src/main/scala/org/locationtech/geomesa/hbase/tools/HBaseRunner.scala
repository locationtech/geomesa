/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.HBaseDistributedCommand
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object HBaseRunner extends Runner {

  override val name: String = "geomesa-hbase"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new data.HBaseCreateSchemaCommand,
    new data.HBaseDeleteCatalogCommand,
    new data.HBaseDeleteFeaturesCommand,
    new data.HBaseManagePartitionsCommand(this, jc),
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
    new status.HBaseVersionRemoteCommand,
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

  override def environmentErrorInfo(): Option[String] = {
    if (sys.env.get("HBASE_HOME").isEmpty || sys.env.get("HADOOP_HOME").isEmpty) {
      Option("Warning: you have not set HBASE_HOME and/or HADOOP_HOME as environment variables." +
        "\nGeoMesa tools will not run without the appropriate HBase and Hadoop jars in the tools classpath." +
        "\nPlease ensure that those jars are present in the classpath by running 'geomesa-hbase classpath'." +
        "\nTo take corrective action, please place the necessary jar files in the lib directory of geomesa-tools.")
    } else { None }
  }
}
