/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.hbase.tools.data._
import org.locationtech.geomesa.hbase.tools.export.{HBaseFileExportCommand, HBaseLeafletExportCommand}
import org.locationtech.geomesa.hbase.tools.ingest.{HBaseBulkIngestCommand, HBaseBulkLoadCommand, HBaseIngestCommand}
import org.locationtech.geomesa.hbase.tools.stats._
import org.locationtech.geomesa.hbase.tools.status._
import org.locationtech.geomesa.tools.export.GenerateAvroSchemaCommand
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, ConvertCommand, Runner}

object HBaseRunner extends Runner {

  override val name: String = "geomesa-hbase"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new HBaseBulkIngestCommand,
    new HBaseBulkLoadCommand,
    new HBaseCreateSchemaCommand,
    new HBaseDeleteCatalogCommand,
    new HBaseDeleteFeaturesCommand,
    new HBaseDescribeSchemaCommand,
    new EnvironmentCommand,
    new HBaseExplainCommand,
    new HBaseFileExportCommand,
    new HBaseLeafletExportCommand,
    new HelpCommand(this, jc),
    new HBaseIngestCommand,
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
    new HBaseStatsHistogramCommand,
    new ConvertCommand,
    new ConfigureCommand,
    new ClasspathCommand,
    new ScalaConsoleCommand
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
