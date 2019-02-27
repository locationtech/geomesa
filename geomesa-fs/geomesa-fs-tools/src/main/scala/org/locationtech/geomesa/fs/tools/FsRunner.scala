/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.fs.tools.compact.CompactCommand
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand
import org.locationtech.geomesa.fs.tools.export.{FsExportCommand, FsPlaybackCommand}
import org.locationtech.geomesa.fs.tools.ingest.{FsIngestCommand, ManageMetadataCommand}
import org.locationtech.geomesa.fs.tools.stats._
import org.locationtech.geomesa.fs.tools.status._
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object FsRunner extends Runner {

  override val name: String = "geomesa-fs"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new FsCreateSchemaCommand,
    new FsDescribeSchemaCommand,
    new EnvironmentCommand,
    new FsExportCommand,
    new FsPlaybackCommand,
    new HelpCommand(this, jc),
    new FsIngestCommand,
    new FsGetTypeNamesCommand,
    new VersionCommand,
    new FsGetSftConfigCommand,
    new GenerateAvroSchemaCommand,
    new ConvertCommand,
    new ManageMetadataCommand(this, jc),
    new ClasspathCommand,
    new ConfigureCommand,
    new ScalaConsoleCommand,
    new CompactCommand,
    new FsGetPartitionsCommand,
    new FsGetFilesCommand,
    new FsStatsBoundsCommand,
    new FsStatsCountCommand,
    new FsStatsHistogramCommand,
    new FsStatsTopKCommand
  )
}
