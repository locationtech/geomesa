/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.fs.tools.export.FsExportCommand
import org.locationtech.geomesa.fs.tools.ingest.FsIngestCommand
import org.locationtech.geomesa.fs.tools.status.{FsDescribeSchemaCommand, FsGetSftConfigCommand, FsGetTypeNamesCommand}
import org.locationtech.geomesa.tools.export.GenerateAvroSchemaCommand
import org.locationtech.geomesa.tools.status.{EnvironmentCommand, HelpCommand, VersionCommand}
import org.locationtech.geomesa.tools.{Command, ConvertCommand, Runner}

object FsRunner extends Runner {

  override val name: String = "geomesa-fs"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new FsDescribeSchemaCommand,
    new EnvironmentCommand,
    new FsExportCommand,
    new HelpCommand(this, jc),
    new FsIngestCommand,
    new FsGetTypeNamesCommand,
    new VersionCommand,
    new FsGetSftConfigCommand,
    new GenerateAvroSchemaCommand,
    new ConvertCommand
  )
}
