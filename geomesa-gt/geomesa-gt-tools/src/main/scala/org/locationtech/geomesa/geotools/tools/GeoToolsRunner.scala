/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._

object GeoToolsRunner extends Runner {

  override val name: String = "geomesa-gt"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new data.GeoToolsCreateSchemaCommand,
    new data.GeoToolsDeleteFeaturesCommand,
    new data.GeoToolsDescribeSchemaCommand,
    new data.GeoToolsGetSftConfigCommand,
    new data.GeoToolsGetTypeNamesCommand,
    new data.GeoToolsRemoveSchemaCommand,
    new data.GeoToolsUpdateSchemaCommand,
    new export.GeoToolsExportCommand,
    new export.GeoToolsPlaybackCommand,
    new ingest.GeoToolsIngestCommand,
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
