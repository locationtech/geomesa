/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.hbase.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object FsHBaseRunner extends Runner {

  override val name: String = "geomesa-fs-hbase"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new BulkCopyCommand,
    new CopyFeaturesCommand,
    new HelpCommand(this, jc),
    new VersionCommand,
    new ClasspathCommand,
    new EnvironmentCommand,
    new ConfigureCommand
  )
}
