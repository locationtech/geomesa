/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2019 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.accumulo.tools.RunnerWithAccumuloEnvironment
import org.locationtech.geomesa.accumulo.tools.data._
import org.locationtech.geomesa.accumulo.tools.stats._
import org.locationtech.geomesa.accumulo.tools.status._
import org.locationtech.geomesa.lambda.tools.data._
import org.locationtech.geomesa.lambda.tools.export.LambdaExportCommand
import org.locationtech.geomesa.lambda.tools.stats._
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._

object LambdaRunner extends RunnerWithAccumuloEnvironment {

  override val name: String = "geomesa-lambda"

  override def createCommands(jc: JCommander): Seq[Command] = Seq(
    new LambdaCreateSchemaCommand,
    new LambdaDeleteFeaturesCommand,
    new AccumuloDescribeSchemaCommand,
    new EnvironmentCommand,
    new AccumuloExplainCommand,
    new LambdaExportCommand,
    new HelpCommand(this, jc),
    new AccumuloGetTypeNamesCommand,
    new LambdaRemoveSchemaCommand,
    new AccumuloVersionRemoteCommand,
    new VersionCommand,
    new AccumuloGetSftConfigCommand,
    new GenerateAvroSchemaCommand,
    new AccumuloStatsAnalyzeCommand,
    new LambdaStatsBoundsCommand,
    new LambdaStatsCountCommand,
    new LambdaStatsTopKCommand,
    new LambdaStatsHistogramCommand,
    new AddIndexCommand,
    new ConvertCommand,
    new ConfigureCommand,
    new ClasspathCommand,
    new ScalaConsoleCommand
  )
}
