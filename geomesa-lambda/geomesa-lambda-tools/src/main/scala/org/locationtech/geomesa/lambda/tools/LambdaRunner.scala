/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2022 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.tools

import org.locationtech.geomesa.accumulo.tools.RunnerWithAccumuloEnvironment
import org.locationtech.geomesa.accumulo.tools.data._
import org.locationtech.geomesa.accumulo.tools.export.AccumuloExplainCommand
import org.locationtech.geomesa.accumulo.tools.stats._
import org.locationtech.geomesa.accumulo.tools.status._
import org.locationtech.geomesa.lambda.tools.data._
import org.locationtech.geomesa.lambda.tools.export.LambdaExportCommand
import org.locationtech.geomesa.lambda.tools.stats._
import org.locationtech.geomesa.tools._

object LambdaRunner extends RunnerWithAccumuloEnvironment {

  override val name: String = "geomesa-lambda"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new LambdaCreateSchemaCommand,
      new LambdaDeleteFeaturesCommand,
      new AccumuloDescribeSchemaCommand,
      new AccumuloExplainCommand,
      new LambdaExportCommand,
      new AccumuloGetTypeNamesCommand,
      new LambdaRemoveSchemaCommand,
      new AccumuloVersionRemoteCommand,
      new AccumuloGetSftConfigCommand,
      new AccumuloStatsAnalyzeCommand,
      new LambdaStatsBoundsCommand,
      new LambdaStatsCountCommand,
      new LambdaStatsTopKCommand,
      new LambdaStatsHistogramCommand,
      new AddIndexCommand
    )
  }
}
