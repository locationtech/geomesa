/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.redis.tools.export.{RedisExplainCommand, RedisExportCommand}
import org.locationtech.geomesa.redis.tools.ingest.{RedisDeleteFeaturesCommand, RedisIngestCommand}
import org.locationtech.geomesa.redis.tools.schema._
import org.locationtech.geomesa.redis.tools.stats._
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object RedisRunner extends Runner {

  override val name: String = "geomesa-redis"

  override protected def createCommands(jc: JCommander): Seq[Command] = Seq(
    new RedisCreateSchemaCommand,
    new RedisDeleteCatalogCommand,
    new RedisDeleteFeaturesCommand,
    new RedisDescribeSchemaCommand,
    new RedisExplainCommand,
    new RedisExportCommand,
    new RedisGetSftConfigCommand,
    new RedisGetTypeNamesCommand,
    new RedisIngestCommand,
    new RedisKeywordsCommand,
    new RedisManagePartitionsCommand(this, jc),
    new RedisRemoveSchemaCommand,
    new RedisStatsAnalyzeCommand,
    new RedisStatsBoundsCommand,
    new RedisStatsCountCommand,
    new RedisStatsHistogramCommand,
    new RedisStatsTopKCommand,
    new HelpCommand(this, jc),
    new ConfigureCommand,
    new ClasspathCommand,
    new ScalaConsoleCommand,
    new VersionCommand
  )
}
