/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools

import com.beust.jcommander.JCommander
import org.locationtech.geomesa.tools.export.{ConvertCommand, GenerateAvroSchemaCommand}
import org.locationtech.geomesa.tools.status._
import org.locationtech.geomesa.tools.{Command, Runner}

object RedisRunner extends Runner {

  override val name: String = "geomesa-redis"

  override protected def createCommands(jc: JCommander): Seq[Command] = Seq(
    new export.RedisExplainCommand,
    new export.RedisExportCommand,
    new ingest.RedisDeleteFeaturesCommand,
    new ingest.RedisIngestCommand,
    new schema.RedisCreateSchemaCommand,
    new schema.RedisDeleteCatalogCommand,
    new schema.RedisDescribeSchemaCommand,
    new schema.RedisGetSftConfigCommand,
    new schema.RedisGetTypeNamesCommand,
    new schema.RedisManagePartitionsCommand(this, jc),
    new schema.RedisRemoveSchemaCommand,
    new schema.RedisUpdateSchemaCommand,
    new stats.RedisStatsAnalyzeCommand,
    new stats.RedisStatsBoundsCommand,
    new stats.RedisStatsCountCommand,
    new stats.RedisStatsHistogramCommand,
    new stats.RedisStatsTopKCommand,
    // common commands, placeholders for script functions
    new ConfigureCommand,
    new ConvertCommand,
    new ClasspathCommand,
    new EnvironmentCommand,
    new GenerateAvroSchemaCommand,
    new HelpCommand(this, jc),
    new ScalaConsoleCommand,
    new VersionCommand
  )
}
