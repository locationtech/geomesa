/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.tools

import org.locationtech.geomesa.tools.{Command, Runner}

object RedisRunner extends Runner {

  override val name: String = "geomesa-redis"

  override protected def commands: Seq[Command] = {
    super.commands ++ Seq(
      new export.RedisExplainCommand,
      new export.RedisExportCommand,
      new ingest.RedisDeleteFeaturesCommand,
      new ingest.RedisIngestCommand,
      new schema.RedisCreateSchemaCommand,
      new schema.RedisDeleteCatalogCommand,
      new schema.RedisDescribeSchemaCommand,
      new schema.RedisGetSftConfigCommand,
      new schema.RedisGetTypeNamesCommand,
      new schema.RedisManagePartitionsCommand,
      new schema.RedisRemoveSchemaCommand,
      new schema.RedisUpdateSchemaCommand,
      new stats.RedisStatsAnalyzeCommand,
      new stats.RedisStatsBoundsCommand,
      new stats.RedisStatsCountCommand,
      new stats.RedisStatsHistogramCommand,
      new stats.RedisStatsTopKCommand
    )
  }
}
