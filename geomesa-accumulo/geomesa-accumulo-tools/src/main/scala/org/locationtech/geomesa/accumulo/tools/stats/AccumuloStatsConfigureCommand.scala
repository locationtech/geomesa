/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.stats.StatsCombiner
import org.locationtech.geomesa.accumulo.tools.stats.AccumuloStatsConfigureCommand.AccumuloStatsConfigureParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.stats.StatsConfigureCommand
import org.locationtech.geomesa.tools.stats.StatsConfigureCommand.StatsConfigureParams
import org.locationtech.geomesa.tools.utils.Prompt

class AccumuloStatsConfigureCommand extends StatsConfigureCommand[AccumuloDataStore] with AccumuloDataStoreCommand {

  override val params = new AccumuloStatsConfigureParams

  override protected def list(ds: AccumuloDataStore): Unit = {
    val configured = StatsCombiner.list(ds.connector, s"${ds.config.catalog}_stats").map { case (k, v) => s"$k -> $v" }
    Command.user.info(s"Configured stats iterator: ${configured.mkString("\n  ", "\n  ", "")}")
  }

  override protected def add(ds: AccumuloDataStore): Unit = {
    val lock = ds.acquireCatalogLock()
    try {
      ds.getTypeNames.map(ds.getSchema).foreach { sft =>
        Command.user.info(s"Configuring stats iterator for '${sft.getTypeName}'...")
        ds.stats.configureStatCombiner(ds.connector, sft)
      }
    } finally {
      lock.release()
    }
    Command.user.info("done")
  }

  override protected def remove(ds: AccumuloDataStore): Unit = {
    val confirm = Prompt.confirm(s"Removing stats iterator configuration for catalog '${ds.config.catalog}'. " +
        "Continue (y/n)? ")
    if (confirm) {
      val lock = ds.acquireCatalogLock()
      try {
        ds.getTypeNames.map(ds.getSchema).foreach { sft =>
          Command.user.info(s"Removing stats iterator for '${sft.getTypeName}'...")
          ds.stats.removeStatCombiner(ds.connector, sft)
        }
      } finally {
        lock.release()
      }
      Command.user.info("done")
    }
  }
}

object AccumuloStatsConfigureCommand {
  @Parameters(commandDescription = "View, add or remove Accumulo stats combining iterator for a catalog")
  class AccumuloStatsConfigureParams extends StatsConfigureParams with AccumuloDataStoreParams
}
