/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import com.beust.jcommander.{Parameter, ParameterException}
import org.geotools.data.DataStore
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.tools.stats.StatsTopKCommand.StatsTopKParams
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.stats.{Stat, TopK}
import org.opengis.filter.Filter

trait StatsTopKCommand[DS <: DataStore with HasGeoMesaStats] extends DataStoreCommand[DS] {

  override val name = "stats-top-k"
  override val params: StatsTopKParams

  override def execute(): Unit = withDataStore(topK)

  protected def topK(ds: DS): Unit = {
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist")
    }

    val attributes = getAttributesFromParams(sft, params)
    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)
    val k = Option(params.k).map(_.intValue)

    if (params.exact) {
      Command.user.info("Running stat query...")
    } else if (filter != Filter.INCLUDE) {
      Command.user.warn("Non-exact stat queries may not fully account for the specified CQL filter")
    }

    val results = ds.stats.getSeqStat[TopK[Any]](sft, attributes.map(Stat.TopK), filter, params.exact)

    attributes.foreach { attribute =>
      Command.output.info(s"Top values for '$attribute':")
      val stat = results.find(_.property == attribute)
      stat match {
        case None => Command.output.info("  unavailable")
        case Some(s) =>
          val stringify = Stat.stringifier(sft.getDescriptor(attribute).getType.getBinding)
          s.topK(k.getOrElse(s.size)).foreach { case (value, count) =>
            Command.output.info(s"  ${stringify(value)} ($count)")
          }
      }
    }
  }
}

object StatsTopKCommand {
  // @Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
  trait StatsTopKParams extends StatsParams with AttributeStatsParams {
    @Parameter(names = Array("-k"), description = "Number of top values to show")
    var k: Integer = _
  }
}
