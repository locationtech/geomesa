/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands.stats

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.index.utils.GeoMesaMetadata
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.CommandWithCatalog
import org.locationtech.geomesa.tools.accumulo.commands.stats.StatsAnalyzeCommand.StatsRunParameters
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType

class StatsAnalyzeCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  import StatsAnalyzeCommand.name
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val command = "stats-analyze"
  override val params = new StatsRunParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)

    def noStats = ds.metadata.read(sft.getTypeName, GeoMesaMetadata.STATS_GENERATION_KEY, cache = false).isEmpty

    logger.info(s"Running stat analysis for feature type ${sft.getTypeName}...")
    val stats = if (noStats) {
      // this is the first time anyone has accessed this data store since stats were implemented
      // stats should be kicked off and run in the background, we just have to wait for it to finish
      // note: if something goes wrong this will just hang... but this is kind of a fringe case
      while (noStats) {
        Thread.sleep(1000)
      }
      // sleep an additional bit so that the stat table gets configured
      Thread.sleep(1000)
      ds.stats.getStats[CountStat](sft) ++ ds.stats.getStats[MinMax[Any]](sft)
    } else {
      ds.stats.generateStats(sft)
    }

    val strings = stats.collect {
      case s: CountStat   => s"Total features: ${s.count}"
      case s: MinMax[Any] =>
        val attribute = name(sft, s.attribute)
        val bounds = if (s.isEmpty) {
          "[ no matching data ]"
        } else if (sft.getGeomField == attribute) {
          val e = s.min.asInstanceOf[Geometry].getEnvelopeInternal
          e.expandToInclude(s.max.asInstanceOf[Geometry].getEnvelopeInternal)
          s"[ ${e.getMinX}, ${e.getMinY}, ${e.getMaxX}, ${e.getMaxY} ] cardinality: ${s.cardinality}"
        } else {
          s"[ ${s.stringify(s.min)} to ${s.stringify(s.max)} ] cardinality: ${s.cardinality}"
        }
        s"Bounds for $attribute: $bounds"
    }

    logger.info("Stats analyzed:")
    println(strings.mkString("  ", "\n  ", ""))
    logger.info("Use 'stats-histogram', 'stats-top-k' or 'stats-count' commands for more details")

    ds.dispose()
  }
}

object StatsAnalyzeCommand {

  @Parameters(commandDescription = "Analyze statistics on a GeoMesa feature type")
  class StatsRunParameters extends GeoMesaConnectionParams with FeatureTypeNameParam

  private def name(sft: SimpleFeatureType, index: Int): String = sft.getDescriptor(index).getLocalName
}
