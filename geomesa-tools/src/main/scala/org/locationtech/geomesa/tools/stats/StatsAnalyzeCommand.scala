/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.stats

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.GeoMesaMetadata
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, TypeNameParam}
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

trait StatsAnalyzeCommand[DS <: GeoMesaDataStore[_, _, _]] extends DataStoreCommand[DS] {

  import StatsAnalyzeCommand.attributeName

  override val name = "stats-analyze"
  override def params: StatsAnalyzeParams

  override def execute() = {
    try { withDataStore(analyze) } catch {
      case NonFatal(e) => logger.error("Error analyzing stats: ", e)
    }
  }

  private def analyze(ds: DS): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

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
        val attribute = attributeName(sft, s.attribute)
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
  }
}

// @Parameters(commandDescription = "Analyze statistics on a GeoMesa feature type")
trait StatsAnalyzeParams extends CatalogParam with TypeNameParam

object StatsAnalyzeCommand {
  private def attributeName(sft: SimpleFeatureType, index: Int): String = sft.getDescriptor(index).getLocalName
}
