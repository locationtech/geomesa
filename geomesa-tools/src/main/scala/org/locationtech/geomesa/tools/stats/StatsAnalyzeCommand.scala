/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import com.beust.jcommander.ParameterException
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.tools.stats.StatsAnalyzeCommand.StatsAnalyzeParams
import org.locationtech.geomesa.tools.{CatalogParam, Command, DataStoreCommand, TypeNameParam}
import org.locationtech.geomesa.utils.stats._
import org.locationtech.jts.geom.Geometry

trait StatsAnalyzeCommand[DS <: GeoMesaDataStore[DS]] extends DataStoreCommand[DS] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  override val name = "stats-analyze"
  override def params: StatsAnalyzeParams

  override def execute(): Unit = withDataStore(analyze)

  private def analyze(ds: DS): Unit = {
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist")
    }

    def noStats = ds.metadata.read(sft.getTypeName, GeoMesaMetadata.StatsGenerationKey, cache = false).isEmpty

    Command.user.info(s"Running stat analysis for feature type ${sft.getTypeName}...")
    val stats = if (noStats) {
      // this is the first time anyone has accessed this data store since stats were implemented
      // stats should be kicked off and run in the background, we just have to wait for it to finish
      // note: if something goes wrong this will just hang... but this is kind of a fringe case
      while (noStats) {
        Thread.sleep(1000)
      }
      // sleep an additional bit so that the stat table gets configured
      Thread.sleep(1000)
      val queries = sft.getAttributeDescriptors.asScala.map(d => Stat.MinMax(d.getLocalName))
      ds.stats.getStat[CountStat](sft, Stat.Count()) ++ ds.stats.getSeqStat[MinMax[Any]](sft, queries)
    } else {
      ds.stats.writer.analyze(sft)
    }

    val strings = stats.collect {
      case s: CountStat   => s"Total features: ${s.count}"
      case s: MinMax[Any] =>
        val bounds = if (s.isEmpty) {
          "[ no matching data ]"
        } else if (sft.getGeomField == s.property) {
          val e = s.min.asInstanceOf[Geometry].getEnvelopeInternal
          e.expandToInclude(s.max.asInstanceOf[Geometry].getEnvelopeInternal)
          s"[ ${e.getMinX}, ${e.getMinY}, ${e.getMaxX}, ${e.getMaxY} ] cardinality: ${s.cardinality}"
        } else {
          val stringify = Stat.stringifier(sft.getDescriptor(s.property).getType.getBinding)
          s"[ ${stringify(s.min)} to ${stringify(s.max)} ] cardinality: ${s.cardinality}"
        }
        s"Bounds for ${s.property}: $bounds"
    }

    Command.user.info("Stats analyzed:")
    Command.output.info(strings.mkString("  ", "\n  ", ""))
    Command.user.info("Use 'stats-histogram', 'stats-top-k' or 'stats-count' commands for more details")
  }
}

object StatsAnalyzeCommand {
  // @Parameters(commandDescription = "Analyze statistics on a GeoMesa feature type")
  trait StatsAnalyzeParams extends CatalogParam with TypeNameParam
}
