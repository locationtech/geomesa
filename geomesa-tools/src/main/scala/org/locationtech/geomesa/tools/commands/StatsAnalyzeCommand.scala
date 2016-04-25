/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.tools.commands.StatsAnalyzeCommand.StatsRunParameters
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType

class StatsAnalyzeCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  import StatsAnalyzeCommand.name

  override val command = "stats-analyze"
  override val params = new StatsRunParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    System.err.println(s"Running stat analysis for feature type ${sft.getTypeName}...")
    val stats = ds.stats.runStats(sft)
    System.err.println("Stats analyzed:")
    println(toString(sft, stats))
    System.err.println("Use list-stats for more details")
  }

  private def toString(sft: SimpleFeatureType, stat: Stat): String = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    stat match {
      case s: CountStat           => s"Total features: ${s.count}"
      case s: SeqStat             => s.stats.map(toString(sft, _)).filterNot(_.isEmpty).mkString("  ", "\n  ", "")
      case s: RangeHistogram[Any] => "" // don't print histograms - they get the bounds from min/max
      case s: MinMax[Any]         =>
        val attribute = name(sft, s.attribute)
        val bounds = if (sft.getGeomField == attribute) {
          val env = s.min.asInstanceOf[Geometry].getEnvelopeInternal
          env.expandToInclude(s.max.asInstanceOf[Geometry].getEnvelopeInternal)
          s"[ ${env.getMinX}, ${env.getMinY}, ${env.getMaxX}, ${env.getMaxY} ]"
        } else {
          s"[ '${s.stringify(s.min)}' to '${s.stringify(s.max)}' ]"
        }
        s"Bounds for $attribute: $bounds"

      // corresponds to what we expect in GeoMesaMetadataStats
      case _ => throw new NotImplementedError("Only Count, MinMax and RangeHistogram stats are supported")
    }
  }
}

object StatsAnalyzeCommand {

  @Parameters(commandDescription = "Analyze statistics on a GeoMesa feature type")
  class StatsRunParameters extends FeatureParams

  private def name(sft: SimpleFeatureType, index: Int): String = sft.getDescriptor(index).getLocalName
}
