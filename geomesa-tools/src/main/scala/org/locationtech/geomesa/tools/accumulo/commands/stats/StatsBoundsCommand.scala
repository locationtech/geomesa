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
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.tools.accumulo.commands.CommandWithCatalog
import org.locationtech.geomesa.tools.common.AttributesParam
import org.locationtech.geomesa.utils.stats.{MinMax, Stat}
import org.opengis.filter.Filter

class StatsBoundsCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val command = "stats-bounds"
  override val params = new StatsBoundsParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    val attributes = StatsCommand.getAttributes(sft, params)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)

    val allBounds = if (params.exact) {
      logger.info("Running stat query...")
      val query = Stat.SeqStat(attributes.map(Stat.MinMax))
      ds.stats.runStats[MinMax[Any]](sft, query, filter)
    } else {
      if (filter != Filter.INCLUDE) {
        logger.warn("Ignoring CQL filter for non-exact stat query")
      }
      ds.stats.getStats[MinMax[Any]](sft, attributes)
    }

    attributes.foreach { attribute =>
      val i = sft.indexOf(attribute)
      val out = allBounds.find(_.attribute == i) match {
        case None => "[ unavailable ]"
        case Some(mm) if mm.isEmpty => "[ no matching data ]"
        case Some(mm) =>
          if (attribute == sft.getGeomField) {
            val e = mm.min.asInstanceOf[Geometry].getEnvelopeInternal
            e.expandToInclude(mm.max.asInstanceOf[Geometry].getEnvelopeInternal)
            s"[ ${e.getMinX}, ${e.getMinY}, ${e.getMaxX}, ${e.getMaxY} ] cardinality: ${mm.cardinality}"
          } else {
            s"[ ${mm.stringify(mm.min)} to ${mm.stringify(mm.max)} ] cardinality: ${mm.cardinality}"
          }
      }
      println(s"  $attribute $out")
    }
  }
}

@Parameters(commandDescription = "View or calculate bounds on attributes in a GeoMesa feature type")
class StatsBoundsParameters extends StatsParams with AttributeStatsParams