/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import org.geotools.data.DataStore
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.stats.{MinMax, Stat}
import org.locationtech.jts.geom.Geometry
import org.opengis.filter.Filter

trait StatsBoundsCommand[DS <: DataStore with HasGeoMesaStats] extends DataStoreCommand[DS] {

  override val name = "stats-bounds"
  override def params: StatsBoundsParams

  override def execute(): Unit = withDataStore(calculateBounds)

  protected def calculateBounds(ds: DS): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val sft = ds.getSchema(params.featureName)
    val attributes = StatsCommand.getAttributesFromParams(sft, params)
    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)

    val allBounds = if (params.exact) {
      Command.user.info("Running stat query...")
      val query = Stat.SeqStat(attributes.map(Stat.MinMax))
      ds.stats.runStats[MinMax[Any]](sft, query, filter)
    } else {
      if (filter != Filter.INCLUDE) {
        Command.user.warn("Ignoring CQL filter for non-exact stat query")
      }
      ds.stats.getStats[MinMax[Any]](sft, attributes)
    }

    attributes.foreach { attribute =>
      val out = allBounds.find(_.property == attribute) match {
        case None => "[ unavailable ]"
        case Some(mm) if mm.isEmpty => "[ no matching data ]"
        case Some(mm) =>
          if (attribute == sft.getGeomField) {
            val e = mm.min.asInstanceOf[Geometry].getEnvelopeInternal
            e.expandToInclude(mm.max.asInstanceOf[Geometry].getEnvelopeInternal)
            s"[ ${e.getMinX}, ${e.getMinY}, ${e.getMaxX}, ${e.getMaxY} ] cardinality: ${mm.cardinality}"
          } else {
            val stringify = Stat.stringifier(sft.getDescriptor(attribute).getType.getBinding)
            s"[ ${stringify(mm.min)} to ${stringify(mm.max)} ] cardinality: ${mm.cardinality}"
          }
      }
      Command.output.info(s"  $attribute $out")
    }
  }
}

// @Parameters(commandDescription = "View or calculate bounds on attributes in a GeoMesa feature type")
trait StatsBoundsParams extends StatsParams with AttributeStatsParams
