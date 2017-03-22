/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.stats.{MinMax, Stat}
import org.opengis.filter.Filter

trait StatsBoundsCommand[DS <: GeoMesaDataStore[_, _, _]] extends DataStoreCommand[DS] {

  override val name = "stats-bounds"
  override def params: StatsBoundsParams

  override def execute(): Unit = withDataStore(calculateBounds)

  protected def calculateBounds(ds: DS): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val sft = ds.getSchema(params.featureName)
    val attributes = StatsCommand.getAttributesFromParams(sft, params)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)

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
      Command.output.info(s"  $attribute $out")
    }
  }
}

// @Parameters(commandDescription = "View or calculate bounds on attributes in a GeoMesa feature type")
trait StatsBoundsParams extends StatsParams with AttributeStatsParams
