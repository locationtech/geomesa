/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.strategies

import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.IdExtractingVisitor
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, Id, Or}

trait IdFilterStrategy[T, U] extends GeoMesaFeatureIndex[T, U] {

  override def getFilterStrategy(filter: Filter,
                                 transform: Option[SimpleFeatureType],
                                 stats: Option[GeoMesaStats]): Option[FilterStrategy] = {
    if (filter == Filter.INCLUDE) {
      Some(FilterStrategy(this, None, None, Long.MaxValue))
    } else if (filter == Filter.EXCLUDE) {
      None
    } else {
      val (ids, notIds) = IdExtractingVisitor(filter)
      if (ids.isDefined) {
        // top-priority index - always 1 if there are actually ID filters
        Some(FilterStrategy(this, ids, notIds, IdFilterStrategy.StaticCost))
      } else {
        Some(FilterStrategy(this, None, Some(filter), Long.MaxValue))
      }
    }
  }
}

object IdFilterStrategy {

  val StaticCost = 1L

  def intersectIdFilters(filter: Filter): Set[String] = {
    import scala.collection.JavaConversions._
    filter match {
      case f: And => f.getChildren.map(intersectIdFilters).reduceLeftOption(_ intersect _).getOrElse(Set.empty)
      case f: Or  => f.getChildren.flatMap(intersectIdFilters).toSet
      case f: Id  => f.getIDs.map(_.toString).toSet
      case _ => throw new IllegalArgumentException(s"Expected ID filter, got ${filterToString(filter)}")
    }
  }
}
