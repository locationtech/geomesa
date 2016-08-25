/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.api

import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.opengis.filter.Filter

/**
  * Filters split into a 'primary' that will be used for range planning,
  * and a 'secondary' that will be applied as a final step.
  */
case class FilterStrategy[Ops <: HasGeoMesaStats, FeatureWrapper, Result, Plan]
    (index: GeoMesaFeatureIndex[Ops, FeatureWrapper, Result, Plan],
    primary: Option[Filter], secondary: Option[Filter] = None) {

  lazy val filter: Option[Filter] = andOption(primary.toSeq ++ secondary)

  override lazy val toString: String =
    s"$index[${primary.map(filterToString).getOrElse("INCLUDE")}]" +
        s"[${secondary.map(filterToString).getOrElse("None")}]"
}

/**
  * A series of queries required to satisfy a filter - basically split on ORs
  */
case class FilterPlan[Ops <: HasGeoMesaStats, FeatureWrapper, Result, Plan]
    (strategies: Seq[FilterStrategy[Ops, FeatureWrapper, Result, Plan]]) {
  override lazy val toString: String = s"FilterPlan[${strategies.mkString(",")}]"
}

object FilterPlan {
  def apply[Ops <: HasGeoMesaStats, FeatureWrapper, Result, Plan]
      (filter: FilterStrategy[Ops, FeatureWrapper, Result, Plan]):
      FilterPlan[Ops, FeatureWrapper, Result, Plan] = FilterPlan(Seq(filter))
}

