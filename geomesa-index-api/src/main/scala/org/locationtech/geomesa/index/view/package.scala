/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import org.geotools.data.Query
import org.opengis.filter.Filter

package object view {

  import org.locationtech.geomesa.filter.andFilters

  /**
    * Helper method to merge a filtered data store query
    *
    * @param query query
    * @param filter data store filter
    * @return
    */
  def mergeFilter(query: Query, filter: Option[Filter]): Query = {
    filter match {
      case None => query
      case Some(f) =>
        val q = new Query(query)
        q.setFilter(if (q.getFilter == Filter.INCLUDE) { f } else { andFilters(Seq(q.getFilter, f)) })
        q
    }
  }

  /**
    * Helper method to merge a filtered data store query
    *
    * @param filter filter
    * @param option data store filter
    * @return
    */
  def mergeFilter(filter: Filter, option: Option[Filter]): Filter = {
    option match {
      case None => filter
      case Some(f) => if (filter == Filter.INCLUDE) { f } else { andFilters(Seq(filter, f)) }
    }
  }
}
