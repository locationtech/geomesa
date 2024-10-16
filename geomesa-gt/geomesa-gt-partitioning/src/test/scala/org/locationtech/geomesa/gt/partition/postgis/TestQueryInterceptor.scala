package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.api.data.{DataStore, Query}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.index.planning.QueryInterceptor

class TestQueryInterceptor extends QueryInterceptor {

  var sft: SimpleFeatureType = _

  override def init(ds: DataStore, sft: SimpleFeatureType): Unit = this.sft = sft

  override def rewrite(query: Query): Unit = query.setFilter(Filter.INCLUDE)

  override def close(): Unit = {}
}
