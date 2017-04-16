package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Query, Result, Scan}
import org.apache.hadoop.hbase.filter.Filter
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.data.{HBaseQueryPlan, MultiRowRangeFilterScanPlan}
import org.opengis.feature.simple.SimpleFeature

trait HBasePlatform extends HBaseFeatureIndex {

  override def buildPlatformScanPlan(filter: HBaseFilterStrategyType,
                                     ranges: Seq[Query],
                                     table: TableName,
                                     hbaseFilters: Seq[Filter],
                                     toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan = {
    MultiRowRangeFilterScanPlan(filter, table, ranges.asInstanceOf[Seq[Scan]], hbaseFilters, toFeatures)
  }
}
