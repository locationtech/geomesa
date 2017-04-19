package org.locationtech.geomesa.hbase.index

import com.google.common.collect.Lists
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Query, Result, Scan}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{Filter, FilterList, MultiRowRangeFilter}
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseQueryPlan, ScanPlan}
import org.locationtech.geomesa.hbase.filters.JSimpleFeatureFilter
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait HBasePlatform extends HBaseFeatureIndex {

  override def buildPlatformScanPlan(ds: HBaseDataStore,
                                     filter: HBaseFilterStrategyType,
                                     originalRanges: Seq[Query],
                                     table: TableName,
                                     hbaseFilters: Seq[Filter],
                                     toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan = {
    import scala.collection.JavaConversions._

    val rowRanges = Lists.newArrayList[RowRange]()
    originalRanges.foreach { r =>
      rowRanges.add(new RowRange(r.asInstanceOf[Scan].getStartRow, true, r.asInstanceOf[Scan].getStopRow, false))
    }
    val sortedRowRanges = MultiRowRangeFilter.sortAndMerge(rowRanges)
    val numRanges = sortedRowRanges.length
    val numThreads = ds.config.queryThreads
    val rangesPerThread = math.min(ds.config.maxRangesPerExtendedScan, math.max(1,math.ceil(numRanges/numThreads*2).toInt))
    // TODO: align partitions with region boundaries
    val groupedRanges = Lists.partition(sortedRowRanges, rangesPerThread)

    // group scans into batches to achieve some client side parallelism
    val groupedScans = groupedRanges.map { localRanges =>
      // TODO: currently, this constructor will call sortAndMerge a second time, this is unnecessary as we have already sorted and merged above
      val mrrf = new MultiRowRangeFilter(localRanges)
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, mrrf)
      hbaseFilters.foreach { f => filterList.addFilter(f) }

      val s = new Scan()
      s.setStartRow(localRanges.head.getStartRow)
      s.setStopRow(localRanges.get(localRanges.length-1).getStopRow)
      s.setFilter(filterList)
      s.setCaching(1000)
      s.setCacheBlocks(true)
      s
    }

    ScanPlan(filter, table, groupedScans, toFeatures)
  }

  override def configurePushDownFilters(config: ScanConfig,
                                        ecql: Option[org.opengis.filter.Filter],
                                        transform: Option[(String, SimpleFeatureType)],
                                        sft: SimpleFeatureType): ScanConfig = {
    // TODO: currently we configure both z3 filter and ECQL geometry/time push down
    // TODO: optimize by removing geo and time predicates from ECQL


    val cqlFilter =
      if(ecql.isDefined || transform.isDefined) {
        configureCQLAndTransformPushDown(ecql, transform, sft)
      } else {
        Seq.empty[org.apache.hadoop.hbase.filter.Filter]
      }

    config.copy(hbaseFilters = config.hbaseFilters ++ cqlFilter)
  }

  private def configureCQLAndTransformPushDown(ecql: Option[org.opengis.filter.Filter], transform: Option[(String, SimpleFeatureType)], sft: SimpleFeatureType) = {
    val (tform, tSchema) = transform.getOrElse(("", null))
    val tSchemaString = Option(tSchema).map(SimpleFeatureTypes.encodeType(_)).getOrElse("")
    Seq[org.apache.hadoop.hbase.filter.Filter](new JSimpleFeatureFilter(sft, ecql.getOrElse(org.opengis.filter.Filter.INCLUDE), tform, tSchemaString))
  }


}
