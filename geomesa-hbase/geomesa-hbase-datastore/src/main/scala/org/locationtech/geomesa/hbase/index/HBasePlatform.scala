package org.locationtech.geomesa.hbase.index

import com.google.common.collect.Lists
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Query, Result, Scan}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{Filter, FilterList, MultiRowRangeFilter}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseQueryPlan, ScanPlan}
import org.locationtech.geomesa.hbase.filters.{JSimpleFeatureFilter, Z3HBaseFilter}
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.index.index.Z3ProcessingValues
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
    // TODO: parameterize this?
    val rangesPerThread = math.min(ds.config.maxRangesPerExtendedScan, math.max(1,math.ceil(numRanges/numThreads*2).toInt))
    // TODO: align partitions with region boundaries
    val groupedRanges = Lists.partition(sortedRowRanges, rangesPerThread)

    // group scans into batches to achieve some client side parallelism
    val groupedScans = groupedRanges.map { localRanges =>
      // TODO: FIX
      // currently, this constructor will call sortAndMerge a second time
      // this is unnecessary as we have already sorted and merged above
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

    // Apply Visibilities
    groupedScans.foreach(ds.applySecurity)

    ScanPlan(filter, table, groupedScans, hbaseFilters, toFeatures)
  }

  override def configurePushDownFilters(config: ScanConfig,
                                        ecql: Option[org.opengis.filter.Filter],
                                        transform: Option[(String, SimpleFeatureType)],
                                        sft: SimpleFeatureType): ScanConfig = {
    // TODO: currently we configure both z3 filter and ECQL geometry/time push down
    // TODO: optimize by removing geo and time predicates from ECQL

    val z3Filter =
      org.locationtech.geomesa.index.index.Z3Index.currentProcessingValues match {
        case None                                           => Seq.empty[org.apache.hadoop.hbase.filter.Filter]
        case Some(Z3ProcessingValues(sfc, _, xy, _, times)) => configureZ3PushDown(sfc, xy, times)
      }

    val cqlFilter =
      if(ecql.isDefined || transform.isDefined) {
        configureCQLAndTransformPushDown(ecql, transform, sft)
      } else {
        Seq.empty[org.apache.hadoop.hbase.filter.Filter]
      }

    config.copy(hbaseFilters = config.hbaseFilters ++ z3Filter ++ cqlFilter)
  }

  private def configureCQLAndTransformPushDown(ecql: Option[org.opengis.filter.Filter], transform: Option[(String, SimpleFeatureType)], sft: SimpleFeatureType) = {
    val (tform, tSchema) = transform.getOrElse(("", null))
    val tSchemaString = Option(tSchema).map(SimpleFeatureTypes.encodeType(_)).getOrElse("")
    Seq[org.apache.hadoop.hbase.filter.Filter](new JSimpleFeatureFilter(sft, ecql.getOrElse(org.opengis.filter.Filter.INCLUDE), tform, tSchemaString))
  }

  private def configureZ3PushDown(sfc: Z3SFC, xy: Seq[(Double, Double, Double, Double)], times: Map[Short, Seq[(Long, Long)]]) = {
    // we know we're only going to scan appropriate periods, so leave out whole ones
    val wholePeriod = Seq((sfc.time.min.toLong, sfc.time.max.toLong))
    val filteredTimes = times.filter(_._2 != wholePeriod)
    val normalizedXY = xy.map { case (xmin, ymin, xmax, ymax) =>
      Array(sfc.lon.normalize(xmin), sfc.lat.normalize(ymin), sfc.lon.normalize(xmax), sfc.lat.normalize(ymax))
    }.toArray
    var minEpoch: Int = Short.MaxValue
    var maxEpoch: Int = Short.MinValue
    val tOpts = filteredTimes.toSeq.sortBy(_._1).map { case (bin, times) =>
      times.map { case (t1, t2) =>
        val lt = sfc.time.normalize(t1)
        val ut = sfc.time.normalize(t2)
        if (lt < minEpoch) minEpoch = lt
        if (ut > maxEpoch) maxEpoch = ut
        Array(lt, ut)
      }.toArray
    }.toArray

    // TODO: deal with non-points in the XZ filter
    val filt = new Z3HBaseFilter(new Z3Filter(
      normalizedXY, tOpts, minEpoch.toShort, maxEpoch.toShort, 1, 8)
    )

    Seq(filt)
  }

}
