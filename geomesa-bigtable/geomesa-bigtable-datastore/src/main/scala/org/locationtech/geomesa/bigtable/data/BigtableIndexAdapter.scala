/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.data

import java.util.UUID

import com.google.cloud.bigtable.hbase.BigtableExtendedScan
import com.google.common.collect.Lists
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.locationtech.geomesa.hbase.data.HBaseIndexAdapter
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.utils.text.StringSerialization

class BigtableIndexAdapter(ds: BigtableDataStore) extends HBaseIndexAdapter(ds) {

  import scala.collection.JavaConverters._

  // https://cloud.google.com/bigtable/quotas#limits-table-id
  override val tableNameLimit: Option[Int] = Some(50)

  override protected def configureScans(
      originalRanges: Seq[Scan],
      colFamily: Array[Byte],
      filters: Seq[HFilter],
      coprocessor: Boolean): Seq[Scan] = {
    if (filters.nonEmpty) {
      // bigtable does support some filters, but currently we only use custom filters that aren't supported
      throw new IllegalArgumentException(s"Bigtable doesn't support filters: ${filters.mkString(", ")}")
    }

    // check if these are large scans or small scans (e.g. gets)
    // only in the case of 'ID IN ()' queries will the scans be small
    if (originalRanges.headOption.exists(_.isSmall)) {
      originalRanges.map(s => new Scan(s).addFamily(colFamily))
    } else {
      val sortedRowRanges = HBaseIndexAdapter.sortAndMerge(originalRanges)
      val numRanges = sortedRowRanges.size()
      val numThreads = ds.config.queryThreads
      // TODO GEOMESA-1802 parameterize this?
      val rangesPerThread = math.min(ds.config.maxRangesPerExtendedScan, math.max(1,math.ceil(numRanges/numThreads*2).toInt))
      // TODO GEOMESA-1802 align partitions with region boundaries
      val groupedRanges = Lists.partition(sortedRowRanges, rangesPerThread)

      // group scans into batches to achieve some client side parallelism
      val groupedScans = groupedRanges.asScala.map { localRanges =>
        val scan = new BigtableExtendedScan()
        localRanges.asScala.foreach(r => scan.addRange(r.getStartRow, r.getStopRow))
        scan.addFamily(colFamily)
        scan
      }

      groupedScans
    }
  }
}
