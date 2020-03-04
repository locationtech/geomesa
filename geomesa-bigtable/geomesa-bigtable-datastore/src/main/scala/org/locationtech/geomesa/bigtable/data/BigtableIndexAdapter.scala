/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.data

import com.google.cloud.bigtable.hbase.BigtableExtendedScan
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, MultiRowRangeFilter, Filter => HFilter}
import org.locationtech.geomesa.hbase.data.HBaseIndexAdapter
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.TableScan

class BigtableIndexAdapter(ds: BigtableDataStore) extends HBaseIndexAdapter(ds) {

  import scala.collection.JavaConverters._

  // https://cloud.google.com/bigtable/quotas#limits-table-id
  override val tableNameLimit: Option[Int] = Some(50)

  override protected def configureScans(
      tables: Seq[TableName],
      ranges: Seq[RowRange],
      small: Boolean,
      colFamily: Array[Byte],
      filters: Seq[HFilter],
      coprocessor: Boolean): Seq[TableScan] = {
    if (filters.nonEmpty) {
      // bigtable does support some filters, but currently we only use custom filters that aren't supported
      throw new IllegalArgumentException(s"Bigtable does not support filters: ${filters.mkString(", ")}")
    } else if (coprocessor) {
      throw new IllegalArgumentException("Bigtable does not support coprocessors")
    }

    val hbase = super.configureScans(tables, ranges, small, colFamily, filters, coprocessor)

    if (small) { hbase } else {
      hbase.map { case TableScan(table, originals) =>
        val scans = originals.map { original =>
          val scan = new BigtableExtendedScan()
          scan.setStartRow(original.getStartRow)
          scan.setStopRow(original.getStopRow)
          val mrrf = original.getFilter match {
            case m: MultiRowRangeFilter => Some(m)
            case f: FilterList => f.getFilters.asScala.collectFirst { case m: MultiRowRangeFilter => m }
            case _ => None
          }
          mrrf.foreach(filter => filter.getRowRanges.asScala.foreach(r => scan.addRange(r.getStartRow, r.getStopRow)))
          scan
        }
        TableScan(table, scans)
      }
    }
  }
}
