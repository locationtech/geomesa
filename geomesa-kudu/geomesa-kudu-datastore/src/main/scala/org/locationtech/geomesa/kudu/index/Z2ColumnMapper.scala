/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index

import java.nio.charset.StandardCharsets
import java.util.Collections

import org.apache.kudu.client.{CreateTableOptions, PartialRow}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.kudu.KuduValue
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, ZColumnAdapter}

object Z2ColumnMapper {
  private val columns = Seq(ZColumnAdapter, FeatureIdAdapter)
}

class Z2ColumnMapper(index: GeoMesaFeatureIndex[_, _]) extends KuduColumnMapper(index, Z2ColumnMapper.columns) {

  override def configurePartitions(): CreateTableOptions = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val options = new CreateTableOptions()

    // add hash splits based on our shards, which we don't need to actually store as a separate column
    val shards = index.sft.getZShards
    if (shards > 1) {
      options.addHashPartitions(Collections.singletonList(FeatureIdAdapter.name), shards)
    }

    options.setRangePartitionColumns(Collections.singletonList(ZColumnAdapter.name))

    val bitSplits = {
      val configured = DefaultSplitter.Parser.z2Splits(splitters)
      if (configured.isEmpty) { Seq(0L, Long.MaxValue) } else {
        // add upper and lower bounds as our splits don't have endpoints
        val builder = Seq.newBuilder[Long]
        builder.sizeHint(configured.length + 2)
        builder += 0L
        builder ++= configured.sorted
        builder += Long.MaxValue
        builder.result.distinct
      }
    }

    bitSplits.sliding(2).foreach { case Seq(lo, hi) =>
      val lower = tableSchema.newPartialRow()
      val upper = tableSchema.newPartialRow()
      lower.addLong(0, lo)
      upper.addLong(0, hi)
      options.addRangePartition(lower, upper)
    }

    options
  }

  override def createKeyValues(value: SingleRowKeyValue[_]): Seq[KuduValue[_]] = {
    val fid = KuduValue(new String(value.id, StandardCharsets.UTF_8), FeatureIdAdapter)
    value.key match {
      case z: Long => Seq(KuduValue(z, ZColumnAdapter), fid)
      case _ => throw new IllegalStateException(s"Expected z value but got '${value.key}'")
    }
  }

  override def toRowRanges(ranges: Seq[ScanRange[_]],
                           tieredKeyRanges: Seq[ByteRange]): Seq[(Option[PartialRow], Option[PartialRow])] = {

    def lower(z: Long): Some[PartialRow] = {
      val row = tableSchema.newPartialRow()
      ZColumnAdapter.writeToRow(row, z)
      FeatureIdAdapter.writeToRow(row, "")
      Some(row)
    }

    def upper(z: Long): Some[PartialRow] = {
      val row = tableSchema.newPartialRow()
      // note: shouldn't have to worry about overflow, as our z2/xz2 curves don't use the full 64 bits
      ZColumnAdapter.writeToRow(row, z + 1L)
      FeatureIdAdapter.writeToRow(row, "")
      Some(row)
    }

    ranges.asInstanceOf[Seq[ScanRange[Long]]].map {
      case BoundedRange(lo, hi) => (lower(lo), upper(hi))
      case UnboundedRange(_)    => (None, None)
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }
  }
}
