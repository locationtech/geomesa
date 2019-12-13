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

import org.apache.kudu.client.{AlterTableOptions, CreateTableOptions, KuduTable, PartialRow}
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.index.z3.Z3IndexKey
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, PeriodColumnAdapter, ZColumnAdapter}
import org.locationtech.geomesa.kudu.{KuduValue, Partitioning}
import org.locationtech.geomesa.utils.index.ByteArrays

object Z3ColumnMapper {
  private val columns = Seq(PeriodColumnAdapter, ZColumnAdapter, FeatureIdAdapter)
}

class Z3ColumnMapper(index: GeoMesaFeatureIndex[_, _]) extends KuduColumnMapper(index, Z3ColumnMapper.columns) {

  override def configurePartitions(): CreateTableOptions = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConverters._

    val options = new CreateTableOptions()

    // add hash splits based on our shards, which we don't need to actually store as a separate column
    val shards = index.sft.getZShards
    if (shards > 1) {
      options.addHashPartitions(Collections.singletonList(FeatureIdAdapter.name), shards)
    }

    options.setRangePartitionColumns(Seq(PeriodColumnAdapter.name, ZColumnAdapter.name).asJava)

    val splits = {
      val configured = DefaultSplitter.Parser.z3Splits(index.sft.getZ3Interval, splitters)
      if (configured.isEmpty) {
        val bin = BinnedTime.timeToBinnedTime(index.sft.getZ3Interval)(System.currentTimeMillis()).bin
        Map(bin -> Seq(0, Long.MaxValue))
      } else {
        configured.groupBy(_._1).map { case (bin, times) =>
          val ts = times.flatMap(_._2)
          // add upper and lower bounds as our splits don't have endpoints
          val t = if (ts.isEmpty) { Seq(0, Long.MaxValue) } else {
            val builder = Seq.newBuilder[Long]
            builder.sizeHint(ts.size + 2)
            builder += 0L
            builder ++= ts.sorted
            builder += Long.MaxValue
            builder.result.distinct
          }
          bin -> t
        }
      }
    }

    splits.foreach { case (bin, times) =>
      times.sliding(2).foreach { case Seq(lo, hi) =>
        val lower = tableSchema.newPartialRow()
        val upper = tableSchema.newPartialRow()
        lower.addShort(0, bin)
        upper.addShort(0, bin)
        lower.addLong(1, lo)
        upper.addLong(1, hi)
        options.addRangePartition(lower, upper)
      }
    }

    options
  }

  override def createPartition(table: KuduTable, bin: Short): Option[Partitioning] = {
    val alteration = new AlterTableOptions()

    val bitSplits = {
      val configured = DefaultSplitter.Parser.z3BitSplits(splitters)
      if (configured.isEmpty) { Seq(0L, Long.MaxValue) } else {
        configured :+ Long.MaxValue // add an upper bound as our splits don't have endpoints
      }
    }

    bitSplits.sliding(2).foreach { case Seq(lo, hi) =>
      val lower = tableSchema.newPartialRow()
      val upper = tableSchema.newPartialRow()
      lower.addShort(0, bin)
      upper.addShort(0, bin)
      lower.addLong(1, lo)
      upper.addLong(1, hi)
      alteration.addRangePartition(lower, upper)
    }

    Some(Partitioning(table.getName, alteration))
  }

  override def createKeyValues(value: SingleRowKeyValue[_]): Seq[KuduValue[_]] = {
    val fid = KuduValue(new String(value.id, StandardCharsets.UTF_8), FeatureIdAdapter)
    value.key match {
      case Z3IndexKey(bin, z) => Seq(KuduValue(bin, PeriodColumnAdapter), KuduValue(z, ZColumnAdapter), fid)
      case _ => throw new IllegalStateException(s"Expected z value but got '${value.key}'")
    }
  }

  override def toRowRanges(ranges: Seq[ScanRange[_]],
                           tieredKeyRanges: Seq[ByteRange]): Seq[(Option[PartialRow], Option[PartialRow])] = {

    def lower(key: Z3IndexKey): Some[PartialRow] = {
      val row = tableSchema.newPartialRow()
      PeriodColumnAdapter.writeToRow(row, key.bin)
      ZColumnAdapter.writeToRow(row, key.z)
      FeatureIdAdapter.writeToRow(row, "")
      Some(row)
    }

    def upper(key: Z3IndexKey): Some[PartialRow] = {
      val row = tableSchema.newPartialRow()
      PeriodColumnAdapter.writeToRow(row, key.bin)
      if (key.z == Long.MaxValue) {
        // push the exclusive value to the feature ID to avoid numeric overflow
        ZColumnAdapter.writeToRow(row, key.z)
        FeatureIdAdapter.writeToRow(row, new String(ByteArrays.ZeroByteArray, StandardCharsets.UTF_8))
      } else {
        ZColumnAdapter.writeToRow(row, key.z + 1L)
        FeatureIdAdapter.writeToRow(row, "")
      }
      Some(row)
    }

    ranges.asInstanceOf[Seq[ScanRange[Z3IndexKey]]].map {
      case BoundedRange(lo, hi) => (lower(lo), upper(hi))
      case UnboundedRange(_)    => (None, None)
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }
  }
}
