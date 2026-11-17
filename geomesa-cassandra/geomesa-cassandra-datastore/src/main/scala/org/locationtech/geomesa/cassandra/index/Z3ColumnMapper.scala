/***********************************************************************
 * Copyright (c) 2017-2026 IBM
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.locationtech.geomesa.cassandra.{ColumnSelect, NamedColumn, RowSelect}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.index.z3.Z3IndexKey

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object Z3ColumnMapper {

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[Integer, Z3ColumnMapper]() {
      override def load(shards: Integer): Z3ColumnMapper = {
        val mappers = Seq.tabulate(shards) { i =>
          ColumnSelect(CassandraColumnMapper.ShardColumn, i, i, startInclusive = true, endInclusive = true)
      }
        new Z3ColumnMapper(mappers)
      }
    }
  )

  def apply(shards: Int): Z3ColumnMapper = cache.get(shards)
}

class Z3ColumnMapper(shards: Seq[ColumnSelect]) extends CassandraColumnMapper {

  private val Shard     = CassandraColumnMapper.ShardColumn
  private val Period    = CassandraColumnMapper.binColumn(1)
  private val ZValue    = CassandraColumnMapper.zColumn(2)
  private val FeatureId = CassandraColumnMapper.featureIdColumn(3)
  private val Feature   = CassandraColumnMapper.featureColumn(4)

  override val columns: Seq[NamedColumn] = Seq(Shard, Period, ZValue, FeatureId, Feature)

  override def bind(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val shard = Byte.box(if (value.shard.isEmpty) { 0 } else { value.shard.head })
    val Z3IndexKey(bin, z) = value.key
    val fid = new String(value.id, StandardCharsets.UTF_8)
    val Seq(feature) = value.values.map(v => ByteBuffer.wrap(v.value))
    Seq(shard, Short.box(bin), Long.box(z), fid, feature)
  }

  override def bindDelete(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val shard = Byte.box(if (value.shard.isEmpty) { 0 } else { value.shard.head })
    val Z3IndexKey(bin, z) = value.key
    val fid = new String(value.id, StandardCharsets.UTF_8)
    Seq(shard, Short.box(bin), Long.box(z), fid)
  }

  override def select(range: ScanRange[_], tieredKeyRanges: Seq[ByteRange]): Seq[RowSelect] = {
    range.asInstanceOf[ScanRange[Z3IndexKey]] match {
      case BoundedRange(lo, hi) =>
        val binSelect = ColumnSelect(Period, lo.bin, hi.bin, startInclusive = true, endInclusive = true)
        val zSelect = ColumnSelect(ZValue, lo.z, hi.z, startInclusive = true, endInclusive = true)
        sharded(Seq(binSelect, zSelect))

      case SingleRowRange(row) =>
        val binSelect = ColumnSelect(Period, row.bin, row.bin, startInclusive = true, endInclusive = true)
        val zSelect = ColumnSelect(ZValue, row.z, row.z, startInclusive = true, endInclusive = true)
        sharded(Seq(binSelect, zSelect))

      case LowerBoundedRange(lo) =>
        Seq(openEndedPeriods(ColumnSelect(Period, lo.bin, null, startInclusive = true, endInclusive = false)))

      case UpperBoundedRange(hi) =>
        Seq(openEndedPeriods(ColumnSelect(Period, null, hi.bin, startInclusive = false, endInclusive = true)))

      case UnboundedRange(_) | PrefixRange(_) =>
        Seq(RowSelect(Seq.empty)) // full table scan - note: prefix ranges are not supported

      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }

  private def sharded(clause: Seq[ColumnSelect]): Seq[RowSelect] = shards.map(s => RowSelect(clause.+:(s)))

  /**
   * Selects an open-ended range of periods (i.e. a time filter with only a lower or upper bound).
   *
   * The period is part of the partition key, which Cassandra can only select by range with 'allow filtering',
   * and the open-ended periods can't be enumerated. Instead, we use a single statement with 'allow filtering' across
   * all shards, which requires a full table scan but filters out the other periods server-side. The z restriction is
   * omitted, as it can't be combined with a range on the period - the full query filter is applied client-side.
   *
   * @param periods period restriction
   * @return
   */
  private def openEndedPeriods(periods: ColumnSelect): RowSelect = RowSelect(Seq(periods), allowFiltering = true)
}
