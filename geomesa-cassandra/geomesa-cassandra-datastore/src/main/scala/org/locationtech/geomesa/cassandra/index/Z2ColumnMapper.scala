/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.locationtech.geomesa.cassandra.{ColumnSelect, NamedColumn, RowSelect}
import org.locationtech.geomesa.index.api._

object Z2ColumnMapper {

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[Integer, Z2ColumnMapper]() {
      override def load(shards: Integer): Z2ColumnMapper = {
        val mappers = Seq.tabulate(shards) { i =>
          ColumnSelect(CassandraColumnMapper.ShardColumn, i, i, startInclusive = true, endInclusive = true)
        }
        new Z2ColumnMapper(mappers)
      }
    }
  )

  def apply(shards: Int): Z2ColumnMapper = cache.get(shards)
}

class Z2ColumnMapper(shards: Seq[ColumnSelect]) extends CassandraColumnMapper {

  private val Shard     = CassandraColumnMapper.ShardColumn
  private val ZValue    = CassandraColumnMapper.zColumn(1)
  private val FeatureId = CassandraColumnMapper.featureIdColumn(2)
  private val Feature   = CassandraColumnMapper.featureColumn(3)

  override val columns: Seq[NamedColumn] = Seq(Shard, ZValue, FeatureId, Feature)

  override def bind(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val shard = Byte.box(if (value.shard.isEmpty) { 0 } else { value.shard.head })
    val z = Long.box(value.key.asInstanceOf[Long])
    val fid = new String(value.id, StandardCharsets.UTF_8)
    val Seq(feature) = value.values.map(v => ByteBuffer.wrap(v.value))
    Seq(shard, z, fid, feature)
  }

  override def bindDelete(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val shard = Byte.box(if (value.shard.isEmpty) { 0 } else { value.shard.head })
    val z = Long.box(value.key.asInstanceOf[Long])
    val fid = new String(value.id, StandardCharsets.UTF_8)
    Seq(shard, z, fid)
  }

  override def select(range: ScanRange[_], tieredKeyRanges: Seq[ByteRange]): Seq[RowSelect] = {
    val clause = range match {
      case BoundedRange(lo, hi)  => Seq(ColumnSelect(ZValue, lo, hi, startInclusive = true, endInclusive = true))
      case UnboundedRange(_)     => Seq.empty
      case SingleRowRange(row)   => Seq(ColumnSelect(ZValue, row, row, startInclusive = true, endInclusive = true))
      case LowerBoundedRange(lo) => Seq(ColumnSelect(ZValue, lo, null, startInclusive = true, endInclusive = false))
      case UpperBoundedRange(hi) => Seq(ColumnSelect(ZValue, null, hi, startInclusive = false, endInclusive = true))
      case PrefixRange(_)        => Seq.empty // not supported
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
    if (clause.isEmpty) { Seq(RowSelect(clause)) } else {
      shards.map(s => RowSelect(clause.+:(s)))
    }
  }
}
