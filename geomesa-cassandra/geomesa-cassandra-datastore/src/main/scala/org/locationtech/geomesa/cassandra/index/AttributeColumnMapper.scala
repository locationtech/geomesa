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
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey

object AttributeColumnMapper {

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[Integer, AttributeColumnMapper]() {
      override def load(shards: Integer): AttributeColumnMapper = {
        val mappers = Seq.tabulate(shards) { i =>
          ColumnSelect(CassandraColumnMapper.ShardColumn, i, i, startInclusive = true, endInclusive = true)
        }
        new AttributeColumnMapper(mappers)
      }
    }
  )

  def apply(shards: Int): AttributeColumnMapper = cache.get(shards)
}

class AttributeColumnMapper(shards: Seq[ColumnSelect]) extends CassandraColumnMapper {

  private val Shard     = CassandraColumnMapper.ShardColumn
  private val Value     = NamedColumn("attrVal",   1, "text",     classOf[String])
  private val Secondary = NamedColumn("secondary", 2, "blob",     classOf[ByteBuffer])
  private val FeatureId = CassandraColumnMapper.featureIdColumn(3)
  private val Feature   = CassandraColumnMapper.featureColumn(4)

  override val columns: Seq[NamedColumn] = Seq(Shard, Value, Secondary, FeatureId, Feature)

  override def bind(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val shard = Byte.box(if (value.shard.isEmpty) { 0 } else { value.shard.head })
    val AttributeIndexKey(_, v, _) = value.key
    val secondary = ByteBuffer.wrap(value.tier)
    val fid = new String(value.id, StandardCharsets.UTF_8)
    val Seq(feature) = value.values.map(v => ByteBuffer.wrap(v.value))
    Seq(shard, v, secondary, fid, feature)
  }

  override def bindDelete(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val shard = Byte.box(if (value.shard.isEmpty) { 0 } else { value.shard.head })
    val AttributeIndexKey(_, v, _) = value.key
    val secondary = ByteBuffer.wrap(value.tier)
    val fid = new String(value.id, StandardCharsets.UTF_8)
    Seq(shard, v, secondary, fid)
  }

  override def select(range: ScanRange[_], tieredKeyRanges: Seq[ByteRange]): Seq[RowSelect] = {
    val primary = range.asInstanceOf[ScanRange[AttributeIndexKey]] match {
      case SingleRowRange(row)   => Seq(ColumnSelect(Value, row.value, row.value, startInclusive = true, endInclusive = true))
      case BoundedRange(lo, hi)  => Seq(ColumnSelect(Value, lo.value, hi.value, lo.inclusive, hi.inclusive))
      case LowerBoundedRange(lo) => Seq(ColumnSelect(Value, lo.value, null, lo.inclusive, endInclusive = false))
      case UpperBoundedRange(hi) => Seq(ColumnSelect(Value, null, hi.value, startInclusive = false, hi.inclusive))
      case PrefixRange(prefix)   => Seq(ColumnSelect(Value, prefix.value, prefix.value + "zzzz", prefix.inclusive, endInclusive = false)) // TODO ?
      case UnboundedRange(empty) => Seq.empty
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
    val clause = if (tieredKeyRanges.isEmpty) { primary } else {
      val minTier = ByteRange.min(tieredKeyRanges)
      val maxTier = ByteRange.max(tieredKeyRanges)
      primary :+ ColumnSelect(Secondary, ByteBuffer.wrap(minTier), ByteBuffer.wrap(maxTier), startInclusive = true, endInclusive = true)
    }
    if (clause.isEmpty) { Seq(RowSelect(clause)) } else {
      shards.map(s => RowSelect(clause.+:(s)))
    }
  }
}
