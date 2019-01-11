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

import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.cassandra.{NamedColumn, RowRange, RowValue}
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.index.ShardStrategy
import org.locationtech.geomesa.index.index.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.index.z3._
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case object CassandraZ3Index extends CassandraZ3Index

trait CassandraZ3Index extends CassandraFeatureIndex[Z3IndexValues, Z3IndexKey]
    with SpatioTemporalFilterStrategy[CassandraDataStore, CassandraFeature, Seq[RowValue]] {

  private val Shard     = CassandraFeatureIndex.ShardColumn
  private val Period    = CassandraFeatureIndex.binColumn(1)
  private val ZValue    = CassandraFeatureIndex.zColumn(2)
  private val FeatureId = CassandraFeatureIndex.featureIdColumn(3)
  private val Feature   = CassandraFeatureIndex.featureColumn(4)

  override val name: String = Z3Index.Name

  override val version: Int = 2

  override protected val columns: Seq[NamedColumn] = Seq(Shard, Period, ZValue, FeatureId, Feature)

  override protected val keySpace: Z3IndexKeySpace = Z3IndexKeySpace

  override protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy = ZShardStrategy(sft)

  override protected def createValues(shards: ShardStrategy,
                                      toIndexKey: SimpleFeature => Seq[Z3IndexKey],
                                      includeFeature: Boolean)
                                     (cf: CassandraFeature): Seq[Seq[RowValue]] = {
    val shard = RowValue(Shard, Byte.box(shards(cf).headOption.getOrElse(0)))
    val fid = RowValue(FeatureId, cf.feature.getID)
    toIndexKey(cf.feature).map { key =>
      val period = RowValue(Period, Short.box(key.bin))
      val z = RowValue(ZValue, Long.box(key.z))
      if (includeFeature) {
        Seq(shard, period, z, fid, RowValue(Feature, ByteBuffer.wrap(cf.fullValue)))
      } else {
        Seq(shard, period, z, fid)
      }
    }
  }

  override protected def toRowRanges(sft: SimpleFeatureType, range: ScanRange[Z3IndexKey]): Seq[RowRange] = {
    range match {
      case BoundedRange(lo, hi)  => Seq(RowRange(Period, lo.bin, hi.bin), RowRange(ZValue, lo.z, hi.z))
      case UnboundedRange(_)     => Seq.empty
      case SingleRowRange(row)   => Seq(RowRange(Period, row.bin, row.bin), RowRange(ZValue, row.z, row.z))
      case LowerBoundedRange(lo) => Seq(RowRange(Period, lo.bin, null), RowRange(ZValue, lo.z, null))
      case UpperBoundedRange(hi) => Seq(RowRange(Period, null, hi.bin), RowRange(ZValue, null, hi.z))
      case PrefixRange(_)        => Seq.empty // not supported
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }
}
