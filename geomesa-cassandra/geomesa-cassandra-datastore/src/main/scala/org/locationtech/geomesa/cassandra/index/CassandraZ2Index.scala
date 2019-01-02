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
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexKeySpace, Z2IndexValues}
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case object CassandraZ2Index extends CassandraZ2Index

trait CassandraZ2Index extends CassandraFeatureIndex[Z2IndexValues, Long]
    with SpatialFilterStrategy[CassandraDataStore, CassandraFeature, Seq[RowValue]] {

  private val Shard     = CassandraFeatureIndex.ShardColumn
  private val ZValue    = CassandraFeatureIndex.zColumn(1)
  private val FeatureId = CassandraFeatureIndex.featureIdColumn(2)
  private val Feature   = CassandraFeatureIndex.featureColumn(3)

  override val name: String = Z2Index.Name

  override val version: Int = 2

  override protected val columns: Seq[NamedColumn] = Seq(Shard, ZValue, FeatureId, Feature)

  override protected val keySpace: Z2IndexKeySpace = Z2IndexKeySpace

  override protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy = ZShardStrategy(sft)

  override protected def createValues(shards: ShardStrategy,
                                      toIndexKey: SimpleFeature => Seq[Long],
                                      includeFeature: Boolean)
                                      (cf: CassandraFeature): Seq[Seq[RowValue]] = {
    val shard = RowValue(Shard, Byte.box(shards(cf).headOption.getOrElse(0)))
    val fid = RowValue(FeatureId, cf.feature.getID)
    toIndexKey(cf.feature).map { z =>
      if (includeFeature) {
        Seq(shard, RowValue(ZValue, Long.box(z)), fid, RowValue(Feature, ByteBuffer.wrap(cf.fullValue)))
      } else {
        Seq(shard, RowValue(ZValue, Long.box(z)), fid)
      }
    }
  }

  override protected def toRowRanges(sft: SimpleFeatureType, range: ScanRange[Long]): Seq[RowRange] = {
    range match {
      case BoundedRange(lo, hi)  => Seq(RowRange(ZValue, lo, hi))
      case UnboundedRange(_)     => Seq.empty
      case SingleRowRange(row)   => Seq(RowRange(ZValue, row, row))
      case LowerBoundedRange(lo) => Seq(RowRange(ZValue, lo, null))
      case UpperBoundedRange(hi) => Seq(RowRange(ZValue, null, hi))
      case PrefixRange(_)        => Seq.empty // not supported
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }
}
