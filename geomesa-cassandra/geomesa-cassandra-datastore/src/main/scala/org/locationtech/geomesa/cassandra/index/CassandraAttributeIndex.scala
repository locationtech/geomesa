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

import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.cassandra.{NamedColumn, RowRange, RowValue}
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.index.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey, AttributeIndexKeySpace, AttributeIndexValues}
import org.locationtech.geomesa.index.index.{IndexKeySpace, ShardStrategy}
import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case object CassandraAttributeIndex extends CassandraAttributeIndex

trait CassandraAttributeIndex extends CassandraTieredFeatureIndex[AttributeIndexValues[Any], AttributeIndexKey]
    with AttributeFilterStrategy[CassandraDataStore, CassandraFeature, Seq[RowValue]] {

  private val Index     = NamedColumn("attrIdx",   0, "smallint", classOf[Short],  partition = true)
  private val Value     = NamedColumn("attrVal",   1, "text",     classOf[String])
  private val Secondary = NamedColumn("secondary", 2, "blob",     classOf[ByteBuffer])
  private val FeatureId = CassandraFeatureIndex.featureIdColumn(3)
  private val Feature   = CassandraFeatureIndex.featureColumn(4)

  override val name: String = AttributeIndex.Name

  override val version: Int = 2

  override protected val columns: Seq[NamedColumn] = Seq(Index, Value, Secondary, FeatureId, Feature)

  override protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy = NoShardStrategy

  override protected def keySpace: AttributeIndexKeySpace = AttributeIndexKeySpace

  override protected def tieredKeySpace(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]] =
    AttributeIndex.TieredOptions.find(_.supports(sft))

  override protected def createValues(shards: ShardStrategy,
                                      toIndexKey: SimpleFeature => Seq[AttributeIndexKey],
                                      includeFeature: Boolean)
                                     (cf: CassandraFeature): Seq[Seq[RowValue]] = {
    toIndexKey(cf.feature).map { k =>
      val i = RowValue(Index, Short.box(k.i))
      val value = RowValue(Value, k.value)
      val secondary = RowValue(Secondary, ByteBuffer.wrap(Array.empty))
      val id = RowValue(FeatureId, cf.feature.getID)
      if (includeFeature) {
        Seq(i, value, secondary, id, RowValue(Feature, ByteBuffer.wrap(cf.fullValue)))
      } else {
        Seq(i, value, secondary, id)
      }
    }
  }

  override protected def createValues(shards: ShardStrategy,
                                      toIndexKey: SimpleFeature => Seq[AttributeIndexKey],
                                      toTieredIndexKey: ToIndexKeyBytes,
                                      includeFeature: Boolean)
                                     (cf: CassandraFeature): Seq[Seq[RowValue]] = {
    toIndexKey(cf.feature).flatMap { k =>
      val i = RowValue(Index, Short.box(k.i))
      val value = RowValue(Value, k.value)
      val secondary = toTieredIndexKey(Seq.empty, cf.feature, Array.empty).map(b => RowValue(Secondary, ByteBuffer.wrap(b)))
      val id = RowValue(FeatureId, cf.feature.getID)
      if (includeFeature) {
        secondary.map(s => Seq(i, value, s, id, RowValue(Feature, ByteBuffer.wrap(cf.fullValue))))
      } else {
        secondary.map(s => Seq(i, value, s, id))
      }
    }
  }

  override protected def toRowRanges(sft: SimpleFeatureType, range: ScanRange[AttributeIndexKey]): Seq[RowRange] = {
    range match {
      case SingleRowRange(row) =>
        val i = Short.box(row.i)
        Seq(RowRange(Index, i, i), RowRange(Value, row.value, row.value))

      case BoundedRange(lo, hi) =>
        val i = Short.box(lo.i) // note: should be the same for upper and lower
        Seq(RowRange(Index, i, i), RowRange(Value, lo.value, hi.value))

      case LowerBoundedRange(lo) =>
        val i = Short.box(lo.i)
        Seq(RowRange(Index, i, i), RowRange(Value, lo.value, null))

      case UpperBoundedRange(hi) =>
        val i = Short.box(hi.i)
        Seq(RowRange(Index, i, i), RowRange(Value, null, hi.value))

      case PrefixRange(prefix) =>
        val i = Short.box(prefix.i)
        Seq(RowRange(Index, i, i), RowRange(Value, prefix.value, prefix.value + "zzzz")) // TODO ?

      case UnboundedRange(empty) =>
        val i = Short.box(empty.i)
        Seq(RowRange(Index, i, i))

      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }

  override protected def toTieredRowRange(lower: Array[Byte], upper: Array[Byte]): RowRange =
    RowRange(Secondary, ByteBuffer.wrap(lower), ByteBuffer.wrap(upper))
}
