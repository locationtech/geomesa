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

import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.index.ShardStrategy
import org.locationtech.geomesa.index.index.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.index.id.{IdIndex, IdIndexKeySpace}
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case object CassandraIdIndex extends CassandraFeatureIndex[Set[Array[Byte]], Array[Byte]]
    with IdFilterStrategy[CassandraDataStore, CassandraFeature, Seq[RowValue]] {

  private val FeatureId = CassandraFeatureIndex.featureIdColumn(0).copy(partition = true)
  private val Feature = CassandraFeatureIndex.featureColumn(1)

  override val name: String = IdIndex.Name

  override val version: Int = 1

  override protected val columns: Seq[NamedColumn] = Seq(FeatureId, Feature)

  override protected def keySpace: IdIndexKeySpace = IdIndexKeySpace

  override protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy = NoShardStrategy

  override protected def createValues(shards: ShardStrategy,
                                      toIndexKey: SimpleFeature => Seq[Array[Byte]],
                                      includeFeature: Boolean)
                                     (cf: CassandraFeature): Seq[Seq[RowValue]] = {
    toIndexKey(cf.feature).map { id =>
      // note: this may be an encoded UUID
      val fid = RowValue(FeatureId, new String(id, StandardCharsets.UTF_8))
      if (includeFeature) {
        Seq(fid, RowValue(Feature, ByteBuffer.wrap(cf.fullValue)))
      } else {
        Seq(fid)
      }
    }
  }

  override protected def toRowRanges(sft: SimpleFeatureType, range: ScanRange[Array[Byte]]): Seq[RowRange] = {
    val idFromBytes = GeoMesaFeatureIndex.idFromBytes(sft)
    def toId(bytes: Array[Byte]): String = idFromBytes(bytes, 0, bytes.length, null)

    range match {
      case SingleRowRange(row)   => val id = toId(row); Seq(RowRange(FeatureId, id, id))
      case UnboundedRange(_)     => Seq.empty
      case BoundedRange(lo, hi)  => Seq(RowRange(FeatureId, toId(lo), toId(hi)))
      case LowerBoundedRange(lo) => Seq(RowRange(FeatureId, toId(lo), null))
      case UpperBoundedRange(hi) => Seq(RowRange(FeatureId, null, toId(hi)))
      case PrefixRange(_)        => Seq.empty // not supported
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }

  // unlike our other indices, the id column may be encoded as a UUID
  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String =
    GeoMesaFeatureIndex.idFromBytes(sft)
}
