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
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object IdColumnMapper {

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[String, IdColumnMapper]() {
      override def load(sft: String): IdColumnMapper = {
        new IdColumnMapper(GeoMesaFeatureIndex.idFromBytes(CacheKeyGenerator.restore(sft)))
      }
    }
  )

  def apply(sft: SimpleFeatureType): IdColumnMapper = cache.get(CacheKeyGenerator.cacheKey(sft))
}

class IdColumnMapper(idFromBytes: (Array[Byte], Int, Int, SimpleFeature) => String) extends CassandraColumnMapper {

  private val FeatureId = CassandraColumnMapper.featureIdColumn(0).copy(partition = true)
  private val Feature   = CassandraColumnMapper.featureColumn(1)

  override val columns: Seq[NamedColumn] = Seq(FeatureId, Feature)

  override def bind(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val fid = new String(value.id, StandardCharsets.UTF_8)
    val Seq(feature) = value.values.map(v => ByteBuffer.wrap(v.value))
    Seq(fid, feature)
  }

  override def bindDelete(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    Seq(new String(value.id, StandardCharsets.UTF_8))
  }

  override def select(range: ScanRange[_], tieredKeyRanges: Seq[ByteRange]): Seq[RowSelect] = {
    range.asInstanceOf[ScanRange[Array[Byte]]] match {
      case SingleRowRange(row)   => val id = toId(row); Seq(RowSelect(Seq(ColumnSelect(FeatureId, id, id, startInclusive = true, endInclusive = true))))
      case UnboundedRange(_)     => Seq(RowSelect(Seq.empty))
      case BoundedRange(lo, hi)  => Seq(RowSelect(Seq(ColumnSelect(FeatureId, toId(lo), toId(hi), startInclusive = true, endInclusive = true))))
      case LowerBoundedRange(lo) => Seq(RowSelect(Seq(ColumnSelect(FeatureId, toId(lo), null, startInclusive = true, endInclusive = false))))
      case UpperBoundedRange(hi) => Seq(RowSelect(Seq(ColumnSelect(FeatureId, null, toId(hi), startInclusive = false, endInclusive = true))))
      case PrefixRange(_)        => Seq.empty // not supported
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }

  private def toId(bytes: Array[Byte]): String = idFromBytes(bytes, 0, bytes.length, null)
}
