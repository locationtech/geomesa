/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.cassandra.{ColumnSelect, NamedColumn, RowSelect}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey

object SharedAttributeColumnMapper extends CassandraColumnMapper {

  private val Index     = NamedColumn("attrIdx",   0, "smallint", classOf[Short],  partition = true)
  private val Value     = NamedColumn("attrVal",   1, "text",     classOf[String])
  private val Secondary = NamedColumn("secondary", 2, "blob",     classOf[ByteBuffer])
  private val FeatureId = CassandraColumnMapper.featureIdColumn(3)
  private val Feature   = CassandraColumnMapper.featureColumn(4)

  override val columns: Seq[NamedColumn] = Seq(Index, Value, Secondary, FeatureId, Feature)

  override def bind(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val AttributeIndexKey(i, v, _) = value.key
    val secondary = ByteBuffer.wrap(value.tier)
    val fid = new String(value.id, StandardCharsets.UTF_8)
    val Seq(feature) = value.values.map(v => ByteBuffer.wrap(v.value))
    Seq(Short.box(i), v, secondary, fid, feature)
  }

  override def bindDelete(value: SingleRowKeyValue[_]): Seq[AnyRef] = {
    val AttributeIndexKey(i, v, _) = value.key
    val secondary = ByteBuffer.wrap(value.tier)
    val fid = new String(value.id, StandardCharsets.UTF_8)
    Seq(Short.box(i), v, secondary, fid)
  }

  override def select(range: ScanRange[_], tieredKeyRanges: Seq[ByteRange]): Seq[RowSelect] = {
    val primary = range.asInstanceOf[ScanRange[AttributeIndexKey]] match {
      case SingleRowRange(row) =>
        val i = Short.box(row.i)
        Seq(ColumnSelect(Index, i, i), ColumnSelect(Value, row.value, row.value))

      case BoundedRange(lo, hi) =>
        val i = Short.box(lo.i) // note: should be the same for upper and lower
        Seq(ColumnSelect(Index, i, i), ColumnSelect(Value, lo.value, hi.value))

      case LowerBoundedRange(lo) =>
        val i = Short.box(lo.i)
        Seq(ColumnSelect(Index, i, i), ColumnSelect(Value, lo.value, null))

      case UpperBoundedRange(hi) =>
        val i = Short.box(hi.i)
        Seq(ColumnSelect(Index, i, i), ColumnSelect(Value, null, hi.value))

      case PrefixRange(prefix) =>
        val i = Short.box(prefix.i)
        Seq(ColumnSelect(Index, i, i), ColumnSelect(Value, prefix.value, prefix.value + "zzzz")) // TODO ?

      case UnboundedRange(empty) =>
        val i = Short.box(empty.i)
        Seq(ColumnSelect(Index, i, i))

      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
    val clause = if (tieredKeyRanges.isEmpty) { primary } else {
      val minTier = ByteRange.min(tieredKeyRanges)
      val maxTier = ByteRange.max(tieredKeyRanges)
      primary :+ ColumnSelect(Secondary, ByteBuffer.wrap(minTier), ByteBuffer.wrap(maxTier))
    }
    Seq(RowSelect(clause))
  }
}
