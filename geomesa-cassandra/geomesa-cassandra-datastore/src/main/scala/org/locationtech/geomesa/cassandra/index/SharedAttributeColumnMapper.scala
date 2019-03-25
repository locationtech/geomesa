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
        val indexSelect = ColumnSelect(Index, i, i, startInclusive = true, endInclusive = true)
        val valueSelect = ColumnSelect(Value, row.value, row.value, startInclusive = true, endInclusive = true)
        Seq(indexSelect, valueSelect)

      case BoundedRange(lo, hi) =>
        val i = Short.box(lo.i) // note: should be the same for upper and lower
        val indexSelect = ColumnSelect(Index, i, i, startInclusive = true, endInclusive = true)
        val valueSelect = ColumnSelect(Value, lo.value, hi.value, lo.inclusive, hi.inclusive)
        Seq(indexSelect, valueSelect)

      case LowerBoundedRange(lo) =>
        val i = Short.box(lo.i)
        val indexSelect = ColumnSelect(Index, i, i, startInclusive = true, endInclusive = true)
        val valueSelect = ColumnSelect(Value, lo.value, null, lo.inclusive, endInclusive = false)
        Seq(indexSelect, valueSelect)

      case UpperBoundedRange(hi) =>
        val i = Short.box(hi.i)
        val indexSelect = ColumnSelect(Index, i, i, startInclusive = true, endInclusive = true)
        val valueSelect = ColumnSelect(Value, null, hi.value, startInclusive = false, hi.inclusive)
        Seq(indexSelect, valueSelect)

      case PrefixRange(prefix) =>
        val i = Short.box(prefix.i)
        val indexSelect = ColumnSelect(Index, i, i, startInclusive = true, endInclusive = true)
        val valueSelect = ColumnSelect(Value, prefix.value, s"${prefix.value}zzzz", prefix.inclusive, endInclusive = false) // TODO ?
        Seq(indexSelect, valueSelect)

      case UnboundedRange(empty) =>
        val i = Short.box(empty.i)
        Seq(ColumnSelect(Index, i, i, startInclusive = true, endInclusive = true))

      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
    val clause = if (tieredKeyRanges.isEmpty) { primary } else {
      val minTier = ByteRange.min(tieredKeyRanges)
      val maxTier = ByteRange.max(tieredKeyRanges)
      primary :+ ColumnSelect(Secondary, ByteBuffer.wrap(minTier), ByteBuffer.wrap(maxTier), startInclusive = true, endInclusive = true)
    }
    Seq(RowSelect(clause))
  }
}
