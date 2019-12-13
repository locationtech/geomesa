/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index

import java.nio.charset.StandardCharsets
import java.util.Collections

import org.apache.kudu.Schema
import org.apache.kudu.client.{CreateTableOptions, PartialRow}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.kudu.KuduValue
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter._
import org.locationtech.geomesa.utils.index.ByteArrays

object AttributeColumnMapper {
  private val columns = Seq(ValueColumnAdapter, SecondaryColumnAdapter, FeatureIdAdapter)
}

class AttributeColumnMapper(index: GeoMesaFeatureIndex[_, _])
    extends KuduColumnMapper(index, AttributeColumnMapper.columns) {

  override def configurePartitions(): CreateTableOptions = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val options = new CreateTableOptions()

    // add hash splits based on our shards, which we don't need to actually store as a separate column
    val shards = index.sft.getAttributeShards
    if (shards > 1) {
      options.addHashPartitions(Collections.singletonList(ValueColumnAdapter.name), shards)
    }

    options.setRangePartitionColumns(Collections.singletonList(ValueColumnAdapter.name))

    val descriptor = index.sft.getDescriptor(index.attributes.head)

    val splits = {
      val upperBound = new String(ByteRange.UnboundedUpperRange, StandardCharsets.UTF_8)
      val configured = DefaultSplitter.Parser.attributeSplits(descriptor.getLocalName, descriptor.getType.getBinding, splitters)
      if (configured.isEmpty) { Seq("", upperBound) } else {
        // add an upper and lower bound as our splits don't have endpoints
        val builder = Seq.newBuilder[String]
        builder.sizeHint(configured.length + 2)
        builder += ""
        builder ++= configured.sorted
        builder += upperBound
        builder.result.distinct
      }
    }
    splits.sliding(2).foreach { case Seq(lo, hi) =>
      val lower = tableSchema.newPartialRow()
      val upper = tableSchema.newPartialRow()
      lower.addString(0, lo)
      upper.addString(0, hi)
      options.addRangePartition(lower, upper)
    }

    options
  }

  override def createKeyValues(value: SingleRowKeyValue[_]): Seq[KuduValue[_]] = {
    val fid = KuduValue(new String(value.id, StandardCharsets.UTF_8), FeatureIdAdapter)
    value.key match {
      case k: AttributeIndexKey => Seq(KuduValue(k.value, ValueColumnAdapter), KuduValue(value.tier, SecondaryColumnAdapter), fid)
      case _ => throw new IllegalStateException(s"Expected attribute index key but got '${value.key}'")
    }
  }

  override def toRowRanges(ranges: Seq[ScanRange[_]],
                           tieredKeyRanges: Seq[ByteRange]): Seq[(Option[PartialRow], Option[PartialRow])] = {

    lazy val minTier = ByteRange.min(tieredKeyRanges)
    lazy val maxTier = ByteRange.max(tieredKeyRanges)

    ranges.asInstanceOf[Seq[ScanRange[AttributeIndexKey]]].flatMap {
      case SingleRowRange(row) =>
        // single row range - can use all the tiered values
        tieredKeyRanges.map {
          case BoundedByteRange(lo, hi) => (lower(tableSchema, row, lo), upper(tableSchema, row, hi))
          case SingleRowByteRange(r)    => (lower(tableSchema, row, r), upper(tableSchema, row, r))
          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }

      case BoundedRange(lo, hi)  => Seq((lower(tableSchema, lo, minTier), upper(tableSchema, hi, maxTier)))
      case PrefixRange(prefix)   => Seq((lower(tableSchema, prefix, prefix = true), upper(tableSchema, prefix, prefix = true)))
      case LowerBoundedRange(lo) => Seq((lower(tableSchema, lo, minTier), upper(tableSchema, lo.copy(value = null, inclusive = false))))
      case UpperBoundedRange(hi) => Seq((lower(tableSchema, hi.copy(value = null, inclusive = false)), upper(tableSchema, hi, maxTier)))
      case UnboundedRange(empty) => Seq((lower(tableSchema, empty), upper(tableSchema, empty)))
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }
  }

  private def lower(schema: Schema,
                    key: AttributeIndexKey,
                    secondary: Array[Byte] = Array.empty,
                    prefix: Boolean = false): Some[PartialRow] = {
    val row = schema.newPartialRow()
    if (key.value == null) {
      ValueColumnAdapter.writeToRow(row, "")
    } else if (prefix || key.inclusive) {
      ValueColumnAdapter.writeToRow(row, key.value)
    } else {
      ValueColumnAdapter.writeToRow(row, key.value + new String(ByteArrays.ZeroByteArray, StandardCharsets.UTF_8))
    }
    SecondaryColumnAdapter.writeToRow(row, secondary)
    FeatureIdAdapter.writeToRow(row, "")
    Some(row)
  }

  private def upper(schema: Schema,
                    key: AttributeIndexKey,
                    secondary: Array[Byte] = Array.empty,
                    prefix: Boolean = false): Some[PartialRow] = {
    val row = schema.newPartialRow()
    if (key.value == null) {
      ValueColumnAdapter.writeToRow(row, "")
      SecondaryColumnAdapter.writeToRow(row, Array.empty)
    } else {
      // note: we can't tier exclusive end points, as we can't calculate previous rows
      if (prefix || secondary.length == 0 || !key.inclusive) {
        if (key.inclusive) {
          // use 3 consecutive max-bytes as the exclusive upper bound - hopefully no values will match this...
          ValueColumnAdapter.writeToRow(row, key.value + new String(ByteRange.UnboundedUpperRange, StandardCharsets.UTF_8))
        } else {
          ValueColumnAdapter.writeToRow(row, key.value)
        }
        SecondaryColumnAdapter.writeToRow(row, Array.empty)
      } else {
        ValueColumnAdapter.writeToRow(row, key.value)
        SecondaryColumnAdapter.writeToRow(row, secondary)
      }
    }
    FeatureIdAdapter.writeToRow(row, "")
    Some(row)
  }
}
