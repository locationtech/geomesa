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
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey, AttributeIndexKeySpace, AttributeIndexValues}
import org.locationtech.geomesa.kudu.data.KuduFeature
import org.locationtech.geomesa.kudu.schema.KuduColumnAdapter
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter._
import org.locationtech.geomesa.kudu.{KuduAttributeFilterStrategy, KuduValue}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case object KuduAttributeIndex extends KuduAttributeIndex

trait KuduAttributeIndex extends KuduTieredFeatureIndex[AttributeIndexValues[Any], AttributeIndexKey]
    with KuduAttributeFilterStrategy {

  import scala.collection.JavaConverters._

  override val name: String = AttributeIndex.Name

  override val version: Int = 1

  override protected val keyColumns: Seq[KuduColumnAdapter[_]] =
    Seq(IdxColumnAdapter, ValueColumnAdapter, SecondaryColumnAdapter, FeatureIdAdapter)

  override protected def keySpace: AttributeIndexKeySpace = AttributeIndexKeySpace

  override protected def tieredKeySpace(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]] =
    AttributeIndex.TieredOptions.find(_.supports(sft))

  override protected def configurePartitions(sft: SimpleFeatureType,
                                             schema: Schema,
                                             config: Map[String, String],
                                             options: CreateTableOptions): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // add hash splits based on our shards, which we don't need to actually store as a separate column
    val shards = sft.getAttributeShards
    if (shards > 1) {
      options.addHashPartitions(Collections.singletonList(ValueColumnAdapter.name), shards)
    }

    options.setRangePartitionColumns(Seq(IdxColumnAdapter.name, ValueColumnAdapter.name).asJava)

    SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).foreach { d =>
      val splits = {
        val upperBound = new String(ByteRange.UnboundedUpperRange, StandardCharsets.UTF_8)
        val configured = DefaultSplitter.Parser.attributeSplits(d.getLocalName, d.getType.getBinding, config)
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
      val i = sft.indexOf(d.getLocalName).toShort
      splits.sliding(2).foreach { case Seq(lo, hi) =>
        val lower = schema.newPartialRow()
        val upper = schema.newPartialRow()
        lower.addShort(0, i)
        upper.addShort(0, i)
        lower.addString(1, lo)
        upper.addString(1, hi)
        options.addRangePartition(lower, upper)
      }
    }
  }

  override protected def createKeyValues(toIndexKey: SimpleFeature => Seq[AttributeIndexKey])
                                        (kf: KuduFeature): Seq[Seq[KuduValue[_]]] = {
    val fid = KuduValue(kf.feature.getID, FeatureIdAdapter)
    toIndexKey(kf.feature).map { key =>
      Seq(
        KuduValue(key.i, IdxColumnAdapter),
        KuduValue(key.value, ValueColumnAdapter),
        KuduValue(Array.empty[Byte], SecondaryColumnAdapter),
        fid
      )
    }
  }


  override protected def createKeyValues(toIndexKey: SimpleFeature => Seq[AttributeIndexKey],
                                         toTieredIndexKey: SimpleFeature => Seq[Array[Byte]])
                                        (kf: KuduFeature): Seq[Seq[KuduValue[_]]] = {
    val fid = KuduValue(kf.feature.getID, FeatureIdAdapter)
    val tiers = toTieredIndexKey(kf.feature)
    toIndexKey(kf.feature).flatMap { key =>
      tiers.map { secondary =>
        Seq(
          KuduValue(key.i, IdxColumnAdapter),
          KuduValue(key.value, ValueColumnAdapter),
          KuduValue(secondary, SecondaryColumnAdapter),
          fid
        )
      }
    }
  }

  override protected def toRowRanges(sft: SimpleFeatureType,
                                     schema: Schema,
                                     range: ScanRange[AttributeIndexKey]): (Option[PartialRow], Option[PartialRow]) = {
    range match {
      case SingleRowRange(row)   => (lower(schema, row), upper(schema, row))
      case BoundedRange(lo, hi)  => (lower(schema, lo), upper(schema, hi))
      case PrefixRange(prefix)   => (lower(schema, prefix, prefix = true), upper(schema, prefix, prefix = true))
      case LowerBoundedRange(lo) => (lower(schema, lo), upper(schema, lo.copy(value = null, inclusive = false)))
      case UpperBoundedRange(hi) => (lower(schema, hi.copy(value = null, inclusive = false)), upper(schema, hi))
      case UnboundedRange(empty) => (lower(schema, empty), upper(schema, empty))
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }

  override protected def toTieredRowRanges(sft: SimpleFeatureType,
                                           schema: Schema,
                                           range: ScanRange[AttributeIndexKey],
                                           tiers: => Seq[ByteRange],
                                           minTier: => Array[Byte],
                                           maxTier: => Array[Byte]): Seq[(Option[PartialRow], Option[PartialRow])] = {
    range match {
      case SingleRowRange(row) =>
        // single row range - can use all the tiered values
        tiers.map {
          case BoundedByteRange(lo, hi) => (lower(schema, row, lo), upper(schema, row, hi))
          case SingleRowByteRange(r)    => (lower(schema, row, r), upper(schema, row, r))
          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }

      case BoundedRange(lo, hi)  => Seq((lower(schema, lo, minTier), upper(schema, hi, maxTier)))
      case PrefixRange(prefix)   => Seq((lower(schema, prefix, prefix = true), upper(schema, prefix, prefix = true)))
      case LowerBoundedRange(lo) => Seq((lower(schema, lo, minTier), upper(schema, lo.copy(value = null, inclusive = false))))
      case UpperBoundedRange(hi) => Seq((lower(schema, hi.copy(value = null, inclusive = false)), upper(schema, hi, maxTier)))
      case UnboundedRange(empty) => Seq((lower(schema, empty), upper(schema, empty)))
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }

  private def lower(schema: Schema,
                    key: AttributeIndexKey,
                    secondary: Array[Byte] = Array.empty,
                    prefix: Boolean = false): Some[PartialRow] = {
    val row = schema.newPartialRow()
    IdxColumnAdapter.writeToRow(row, key.i)
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
      if (key.i + 1 == Short.MaxValue) {
        // push the exclusive value to the value column to avoid numeric overflow
        IdxColumnAdapter.writeToRow(row, key.i)
        ValueColumnAdapter.writeToRow(row, new String(ByteArrays.ZeroByteArray, StandardCharsets.UTF_8))
      } else {
        IdxColumnAdapter.writeToRow(row, (key.i + 1).toShort)
        ValueColumnAdapter.writeToRow(row, "")
      }
      SecondaryColumnAdapter.writeToRow(row, Array.empty)
    } else {
      IdxColumnAdapter.writeToRow(row, key.i)
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
