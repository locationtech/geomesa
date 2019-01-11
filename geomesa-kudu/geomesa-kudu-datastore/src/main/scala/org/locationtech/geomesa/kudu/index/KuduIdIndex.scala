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

import org.apache.kudu.ColumnSchema.Encoding
import org.apache.kudu.Schema
import org.apache.kudu.client.{CreateTableOptions, PartialRow}
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.index.id.{IdIndex, IdIndexKeySpace}
import org.locationtech.geomesa.kudu._
import org.locationtech.geomesa.kudu.data._
import org.locationtech.geomesa.kudu.schema.KuduColumnAdapter
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.FeatureIdAdapter
import org.locationtech.geomesa.kudu.utils.ColumnConfiguration
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case object KuduIdIndex extends KuduIdIndex

trait KuduIdIndex extends KuduFeatureIndex[Set[Array[Byte]], Array[Byte]] with KuduIdFilterStrategy {

  // use prefix encoding - since this is the first pk column it will be sorted
  private val featureIdAdapter = new FeatureIdAdapter(Encoding.PREFIX_ENCODING, ColumnConfiguration.compression())

  override val name: String = IdIndex.Name

  override val version: Int = 1

  override protected val keyColumns: Seq[KuduColumnAdapter[_]] = Seq(featureIdAdapter)

  override protected def keySpace: IdIndexKeySpace = IdIndexKeySpace

  override protected def configurePartitions(sft: SimpleFeatureType,
                                             schema: Schema,
                                             config: Map[String, String],
                                             options: CreateTableOptions): Unit = {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // add hash splits based on our shards
    val shards = sft.getIdShards
    if (shards > 1) {
      options.addHashPartitions(Collections.singletonList(featureIdAdapter.name), shards)
    }

    options.setRangePartitionColumns(Collections.singletonList(featureIdAdapter.name))

    val idSplits = {
      val upperBound = new String(ByteRange.UnboundedUpperRange, StandardCharsets.UTF_8)
      val configured = DefaultSplitter.Parser.idSplits(config)
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

    idSplits.sliding(2).foreach { case Seq(lo, hi) =>
      val lower = schema.newPartialRow()
      val upper = schema.newPartialRow()
      lower.addString(0, lo)
      upper.addString(0, hi)
      options.addRangePartition(lower, upper)
    }
  }

  override protected def createKeyValues(toIndexKey: SimpleFeature => Seq[Array[Byte]])
                                        (kf: KuduFeature): Seq[Seq[KuduValue[_]]] = {
    toIndexKey(kf.feature).map(key => Seq(KuduValue(new String(key, StandardCharsets.UTF_8), featureIdAdapter)))
  }

  override protected def toRowRanges(sft: SimpleFeatureType,
                                     schema: Schema,
                                     range: ScanRange[Array[Byte]]): (Option[PartialRow], Option[PartialRow]) = {

    def lower(id: Array[Byte]): Some[PartialRow] = {
      val row = schema.newPartialRow()
      featureIdAdapter.writeToRow(row, new String(id, StandardCharsets.UTF_8))
      Some(row)
    }

    def upper(id: Array[Byte]): Some[PartialRow] = {
      val row = schema.newPartialRow()
      featureIdAdapter.writeToRow(row, new String(id, StandardCharsets.UTF_8))
      Some(row)
    }

    range match {
      case SingleRowRange(row)   => (lower(row), upper(row :+ ByteArrays.ZeroByte))
      case BoundedRange(lo, hi)  => (lower(lo), upper(hi))
      case UnboundedRange(empty) => (None, None)
      case LowerBoundedRange(lo) => (lower(lo), None)
      case UpperBoundedRange(hi) => (None, upper(hi))
      case PrefixRange(prefix)   => (None, None) // not supported
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }
}
