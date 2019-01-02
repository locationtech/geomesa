/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index.z2

import java.util.Collections

import org.apache.kudu.Schema
import org.apache.kudu.client.{CreateTableOptions, PartialRow}
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.index.IndexKeySpace.{BoundedRange, ScanRange, UnboundedRange}
import org.locationtech.geomesa.kudu.data.KuduFeature
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex
import org.locationtech.geomesa.kudu.schema.KuduColumnAdapter
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, ZColumnAdapter}
import org.locationtech.geomesa.kudu.{KuduSpatialFilterStrategy, KuduValue}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait KuduZ2Schema[K] extends KuduFeatureIndex[K, Long] with KuduSpatialFilterStrategy {

  override protected val keyColumns: Seq[KuduColumnAdapter[_]] = Seq(ZColumnAdapter, FeatureIdAdapter)

  override protected def configurePartitions(sft: SimpleFeatureType,
                                             schema: Schema,
                                             config: Map[String, String],
                                             options: CreateTableOptions): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // add hash splits based on our shards, which we don't need to actually store as a separate column
    val shards = sft.getZShards
    if (shards > 1) {
      options.addHashPartitions(Collections.singletonList(FeatureIdAdapter.name), shards)
    }

    options.setRangePartitionColumns(Collections.singletonList(ZColumnAdapter.name))

    val bitSplits = {
      val configured = DefaultSplitter.Parser.z2Splits(config)
      if (configured.isEmpty) { Seq(0L, Long.MaxValue) } else {
        // add upper and lower bounds as our splits don't have endpoints
        val builder = Seq.newBuilder[Long]
        builder.sizeHint(configured.length + 2)
        builder += 0L
        builder ++= configured.sorted
        builder += Long.MaxValue
        builder.result.distinct
      }
    }

    bitSplits.sliding(2).foreach { case Seq(lo, hi) =>
      val lower = schema.newPartialRow()
      val upper = schema.newPartialRow()
      lower.addLong(0, lo)
      upper.addLong(0, hi)
      options.addRangePartition(lower, upper)
    }
  }

  override protected def createKeyValues(toIndexKey: SimpleFeature => Seq[Long])
                                        (kf: KuduFeature): Seq[Seq[KuduValue[_]]] = {
    val fid = KuduValue(kf.feature.getID, FeatureIdAdapter)
    toIndexKey(kf.feature).map(z => Seq(KuduValue(z, ZColumnAdapter), fid))
  }


  override protected def toRowRanges(sft: SimpleFeatureType,
                                     schema: Schema,
                                     range: ScanRange[Long]): (Option[PartialRow], Option[PartialRow]) = {
    def lower(z: Long): Some[PartialRow] = {
      val row = schema.newPartialRow()
      ZColumnAdapter.writeToRow(row, z)
      FeatureIdAdapter.writeToRow(row, "")
      Some(row)
    }

    def upper(z: Long): Some[PartialRow] = {
      val row = schema.newPartialRow()
      // note: shouldn't have to worry about overflow, as our z2/xz2 curves don't use the full 64 bits
      ZColumnAdapter.writeToRow(row, z + 1L)
      FeatureIdAdapter.writeToRow(row, "")
      Some(row)
    }

    range match {
      case BoundedRange(lo, hi)  => (lower(lo), upper(hi))
      case UnboundedRange(empty) => (None, None)
      case _ => throw new IllegalArgumentException(s"Unexpected range type $range")
    }
  }
}
