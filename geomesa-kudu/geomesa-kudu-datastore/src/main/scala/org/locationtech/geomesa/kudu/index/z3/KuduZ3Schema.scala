/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index.z3

import java.util.Collections

import org.apache.kudu.Schema
import org.apache.kudu.client.{AlterTableOptions, CreateTableOptions, KuduTable}
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.conf.splitter.DefaultSplitter
import org.locationtech.geomesa.index.index.z3.Z3IndexKey
import org.locationtech.geomesa.kudu.data.KuduFeature
import org.locationtech.geomesa.kudu.index.KuduFeatureIndex
import org.locationtech.geomesa.kudu.schema.KuduColumnAdapter
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, PeriodColumnAdapter, ZColumnAdapter}
import org.locationtech.geomesa.kudu.{KuduSpatioTemporalFilterStrategy, KuduValue, Partitioning}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait KuduZ3Schema[K] extends KuduFeatureIndex[K, Z3IndexKey] with KuduSpatioTemporalFilterStrategy {

  override protected val keyColumns: Seq[KuduColumnAdapter[_]] =
    Seq(PeriodColumnAdapter, ZColumnAdapter, FeatureIdAdapter)


  override protected def configurePartitions(sft: SimpleFeatureType,
                                             schema: Schema,
                                             config: Map[String, String],
                                             options: CreateTableOptions): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConverters._

    // add hash splits based on our shards, which we don't need to actually store as a separate column
    val shards = sft.getZShards
    if (shards > 1) {
      options.addHashPartitions(Collections.singletonList(FeatureIdAdapter.name), shards)
    }

    options.setRangePartitionColumns(Seq(PeriodColumnAdapter.name, ZColumnAdapter.name).asJava)

    val splits = {
      val configured = DefaultSplitter.Parser.z3Splits(sft.getZ3Interval, config)
      if (configured.isEmpty) {
        val bin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)(System.currentTimeMillis()).bin
        Map(bin -> Seq(0, Long.MaxValue))
      } else {
        configured.groupBy(_._1).map { case (bin, times) =>
          val ts = times.flatMap(_._2)
          // add upper and lower bounds as our splits don't have endpoints
          val t = if (ts.isEmpty) { Seq(0, Long.MaxValue) } else {
            val builder = Seq.newBuilder[Long]
            builder.sizeHint(ts.size + 2)
            builder += 0L
            builder ++= ts.sorted
            builder += Long.MaxValue
            builder.result.distinct
          }
          bin -> t
        }
      }
    }

    splits.foreach { case (bin, times) =>
      times.sliding(2).foreach { case Seq(lo, hi) =>
        val lower = schema.newPartialRow()
        val upper = schema.newPartialRow()
        lower.addShort(0, bin)
        upper.addShort(0, bin)
        lower.addLong(1, lo)
        upper.addLong(1, hi)
        options.addRangePartition(lower, upper)
      }
    }
  }

  override protected def createPartition(sft: SimpleFeatureType,
                                         table: KuduTable,
                                         splitters: Map[String, String],
                                         bin: Short): Option[Partitioning] = {
    val alteration = new AlterTableOptions()

    val bitSplits = {
      val configured = DefaultSplitter.Parser.z3BitSplits(splitters)
      if (configured.isEmpty) { Seq(0L, Long.MaxValue) } else {
        configured :+ Long.MaxValue // add an upper bound as our splits don't have endpoints
      }
    }

    val schema = tableSchema(sft)

    bitSplits.sliding(2).foreach { case Seq(lo, hi) =>
      val lower = schema.newPartialRow()
      val upper = schema.newPartialRow()
      lower.addShort(0, bin)
      upper.addShort(0, bin)
      lower.addLong(1, lo)
      upper.addLong(1, hi)
      alteration.addRangePartition(lower, upper)
    }

    Some(Partitioning(table.getName, alteration))
  }

  override protected def createKeyValues(toIndexKey: SimpleFeature => Seq[Z3IndexKey])
                                        (kf: KuduFeature): Seq[Seq[KuduValue[_]]] = {
    val fid = KuduValue(kf.feature.getID, FeatureIdAdapter)
    toIndexKey(kf.feature).map(k => Seq(KuduValue(k.bin, PeriodColumnAdapter), KuduValue(k.z, ZColumnAdapter), fid))
  }
}
