/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.HBaseSystemProperties.WriteBatchSize
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.utils.io.FlushQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer


abstract class HBaseFeatureWriter(val sft: SimpleFeatureType,
                                  val ds: HBaseDataStore,
                                  val indices: Seq[HBaseFeatureIndexType],
                                  val filter: Filter,
                                  val partition: TablePartition) extends HBaseFeatureWriterType {

  private val wrapper = HBaseFeature.wrapper(sft)

  private val mutators = ArrayBuffer.empty[BufferedMutator]

  override protected def createMutator(table: String): BufferedMutator = {
    val batchSize = WriteBatchSize.option.map(_.toLong)
    val params = new BufferedMutatorParams(TableName.valueOf(table))
    batchSize.foreach(params.writeBufferSize)
    val mutator = ds.connection.getBufferedMutator(params)
    mutators += mutator // track our mutators so we can flush them, below
    mutator
  }

  override protected def executeWrite(mutator: BufferedMutator, writes: Seq[Mutation]): Unit =
    writes.foreach(mutator.mutate)

  override protected def executeRemove(mutator: BufferedMutator, removes: Seq[Mutation]): Unit =
    removes.foreach(mutator.mutate)

  override protected def wrapFeature(feature: SimpleFeature): HBaseFeature = wrapper(feature)

  // note: BufferedMutator doesn't implement Flushable, so super class won't call it
  override def flush(): Unit = mutators.foreach(m => FlushQuietly(m).foreach(suppressException))

  override def close(): Unit = {}
}

object HBaseFeatureWriter {

  class HBaseFeatureWriterFactory(ds: HBaseDataStore) extends HBaseFeatureWriterFactoryType {
    override def createFeatureWriter(sft: SimpleFeatureType,
                                     indices: Seq[HBaseFeatureIndexType],
                                     filter: Option[Filter]): FlushableFeatureWriter = {
      (TablePartition(ds, sft), filter) match {
        case (None, None) =>
          new HBaseFeatureWriter(sft, ds, indices, null, null)
              with HBaseTableFeatureWriterType with HBaseAppendFeatureWriterType

        case (None, Some(f)) =>
          new HBaseFeatureWriter(sft, ds, indices, f, null)
              with HBaseTableFeatureWriterType with HBaseModifyFeatureWriterType

        case (Some(p), None) =>
          new HBaseFeatureWriter(sft, ds, indices, null, p)
              with HBasePartitionedFeatureWriterType with HBaseAppendFeatureWriterType

        case (Some(p), Some(f)) =>
          new HBaseFeatureWriter(sft, ds, indices, f, p)
              with HBasePartitionedFeatureWriterType with HBaseModifyFeatureWriterType
      }
    }
  }
}
