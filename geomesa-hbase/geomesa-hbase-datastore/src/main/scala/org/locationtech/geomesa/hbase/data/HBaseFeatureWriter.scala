/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.HBaseSystemProperties.WriteBatchSize
import org.locationtech.geomesa.hbase.{HBaseAppendFeatureWriterType, HBaseFeatureIndexType, HBaseFeatureWriterType, HBaseModifyFeatureWriterType}
import org.locationtech.geomesa.utils.io.FlushQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class HBaseAppendFeatureWriter(sft: SimpleFeatureType, ds: HBaseDataStore, indices: Option[Seq[HBaseFeatureIndexType]])
    extends HBaseFeatureWriterType(sft, ds, indices) with HBaseAppendFeatureWriterType with HBaseFeatureWriter

class HBaseModifyFeatureWriter(sft: SimpleFeatureType,
                               ds: HBaseDataStore,
                               indices: Option[Seq[HBaseFeatureIndexType]],
                               val filter: Filter)
    extends HBaseFeatureWriterType(sft, ds, indices) with HBaseModifyFeatureWriterType with HBaseFeatureWriter

trait HBaseFeatureWriter extends HBaseFeatureWriterType {

  private val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

  override protected def createMutators(tables: IndexedSeq[String]): IndexedSeq[BufferedMutator] = {
    val batchSize = WriteBatchSize.option.map(_.toLong)
    tables.map { name =>
      val params = new BufferedMutatorParams(TableName.valueOf(name))
      batchSize.foreach(params.writeBufferSize)
      ds.connection.getBufferedMutator(params)
    }
  }

  override protected def executeWrite(mutator: BufferedMutator, writes: Seq[Mutation]): Unit =
    writes.foreach(mutator.mutate)

  override protected def executeRemove(mutator: BufferedMutator, removes: Seq[Mutation]): Unit =
    removes.foreach(mutator.mutate)

  override def wrapFeature(feature: SimpleFeature): HBaseFeature = new HBaseFeature(feature, serializer)

  override def flush(): Unit = {
    // note: BufferedMutator doesn't implement Flushable, so super class won't call it
    mutators.foreach(m => FlushQuietly(m).foreach(exceptions.+=))
    super.flush()
  }
}