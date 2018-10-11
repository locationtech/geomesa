/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

abstract class AccumuloFeatureWriter(val sft: SimpleFeatureType,
                                     val ds: AccumuloDataStore,
                                     val indices: Seq[AccumuloFeatureIndexType],
                                     val defaultVisibility: String,
                                     val filter: Filter,
                                     val partition: TablePartition) extends AccumuloFeatureWriterType {

  import scala.collection.JavaConversions._

  private val multiWriter = ds.connector.createMultiTableBatchWriter(GeoMesaBatchWriterConfig())
  private val wrapper = AccumuloFeature.wrapper(sft, defaultVisibility)

  override protected def createMutator(table: String): BatchWriter = multiWriter.getBatchWriter(table)

  override protected def executeWrite(mutator: BatchWriter, writes: Seq[Mutation]): Unit =
    mutator.addMutations(writes)

  override protected def executeRemove(mutator: BatchWriter, removes: Seq[Mutation]): Unit =
    mutator.addMutations(removes)

  override def wrapFeature(feature: SimpleFeature): AccumuloFeature = wrapper(feature)

  override def flush(): Unit = multiWriter.flush()

  override def close(): Unit = multiWriter.close()
}

object AccumuloFeatureWriter {

  class AccumuloFeatureWriterFactory(ds: AccumuloDataStore) extends AccumuloFeatureWriterFactoryType {
    override def createFeatureWriter(sft: SimpleFeatureType,
                                     indices: Seq[AccumuloFeatureIndexType],
                                     filter: Option[Filter]): FlushableFeatureWriter = {
      (TablePartition(ds, sft), filter) match {
        case (None, None) =>
          new AccumuloFeatureWriter(sft, ds, indices, ds.config.defaultVisibilities, null, null)
              with AccumuloTableFeatureWriterType with AccumuloAppendFeatureWriterType

        case (None, Some(f)) =>
          new AccumuloFeatureWriter(sft, ds, indices, ds.config.defaultVisibilities, f, null)
              with AccumuloTableFeatureWriterType with AccumuloModifyFeatureWriterType

        case (Some(p), None) =>
          new AccumuloFeatureWriter(sft, ds, indices, ds.config.defaultVisibilities, null, p)
              with AccumuloPartitionedFeatureWriterType with AccumuloAppendFeatureWriterType

        case (Some(p), Some(f)) =>
          new AccumuloFeatureWriter(sft, ds, indices, ds.config.defaultVisibilities, f, p)
              with AccumuloPartitionedFeatureWriterType with AccumuloModifyFeatureWriterType
      }
    }
  }
}
