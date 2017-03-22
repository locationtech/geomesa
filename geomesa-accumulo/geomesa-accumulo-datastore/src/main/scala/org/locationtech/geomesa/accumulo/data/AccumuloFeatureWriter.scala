/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.accumulo.{AccumuloAppendFeatureWriterType, AccumuloFeatureIndexType, AccumuloFeatureWriterType, AccumuloModifyFeatureWriterType}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter


class AccumuloAppendFeatureWriter(sft: SimpleFeatureType,
                                  ds: AccumuloDataStore,
                                  indices: Option[Seq[AccumuloFeatureIndexType]],
                                  val defaultVisibility: String)
    extends AccumuloFeatureWriterType(sft, ds, indices) with AccumuloAppendFeatureWriterType with AccumuloFeatureWriter

class AccumuloModifyFeatureWriter(sft: SimpleFeatureType,
                                  ds: AccumuloDataStore,
                                  indices: Option[Seq[AccumuloFeatureIndexType]],
                                  val defaultVisibility: String,
                                  val filter: Filter)
    extends AccumuloFeatureWriterType(sft, ds, indices) with AccumuloModifyFeatureWriterType with AccumuloFeatureWriter

trait AccumuloFeatureWriter extends AccumuloFeatureWriterType {

  import scala.collection.JavaConversions._

  def defaultVisibility: String

  // note: has to be lazy for initialization of super class
  private lazy val multiWriter = ds.connector.createMultiTableBatchWriter(GeoMesaBatchWriterConfig())
  private val wrapper = AccumuloFeature.wrapper(sft, defaultVisibility)

  override protected def createMutators(tables: IndexedSeq[String]): IndexedSeq[BatchWriter] =
    tables.map(multiWriter.getBatchWriter)

  override protected def executeWrite(mutator: BatchWriter, writes: Seq[Mutation]): Unit = mutator.addMutations(writes)

  override protected def executeRemove(mutator: BatchWriter, removes: Seq[Mutation]): Unit = mutator.addMutations(removes)

  override def wrapFeature(feature: SimpleFeature): AccumuloFeature = wrapper(feature)

  abstract override def flush(): Unit = {
    multiWriter.flush()
    super.flush()
  }

  abstract override def close(): Unit = {
    multiWriter.close()
    super.close()
  }
}