/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.writer

import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.api.IndexAdapter.BaseIndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._

/**
  * Accumulo index writer implementation
  *
  * @param ds data store
  * @param indices indices to write to
  * @param wrapper feature wrapper
  * @param partition partition to write to (if partitioned schema)
  */
class AccumuloIndexWriter(
    ds: AccumuloDataStore,
    indices: Seq[GeoMesaFeatureIndex[_, _]],
    wrapper: FeatureWrapper[WritableFeature],
    partition: Option[String]
  ) extends BaseIndexWriter[WritableFeature](indices, wrapper) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val multiWriter = ds.connector.createMultiTableBatchWriter()
  private val writers = indices.toArray.map { index =>
    val table = index.getTableNames(partition) match {
      case Seq(t) => t // should always be writing to a single table here
      case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
    }
    multiWriter.getBatchWriter(table)
  }

  private val colFamilyMappings = indices.map(ColumnFamilyMapper.apply).toArray
  private val timestamps = indices.exists(i => !i.sft.isLogicalTime)
  private val visCache = new VisibilityCache()

  override protected def append(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
    var i = 0
    while (i < values.length) {
      values(i) match {
        case kv: SingleRowKeyValue[_] =>
          val mutation = new Mutation(kv.row)
          kv.values.foreach { v =>
            mutation.put(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis), v.value)
          }
          writers(i).addMutation(mutation)

        case mkv: MultiRowKeyValue[_] =>
          mkv.rows.foreach { row =>
            val mutation = new Mutation(row)
            mkv.values.foreach { v =>
              mutation.put(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis), v.value)
            }
            writers(i).addMutation(mutation)
          }
      }
      i += 1
    }
  }

  override protected def update(
      feature: WritableFeature,
      values: Array[RowKeyValue[_]],
      previous: WritableFeature,
      previousValues: Array[RowKeyValue[_]]): Unit = {
    delete(previous, previousValues)
    if (timestamps) {
      // for updates, ensure that our timestamps don't clobber each other
      multiWriter.flush()
      Thread.sleep(1)
    }
    append(feature, values)
  }

  override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
    var i = 0
    while (i < values.length) {
      values(i) match {
        case kv: SingleRowKeyValue[_] =>
          val mutation = new Mutation(kv.row)
          kv.values.foreach { v =>
            mutation.putDelete(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis))
          }
          writers(i).addMutation(mutation)

        case kv: MultiRowKeyValue[_] =>
          kv.rows.foreach { row =>
            val mutation = new Mutation(row)
            kv.values.foreach { v =>
              mutation.putDelete(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis))
            }
            writers(i).addMutation(mutation)
          }
      }
      i += 1
    }
  }

  override def flush(): Unit = multiWriter.flush()

  override def close(): Unit = multiWriter.close()
}
