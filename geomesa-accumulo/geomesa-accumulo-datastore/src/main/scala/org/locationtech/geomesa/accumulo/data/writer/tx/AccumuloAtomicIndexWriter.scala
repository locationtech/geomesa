/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.writer
package tx

import org.apache.accumulo.core.client.{ConditionalWriter, ConditionalWriterConfig, IsolatedScanner}
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.accumulo.core.data.{Key, PartialKey}
import org.apache.hadoop.io.Text
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException.ConditionalWriteStatus
import org.locationtech.geomesa.accumulo.data.writer.tx.MutationBuilder.{AppendBuilder, DeleteBuilder, UpdateBuilder}
import org.locationtech.geomesa.index.api.IndexAdapter.BaseIndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.utils.io.{CloseQuietly, WithClose}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

/**
 * Accumulo atomic index writer implementation
 *
 * @param ds data store
 * @param sft simple feature type
 * @param indices indices to write to
 * @param wrapper feature wrapper
 * @param partition partition to write to (if partitioned schema)
 */
class AccumuloAtomicIndexWriter(
    ds: AccumuloDataStore,
    sft: SimpleFeatureType,
    indices: Seq[GeoMesaFeatureIndex[_, _]],
    wrapper: FeatureWrapper[WritableFeature],
    partition: Option[String]
  ) extends BaseIndexWriter[WritableFeature](indices, wrapper) {

  import scala.collection.JavaConverters._

  private val tables = indices.map { index =>
    index.getTableNames(partition) match {
      case Seq(t) => t // should always be writing to a single table here
      case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
    }
  }

  private val writers: Array[ConditionalWriter] = {
    val config = new ConditionalWriterConfig()
    config.setAuthorizations(ds.auths)
    val maxThreads = ClientProperty.BATCH_WRITER_THREADS_MAX.getInteger(ds.connector.properties())
    if (maxThreads != null) {
      config.setMaxWriteThreads(maxThreads)
    }
    val timeout = ClientProperty.BATCH_WRITER_TIMEOUT_MAX.getTimeInMillis(ds.connector.properties())
    if (timeout != null) {
      config.setTimeout(timeout, TimeUnit.MILLISECONDS)
    }
    tables.map(ds.connector.createConditionalWriter(_, config)).toArray
  }

  private val colFamilyMappings = indices.map(ColumnFamilyMapper.apply).toArray
  private val visCache = new VisibilityCache()

  override protected def append(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit =
    mutate(feature.feature.getID, buildMutations(values, AppendBuilder.apply))

  override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit =
    mutate(feature.feature.getID, buildMutations(values, DeleteBuilder.apply))

  override protected def update(
      feature: WritableFeature,
      values: Array[RowKeyValue[_]],
      previous: WritableFeature,
      previousValues: Array[RowKeyValue[_]]): Unit = {
    val mutations = Array.ofDim[Seq[MutationBuilder]](values.length)
    // note: these are temporary conditions - we update them below
    val updates = buildMutations(values, AppendBuilder.apply)
    val deletes = buildMutations(previousValues, DeleteBuilder.apply)
    var i = 0
    while (i < values.length) {
      val dels = ArrayBuffer(deletes(i): _*)
      val puts = updates(i).map { mutations =>
        // find any previous values that will be updated
        val d = dels.indexWhere(p => java.util.Arrays.equals(p.row, mutations.row))
        if (d == -1) { mutations } else {
          // note: side-effect
          val toUpdate = dels.remove(d)
          // any previous values that are overwritten are added as conditions on the put
          // any previous values that aren't overwritten need to be deleted
          val remaining = toUpdate.kvs.filterNot(kv => mutations.kvs.exists(_.equalKey(kv)))
          if (remaining.nonEmpty) {
            dels.append(toUpdate.copy(kvs = remaining))
          }
          UpdateBuilder(mutations.row, mutations.kvs, toUpdate.kvs)
        }
      }
      // note: order here is important, we need puts to operate before deletes in order for conditions to succeed
      mutations(i) = puts ++ dels
      i += 1
    }
    mutate(feature.feature.getID, mutations)
  }

  /**
   * Apply the mutations
   *
   * @param id feature id
   * @param mutations mutations to apply
   */
  @throws[ConditionalWriteException]
  private def mutate(id: String, mutations: Array[Seq[MutationBuilder]]): Unit = {
    val errors = ArrayBuffer.empty[ConditionalWriteStatus]
    val successes = ArrayBuffer.empty[(Int, MutationBuilder)]
    var i = 0
    while (i < mutations.length) {
      // note: mutations need to be applied in order so that conditions are correct
      mutations(i).foreach { m =>
        val status = writers(i).write(m.apply()).getStatus
        if (status == ConditionalWriter.Status.ACCEPTED ||
            (status == ConditionalWriter.Status.UNKNOWN && verifyWrite(tables(i), m))) {
          successes += i -> m
        } else {
          errors += ConditionalWriteStatus(indices(i), m.name, status)
        }
      }
      i += 1
    }
    if (errors.nonEmpty) {
      // revert any writes we made
      successes.foreach { case (i, m) =>
        // note: if these are rejected, some other update has come through and we don't want to overwrite it
        writers(i).write(m.invert())
      }
      throw ConditionalWriteException(id, errors.toSeq)
    }
  }

  /**
   * Verify if a mutation was successfully applied or not
   *
   * @param table table being mutated
   * @param mutation mutation being applied
   * @return true if the current state of the table reflects the mutation's intended result
   */
  private def verifyWrite(table: String, mutation: MutationBuilder): Boolean = {
    WithClose(new IsolatedScanner(ds.connector.createScanner(table, ds.auths))) { scanner =>
      val key = new Key(mutation.row)
      scanner.setRange(new org.apache.accumulo.core.data.Range(key, true, key.followingKey(PartialKey.ROW), false))
      val found = scanner.iterator().asScala.toList
      mutation.apply().getUpdates.asScala.forall { update =>
        if (update.isDeleted) {
          found.forall { entry =>
            !compare(entry.getKey.getColumnFamily, update.getColumnFamily) ||
                !compare(entry.getKey.getColumnQualifier, update.getColumnQualifier) ||
                !compare(entry.getKey.getColumnVisibility, update.getColumnVisibility) ||
                entry.getKey.isDeleted
          }
        } else {
          found.exists { entry =>
            compare(entry.getKey.getColumnFamily, update.getColumnFamily) &&
                compare(entry.getKey.getColumnQualifier, update.getColumnQualifier) &&
                compare(entry.getKey.getColumnVisibility, update.getColumnVisibility) &&
                entry.getValue.compareTo(update.getValue) == 0
          }
        }
      }
    }
  }

  private def buildMutations[T <: MutationBuilder](
      values: Array[RowKeyValue[_]],
      builderFactory: (Array[Byte], Seq[MutationValue]) => T): Array[Seq[T]] = {
    val mutations = Array.ofDim[Seq[T]](values.length)
    var i = 0
    while (i < values.length) {
      mutations(i) = values(i) match {
        case kv: SingleRowKeyValue[_] =>
          val values = kv.values.map { v =>
            MutationValue(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis), v.value)
          }
          Seq(builderFactory(kv.row, values))

        case mkv: MultiRowKeyValue[_] =>
          mkv.rows.map { row =>
            val values = mkv.values.map { v =>
              MutationValue(colFamilyMappings(i)(v.cf), v.cq, visCache(v.vis), v.value)
            }
            builderFactory(row, values)
          }
      }
      i += 1
    }
    mutations
  }

  private def compare(text: Text, bytes: Array[Byte]): Boolean = text.compareTo(bytes, 0, bytes.length) == 0

  override def flush(): Unit = {} // there is no batching here, every single write gets flushed

  override def close(): Unit = CloseQuietly(writers).foreach(e => throw e)
}
